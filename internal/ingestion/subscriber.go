package ingestion

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/logger"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

const (
	// headBufferSize bounds queued newHeads notifications while a fetch is in
	// flight. Heads are coalesced on read, so the buffer absorbs a burst, not
	// an outage: go-ethereum queues up to 20k more behind it and then drops
	// the subscription with an error, which restarts ingestion from the cursor.
	headBufferSize = 64

	// catchupBatchBlocks bounds one eth_getLogs fetch while filling a gap.
	// Raw topic matches average ~470 per mainnet block (ERC-20 Transfers share
	// the ERC-721 signature and are only discarded by eventset.Keep) and busy
	// stretches run well above that: measured live, a 20-block batch tripped
	// Infura's 10k-result cap. Ten blocks is ~4.7k logs on average.
	catchupBatchBlocks = 10

	// catchupLogEvery is the progress cadence (in batches) during a gap fill.
	catchupLogEvery = 50
)

// ErrCatchupTooLarge is returned when the gap between the cursor and the chain
// head exceeds Config.MaxCatchupBlocks. It is deliberately fatal: a gap that
// large is a stale database or an unreviewed rewind, and walking it silently
// would cost a long unattended RPC walk. Raise the knob deliberately.
var ErrCatchupTooLarge = errors.New("ingestion catch-up exceeds max_catchup_blocks")

// Config tunes the head-following loop.
type Config struct {
	// MaxCatchupBlocks bounds the block range fetched to reach the chain head
	// (from the cursor after a restart or socket drop). 0 = unbounded.
	MaxCatchupBlocks uint64
	// ConfirmationBlocks is how many blocks behind the newest head ingestion
	// writes: a block is fetched only once head - ConfirmationBlocks reaches
	// it. 0 writes the tip immediately.
	ConfirmationBlocks uint64
}

// Sink receives each confirmed batch. WriteRange must be atomic: blocks and
// logs for [from, to] plus the cursor move to `to`, or nothing.
type Sink interface {
	WriteRange(ctx context.Context, from, to uint64, blocks []logstore.Block, logs []types.Log) error
}

// Subscriber follows the chain head and hands confirmed batches to a Sink.
type Subscriber struct {
	client  chain.EthClient
	fetcher *Fetcher
	sink    Sink
	cfg     Config
}

// NewSubscriber creates a Subscriber.
func NewSubscriber(cfg Config, client chain.EthClient, fetcher *Fetcher, sink Sink) *Subscriber {
	return &Subscriber{client: client, fetcher: fetcher, sink: sink, cfg: cfg}
}

// streamState is the subscriber's position: the next block to write, the
// lower bound below which nothing is ever written (the caller's fromBlock — a
// future start_block must stay a hard boundary even while heads arrive below
// it), the highest head seen, and the heads received at heights not yet
// written (plus the last written one), keyed by height, for reorg accounting.
type streamState struct {
	next       uint64
	lowerBound uint64
	tip        uint64
	heads      map[uint64]*chain.BlockHead
}

// Run streams blocks from fromBlock onward, driven by the newHeads
// subscription: each new head triggers eth_getLogs fetches covering every
// block not yet written, up to head - ConfirmationBlocks.
//
// Trade-offs: blocks land ConfirmationBlocks blocks (≈12 s each) plus one
// fetch round-trip after the tip. That lag is the reorg strategy — see
// planRange: a written block is never rewound automatically, so blocks are
// written only after the chain has built ConfirmationBlocks on top, and a
// reorg deeper than that is reported as an error, never replayed (the
// operator rewinds with `ff-eth-logs rewind`).
//
// Constraints: any subscription error, fetch failure or sink failure returns
// and ends the process; the supervisor restarts it and it resumes from the
// durable cursor. There is no in-process reconnect, matching the indexer.
func (s *Subscriber) Run(ctx context.Context, fromBlock uint64) error {
	heads := make(chan *chain.BlockHead, headBufferSize)
	sub, err := s.client.SubscribeNewHead(ctx, heads)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new heads: %w", err)
	}
	defer func() {
		logger.InfoCtx(ctx, "Unsubscribing from ethereum new heads")
		sub.Unsubscribe()
		logger.InfoCtx(ctx, "Unsubscribed from ethereum new heads")
	}()

	state := streamState{next: fromBlock, lowerBound: fromBlock, heads: map[uint64]*chain.BlockHead{}}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("new heads subscription error: %w", err)
		case head := <-heads:
			from, to, ok, err := s.planRange(ctx, &state, append([]*chain.BlockHead{head}, drainHeads(heads)...))
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if err := s.ingestRange(ctx, &state, from, to); err != nil {
				return err
			}
			state.advance(to)
		}
	}
}

// drainHeads returns every head already queued behind the one just read.
func drainHeads(heads <-chan *chain.BlockHead) []*chain.BlockHead {
	var queued []*chain.BlockHead
	for {
		select {
		case h := <-heads:
			queued = append(queued, h)
		default:
			return queued
		}
	}
}

// planRange records the received heads and returns the block range to write,
// or ok=false when the chain has not yet confirmed anything new.
//
// Reorg accounting, by height relative to `next` (the first unwritten block):
//   - below the lower bound: ignored — a future start_block is a hard boundary;
//   - at or above next: recorded; a replacement (different hash at the same
//     height) simply overwrites — a shallow reorg absorbed by the confirmation
//     lag, logged at info;
//   - below next: a written height was replaced, i.e. the reorg is deeper than
//     ConfirmationBlocks. It is logged as an error naming the affected heights
//     and NOT rewritten automatically — the served data for those heights is
//     stale until an operator rewinds (docs/operations.md).
//
// A head whose parent disagrees with the retained chain is reconciled against
// canonical heads by number (see reconcile), so a deep reorg announced only by
// a later tip is still found and reported.
func (s *Subscriber) planRange(ctx context.Context, st *streamState, batch []*chain.BlockHead) (from, to uint64, ok bool, err error) {
	for _, h := range batch {
		if err := s.record(ctx, st, h); err != nil {
			return 0, 0, false, err
		}
	}
	// The catch-up bound covers the whole gap to the tip, pending window
	// included: measuring only the confirmed range would let a gap of
	// max+lag through, and a large lag would defer an oversized gap past the
	// bound one block at a time.
	if s.cfg.MaxCatchupBlocks > 0 && st.tip >= st.next && st.tip-st.next+1 > s.cfg.MaxCatchupBlocks {
		return 0, 0, false, fmt.Errorf("%w: need blocks %d-%d (%d blocks, max %d); rewind deliberately or raise ethereum.max_catchup_blocks",
			ErrCatchupTooLarge, st.next, st.tip, st.tip-st.next+1, s.cfg.MaxCatchupBlocks)
	}
	if st.tip < s.cfg.ConfirmationBlocks {
		return 0, 0, false, nil
	}
	to = st.tip - s.cfg.ConfirmationBlocks
	if to < st.next {
		return 0, 0, false, nil
	}
	// The written boundary must carry a canonical hash for later
	// reconciliation to compare against. Received heads cover it in steady
	// state; after a (re)subscribe the first head can sit above it.
	if _, ok := st.heads[to]; !ok {
		boundary, err := s.client.HeadByNumber(ctx, to)
		if err != nil {
			return 0, 0, false, fmt.Errorf("fetch emitted boundary head %d: %w", to, err)
		}
		st.heads[to] = boundary
	}
	return st.next, to, true, nil
}

// record stores one head according to the rules in planRange, reconciling
// the retained chain when the head's parent disagrees with it.
func (s *Subscriber) record(ctx context.Context, st *streamState, h *chain.BlockHead) error {
	n := uint64(h.Number)
	if n < st.lowerBound {
		logger.DebugCtx(ctx, "Ignoring ethereum head below start block",
			zap.Uint64("head", n), zap.Uint64("startBlock", st.lowerBound))
		return nil
	}
	prev, seen := st.heads[n]
	if n < st.next {
		if seen && prev.Hash == h.Hash {
			return nil // duplicate notification of an already-written head
		}
		st.reportDeepReorg(ctx, n, h.Hash)
		return nil
	}
	// Reconcile before retaining: a head is only allowed to extend the
	// retained chain (and raise the confirmation tip) once its ancestry agrees
	// with it. A stale tip — one whose parent the node itself no longer
	// considers canonical — must not shorten the lag for everyone else.
	stale, replaced, err := s.reconcile(ctx, st, h)
	if err != nil {
		return err
	}
	if replaced {
		// The walk swapped retained heads for canonical ones from n-1 down, so
		// anything retained above n-1 descended from the old branch. Drop
		// those and lower the tip whether or not the incoming head survives.
		st.truncateAbove(n - 1)
	}
	if stale {
		logger.DebugCtx(ctx, "Ignoring stale ethereum head (parent is not canonical)",
			zap.Uint64("height", n), zap.String("hash", h.Hash.Hex()), zap.String("parent", h.ParentHash.Hex()))
		return nil
	}
	if seen && prev.Hash != h.Hash {
		logger.InfoCtx(ctx, "Ethereum shallow reorg absorbed within confirmation lag",
			zap.Uint64("height", n), zap.String("old", prev.Hash.Hex()), zap.String("new", h.Hash.Hex()))
		st.truncateAbove(n)
	}
	st.heads[n] = h
	if n > st.tip {
		st.tip = n
	}
	return nil
}

// truncateAbove forgets every retained head above height and makes height the
// tip: the confirmation depth restarts from the replacement branch.
func (st *streamState) truncateAbove(height uint64) {
	for m := range st.heads {
		if m > height {
			delete(st.heads, m)
		}
	}
	st.tip = height
}

// reconcile handles a head whose parent disagrees with the retained head at
// the previous height: the chain reorganized somewhere below it, and a
// provider may announce that only through this later tip. It walks canonical
// heads by number (wire hashes) down from the parent until the retained chain
// matches again, replacing stale retained heads and bridging heights no head
// was received for. Reaching a written height with a different hash is a reorg
// deeper than the lag: reported, never replayed. Fetches happen only on a
// known mismatch and are bounded by the retained window.
//
// It returns stale=true when the node says the retained or canonical chain
// disagrees with the new head's ancestry — the new head must not be retained
// — and replaced=true when any retained head was swapped for the canonical
// one. Not covered: a deep reorg that spans a process restart — the retained
// heads live in memory and the cursor stores no hash.
func (s *Subscriber) reconcile(ctx context.Context, st *streamState, h *chain.BlockHead) (stale, replaced bool, err error) {
	n := uint64(h.Number)
	if n == 0 {
		return false, false, nil
	}
	// Bridging unreceived heights is only meaningful once a written boundary
	// is retained to walk down to; before the first write there is nothing a
	// reorg could have orphaned, so unretained heights end the walk.
	_, bridge := st.heads[st.next-1]
	expected := h.ParentHash
	for k := n - 1; ; k-- {
		retained, ok := st.heads[k]
		if ok && retained.Hash == expected {
			return false, replaced, nil // the chains rejoin here
		}
		if !ok && (!bridge || k < st.next) {
			return false, replaced, nil // nothing retained to reconcile against
		}
		canonical, err := s.client.HeadByNumber(ctx, k)
		if err != nil {
			return false, replaced, fmt.Errorf("reconcile reorg at height %d: %w", k, err)
		}
		if ok && retained.Hash != canonical.Hash {
			replaced = true
			if k < st.next {
				st.reportDeepReorg(ctx, k, canonical.Hash)
			} else {
				logger.InfoCtx(ctx, "Ethereum shallow reorg absorbed within confirmation lag",
					zap.Uint64("height", k), zap.String("old", retained.Hash.Hex()), zap.String("new", canonical.Hash.Hex()))
			}
		}
		st.heads[k] = canonical
		if canonical.Hash != expected {
			// The ancestry the incoming head claims is not what the node holds
			// canonical at this height: the incoming head is stale.
			return true, replaced, nil
		}
		expected = canonical.ParentHash
		if k == 0 {
			return false, replaced, nil
		}
	}
}

// reportDeepReorg logs the operator-visible signal for a written block that
// the chain has since replaced. It reaches Sentry (error level).
func (st *streamState) reportDeepReorg(ctx context.Context, height uint64, newHash common.Hash) {
	logger.ErrorCtx(ctx, errors.New("ethereum reorg deeper than confirmation lag: a written block was replaced"),
		zap.Uint64("height", height), zap.Uint64("lastWritten", st.next-1),
		zap.String("newHash", newHash.Hex()),
		zap.String("hint", "logs for the affected heights are stale; run `ff-eth-logs rewind -to <height-1>` and restart"))
}

// advance moves past `to` and forgets heads below the last written height.
func (st *streamState) advance(to uint64) {
	st.next = to + 1
	for n := range st.heads {
		if n+1 < st.next {
			delete(st.heads, n)
		}
	}
}

// ingestRange fetches and writes every warehouse log in [from, to] in bounded
// batches so a long catch-up streams through memory instead of materializing
// the whole range. Each batch is committed (cursor included) before the next
// is fetched, so a later failure never re-scans this one or trips the
// catch-up bound on it.
func (s *Subscriber) ingestRange(ctx context.Context, st *streamState, from, to uint64) error {
	span := to - from + 1
	if span > 1 {
		logger.InfoCtx(ctx, "Ethereum ingestion catching up to head",
			zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to), zap.Uint64("blocks", span))
	}
	batches := 0
	for batchFrom := from; batchFrom <= to; batchFrom += catchupBatchBlocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		batchTo := min(batchFrom+catchupBatchBlocks-1, to)
		if err := s.ingestBatch(ctx, st, batchFrom, batchTo); err != nil {
			return err
		}
		batches++
		if batches%catchupLogEvery == 0 {
			logger.InfoCtx(ctx, "Ethereum ingestion catch-up progress",
				zap.Uint64("throughBlock", batchTo), zap.Uint64("targetBlock", to), zap.Int("batches", batches))
		}
	}
	return nil
}

// ingestBatch fetches one batch, drops logs outside the warehouse shape, and
// writes blocks + logs + cursor atomically.
func (s *Subscriber) ingestBatch(ctx context.Context, st *streamState, from, to uint64) error {
	logs, err := s.fetcher.FetchIngestionLogs(ctx, from, to)
	if err != nil {
		return fmt.Errorf("fetch ingestion logs for blocks %d-%d: %w", from, to, err)
	}
	kept := logs[:0]
	for _, l := range logs {
		if eventset.Keep(l.Topics, l.Address) {
			kept = append(kept, l)
		}
	}
	blocks, err := s.blockMetadata(ctx, st, from, to)
	if err != nil {
		return err
	}
	if err := s.sink.WriteRange(ctx, from, to, blocks, kept); err != nil {
		return fmt.Errorf("write blocks %d-%d: %w", from, to, err)
	}
	logger.DebugCtx(ctx, "Wrote confirmed blocks",
		zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to), zap.Int("logs", len(kept)), zap.Int("fetched", len(logs)))
	return nil
}

// blockMetadata returns (number, hash, timestamp) for every block in
// [from, to], from the retained heads when the subscription delivered them
// (steady state) and from eth_getBlockByNumber otherwise (catch-up).
func (s *Subscriber) blockMetadata(ctx context.Context, st *streamState, from, to uint64) ([]logstore.Block, error) {
	blocks := make([]logstore.Block, 0, to-from+1)
	for n := from; n <= to; n++ {
		head, ok := st.heads[n]
		if !ok {
			var err error
			if head, err = s.client.HeadByNumber(ctx, n); err != nil {
				return nil, fmt.Errorf("fetch block %d metadata: %w", n, err)
			}
			st.heads[n] = head
		}
		blocks = append(blocks, logstore.Block{Number: n, Hash: head.Hash, Timestamp: uint64(head.Timestamp)})
	}
	return blocks, nil
}
