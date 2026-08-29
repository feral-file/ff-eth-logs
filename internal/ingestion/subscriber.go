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

// Sink is the warehouse as ingestion sees it. WriteRange must be atomic:
// blocks and logs for [from, to] plus the cursor move to `to`, or nothing.
// StoredBlockHash and Rewind serve deep-reorg recovery: the persisted block
// hashes are the only durable record of what was written, so they are what
// the fork point is verified against, and Rewind drops everything above it.
type Sink interface {
	WriteRange(ctx context.Context, from, to uint64, blocks []logstore.Block, logs []types.Log) error
	StoredBlockHash(ctx context.Context, n uint64) (common.Hash, bool, error)
	Rewind(ctx context.Context, to uint64) error
}

// maxForkWalk bounds the search for a deep reorg's common ancestor. Mainnet
// finality is ~64 blocks; a fork deeper than 1024 is not a reorg to recover
// from automatically but an incident to look at.
const maxForkWalk = 1024

// deepReorgError carries the first written height reconcile found replaced,
// so record can run the recovery and then retry the head that revealed it.
type deepReorgError struct{ height uint64 }

func (e *deepReorgError) Error() string {
	return fmt.Sprintf("written block %d was replaced by a reorg deeper than the confirmation lag", e.height)
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
// planRange: blocks are written only after the chain has built
// ConfirmationBlocks on top; a reorg deeper than that is recovered by
// rewinding to the verified common ancestor (recoverDeepReorg) and logged at
// error level so it is visible.
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
	if err := s.verifyResumePoint(ctx, &state); err != nil {
		return err
	}
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

// verifyResumePoint checks the block the stream resumes after (next-1)
// against the canonical chain before the first head is processed. Reason: a
// reorg that replaced written blocks while the process was down leaves no
// retained head to disagree with, so reconcile alone would append canonical
// blocks on top of orphaned ones. Comparing the persisted hash with
// eth_getBlockByNumber closes that gap; a mismatch runs the same
// verified-ancestor rewind as a live deep reorg. When the hash matches (or
// nothing is stored there — a fresh start_block), the canonical head is
// retained so reconciliation has its bridge from the first head on.
func (s *Subscriber) verifyResumePoint(ctx context.Context, st *streamState) error {
	if st.next == 0 {
		return nil
	}
	last := st.next - 1
	stored, ok, err := s.sink.StoredBlockHash(ctx, last)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	canonical, err := s.client.HeadByNumber(ctx, last)
	if err != nil {
		return fmt.Errorf("verify resume point %d: %w", last, err)
	}
	if canonical.Hash == stored {
		st.heads[last] = canonical
		st.tip = last
		return nil
	}
	logger.WarnCtx(ctx, "Written head is no longer canonical at start; a reorg happened while the process was down",
		zap.Uint64("height", last), zap.String("stored", stored.Hex()), zap.String("canonical", canonical.Hash.Hex()))
	if err := s.recoverDeepReorg(ctx, st, last); err != nil {
		return err
	}
	st.lowerBound = min(st.lowerBound, st.next)
	return nil
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
//     ConfirmationBlocks. The verified common ancestor is located against the
//     persisted block hashes, everything above it is rewound, and the stream
//     restarts there (recoverDeepReorg); the event is logged at error level.
//
// A head whose parent disagrees with the retained chain is reconciled against
// canonical heads by number (see reconcile), so a deep reorg announced only by
// a later tip is still found and reported.
func (s *Subscriber) planRange(ctx context.Context, st *streamState, batch []*chain.BlockHead) (from, to uint64, ok bool, err error) {
	// Preflight the bound on the received heads before any of them is
	// reconciled: with a bridge retained (always, after a restart), reconcile
	// walks eth_getBlockByNumber down to it, so a stale cursor would pay the
	// whole gap in header RPCs before the guard below ever ran.
	highest := st.tip
	for _, h := range batch {
		highest = max(highest, uint64(h.Number))
	}
	if err := s.checkCatchupBound(st.next, highest); err != nil {
		return 0, 0, false, err
	}
	for _, h := range batch {
		if err := s.record(ctx, st, h); err != nil {
			return 0, 0, false, err
		}
	}
	if err := s.checkCatchupBound(st.next, st.tip); err != nil {
		return 0, 0, false, err
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

// checkCatchupBound enforces MaxCatchupBlocks on the gap from the first
// unwritten block to `tip`. The bound covers the whole gap, pending window
// included: measuring only the confirmed range would let a gap of max+lag
// through, and a large lag would defer an oversized gap past the bound one
// block at a time.
func (s *Subscriber) checkCatchupBound(next, tip uint64) error {
	if s.cfg.MaxCatchupBlocks > 0 && tip >= next && tip-next+1 > s.cfg.MaxCatchupBlocks {
		return fmt.Errorf("%w: need blocks %d-%d (%d blocks, max %d); rewind deliberately or raise ethereum.max_catchup_blocks",
			ErrCatchupTooLarge, next, tip, tip-next+1, s.cfg.MaxCatchupBlocks)
	}
	return nil
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
		return s.handleWrittenHeightHead(ctx, st, h)
	}
	// Reconcile before retaining: a head is only allowed to extend the
	// retained chain (and raise the confirmation tip) once its ancestry agrees
	// with it. A stale tip — one whose parent the node itself no longer
	// considers canonical — must not shorten the lag for everyone else.
	stale, replaced, err := s.reconcile(ctx, st, h)
	var deep *deepReorgError
	if errors.As(err, &deep) {
		if err := s.recoverDeepReorg(ctx, st, deep.height); err != nil {
			return err
		}
		return s.record(ctx, st, h) // the head is above the rewound position now
	}
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
// deeper than the lag: it returns a deepReorgError for the caller to recover
// from (recoverDeepReorg). Fetches happen only on a
// known mismatch and are bounded by the retained window.
//
// It returns stale=true when the node says the retained or canonical chain
// disagrees with the new head's ancestry — the new head must not be retained
// — and replaced=true when any retained head was swapped for the canonical
// one. A deep reorg that spans a process restart is caught by
// verifyResumePoint before the first head, against the persisted hashes.
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
			if k < st.next {
				// A written height is not canonical any more. The retained
				// window ends here, so the real fork may be lower still; the
				// caller locates it against the persisted hashes and rewinds.
				return false, replaced, &deepReorgError{height: k}
			}
			replaced = true
			logger.InfoCtx(ctx, "Ethereum shallow reorg absorbed within confirmation lag",
				zap.Uint64("height", k), zap.String("old", retained.Hash.Hex()), zap.String("new", canonical.Hash.Hex()))
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

// handleWrittenHeightHead deals with a notification for a height that is
// already written but whose hash is not the retained one. It may be a late
// duplicate of a block we hold, a stale head from a branch the node has
// already abandoned, or a real replacement; only the node's canonical header
// decides, compared with the hash persisted for that height.
func (s *Subscriber) handleWrittenHeightHead(ctx context.Context, st *streamState, h *chain.BlockHead) error {
	n := uint64(h.Number)
	stored, ok, err := s.sink.StoredBlockHash(ctx, n)
	if err != nil {
		return err
	}
	if ok && stored == h.Hash {
		return nil // late re-delivery of the block we wrote
	}
	canonical, err := s.client.HeadByNumber(ctx, n)
	if err != nil {
		return fmt.Errorf("verify written block %d after unexpected head: %w", n, err)
	}
	if ok && canonical.Hash == stored {
		logger.DebugCtx(ctx, "Ignoring stale ethereum head at a written height",
			zap.Uint64("height", n), zap.String("hash", h.Hash.Hex()))
		return nil
	}
	if err := s.recoverDeepReorg(ctx, st, n); err != nil {
		return err
	}
	return s.record(ctx, st, h)
}

// recoverDeepReorg is the response to a written block the chain has since
// replaced: find the highest block below it whose persisted hash still
// matches the canonical header (the verified common ancestor), drop
// everything above it, and restart the stream there so the canonical blocks
// are re-fetched.
//
// Reason: the indexer only logs a deep reorg because it cannot un-enqueue the
// events it already emitted; the warehouse can, because a rewind is a range
// delete and its writes are idempotent. Deriving the target from the first
// retained mismatch (the old runbook) was wrong: only the last written head
// is retained, so a fork one block lower would have kept a stale block inside
// the advertised coverage. Trade-offs: the walk costs one eth_getBlockByNumber
// per block back to the ancestor; a fork deeper than maxForkWalk, or one
// reaching below the covered interval, is fatal and needs an operator (the
// process restarts into the same detection and keeps failing loudly).
func (s *Subscriber) recoverDeepReorg(ctx context.Context, st *streamState, height uint64) error {
	ancestor, err := s.findForkPoint(ctx, height)
	if err != nil {
		return fmt.Errorf("reorg deeper than confirmation lag replaced written block %d: %w", height, err)
	}
	logger.ErrorCtx(ctx, errors.New("ethereum reorg deeper than confirmation lag: rewinding to the verified common ancestor"),
		zap.Uint64("replacedHeight", height), zap.Uint64("lastWritten", st.next-1),
		zap.Uint64("ancestor", uint64(ancestor.Number)), zap.Uint64("blocksDropped", st.next-1-uint64(ancestor.Number)))
	if err := s.sink.Rewind(ctx, uint64(ancestor.Number)); err != nil {
		return fmt.Errorf("rewind to %d after deep reorg: %w", uint64(ancestor.Number), err)
	}
	// Every retained head descends from a branch the walk just proved stale
	// (or is the head that revealed it and will be re-recorded); restart the
	// window from the ancestor so reconciliation has a canonical anchor.
	st.heads = map[uint64]*chain.BlockHead{uint64(ancestor.Number): ancestor}
	st.next = uint64(ancestor.Number) + 1
	st.tip = uint64(ancestor.Number)
	return nil
}

// findForkPoint walks down from height-1 until the persisted hash equals the
// canonical header, returning that canonical head. Heights the warehouse
// does not hold end the walk with an error: the reorg reaches below coverage.
func (s *Subscriber) findForkPoint(ctx context.Context, height uint64) (*chain.BlockHead, error) {
	for k := height; k > 0; k-- {
		n := k - 1
		if height-n > maxForkWalk {
			return nil, fmt.Errorf("no common ancestor within %d blocks below %d; verify the chain and rewind manually", maxForkWalk, height)
		}
		stored, ok, err := s.sink.StoredBlockHash(ctx, n)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("block %d is not stored, the fork reaches below warehouse coverage; rebuild from the export", n)
		}
		canonical, err := s.client.HeadByNumber(ctx, n)
		if err != nil {
			return nil, fmt.Errorf("fetch canonical block %d: %w", n, err)
		}
		if canonical.Hash == stored {
			return canonical, nil
		}
	}
	return nil, errors.New("no common ancestor above genesis")
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

// batchConsistencyAttempts bounds how often a batch is refetched because
// the chain moved between its log fetch and its header fetch.
const batchConsistencyAttempts = 3

// ingestBatch fetches one batch, drops logs outside the warehouse shape, and
// writes blocks + logs + cursor atomically — but only once the logs and the
// block metadata describe the same chain.
//
// Reason: eth_getLogs and the block headers are two calls (and the headers
// may be retained from an earlier notification). A reorg landing between
// them would pair old-branch logs with new-branch hashes, and a later fork
// walk would then trust those hashes and stop above the stale logs. Every
// log carries the hash of the block it came from, so the batch is accepted
// only when each log's blockHash equals the metadata hash for its height;
// otherwise the retained heads for the disagreeing heights are dropped (so
// they are refetched canonical) and the whole batch is fetched again.
// Trade-offs: a provider that omits blockHash cannot be verified this way
// (the zero hash is skipped); mainnet providers always populate it.
func (s *Subscriber) ingestBatch(ctx context.Context, st *streamState, from, to uint64) error {
	for attempt := 1; ; attempt++ {
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
		moved := disagreeingHeights(kept, blocks)
		if len(moved) == 0 {
			if err := s.sink.WriteRange(ctx, from, to, blocks, kept); err != nil {
				return fmt.Errorf("write blocks %d-%d: %w", from, to, err)
			}
			logger.DebugCtx(ctx, "Wrote confirmed blocks",
				zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to), zap.Int("logs", len(kept)), zap.Int("fetched", len(logs)))
			return nil
		}
		if attempt == batchConsistencyAttempts {
			return fmt.Errorf("blocks %v kept changing between log and header fetches for batch %d-%d (%d attempts); the chain is unstable at the confirmation depth",
				moved, from, to, attempt)
		}
		logger.WarnCtx(ctx, "Block hashes moved between the log fetch and the header fetch; refetching the batch",
			zap.Uint64("fromBlock", from), zap.Uint64("toBlock", to), zap.Uint64s("heights", moved), zap.Int("attempt", attempt))
		for _, n := range moved {
			delete(st.heads, n)
		}
	}
}

// disagreeingHeights returns the block numbers whose logs report a different
// block hash than the metadata for that height, ascending.
func disagreeingHeights(logs []types.Log, blocks []logstore.Block) []uint64 {
	byNumber := make(map[uint64]common.Hash, len(blocks))
	for _, b := range blocks {
		byNumber[b.Number] = b.Hash
	}
	seen := map[uint64]struct{}{}
	var out []uint64
	for _, l := range logs {
		if l.BlockHash == (common.Hash{}) {
			continue
		}
		if h, ok := byNumber[l.BlockNumber]; ok && h != l.BlockHash {
			if _, dup := seen[l.BlockNumber]; !dup {
				seen[l.BlockNumber] = struct{}{}
				out = append(out, l.BlockNumber)
			}
		}
	}
	return out
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
