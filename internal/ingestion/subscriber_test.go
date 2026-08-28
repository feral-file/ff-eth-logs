package ingestion_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/ingestion"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
	"github.com/feral-file/ff-eth-logs/internal/mocks"
)

// baseTimestamp + height is every synthetic head's timestamp, so a test can
// check that block metadata came from the head it expects.
const baseTimestamp = uint64(1_700_000_000)

// headChain builds parent-linked heads so the subscriber's continuity check
// sees a canonical chain unless a test deliberately breaks it. Hashes are
// synthetic: the subscriber only ever compares them, never recomputes them.
type headChain struct {
	last     *chain.BlockHead
	byHeight map[uint64]*chain.BlockHead
	seq      uint64
}

func (c *headChain) hash(n uint64, tag string) common.Hash {
	c.seq++
	return common.BytesToHash([]byte(fmt.Sprintf("%s-%d-%d", tag, n, c.seq)))
}

func (c *headChain) store(h *chain.BlockHead) *chain.BlockHead {
	if c.byHeight == nil {
		c.byHeight = map[uint64]*chain.BlockHead{}
	}
	c.byHeight[uint64(h.Number)] = h
	return h
}

func (c *headChain) remember(h *chain.BlockHead) *chain.BlockHead {
	c.last = c.store(h)
	return h
}

// next returns a head at height n whose parent is the previously built head.
func (c *headChain) next(n uint64) *chain.BlockHead {
	h := &chain.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "canonical"), Timestamp: hexutil.Uint64(baseTimestamp + n)}
	if c.last != nil {
		h.ParentHash = c.last.Hash
	}
	return c.remember(h)
}

// fork returns a replacement head at height n: a different block that still
// descends from the head built at n-1 (a one-block reorg at n). It becomes the
// new tip.
func (c *headChain) fork(n uint64) *chain.BlockHead {
	h := &chain.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "fork"), ParentHash: common.HexToHash("0xdead"), Timestamp: hexutil.Uint64(baseTimestamp + n)}
	if parent, ok := c.byHeight[n-1]; ok {
		h.ParentHash = parent.Hash
	}
	return c.remember(h)
}

// orphanTip returns a head at height n whose parent is a block this chain has
// never seen — a reorg below n announced only by this later tip.
func (c *headChain) orphanTip(n uint64, parent common.Hash) *chain.BlockHead {
	return c.remember(&chain.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "orphan-tip"), ParentHash: parent})
}

// canonical returns the head built at n, synthesizing (and keeping) one for a
// height no test built — the eth_getBlockByNumber answer during a catch-up.
func (c *headChain) canonical(n uint64) *chain.BlockHead {
	if h, ok := c.byHeight[n]; ok {
		return h
	}
	h := &chain.BlockHead{Number: hexutil.Uint64(n), Hash: c.hash(n, "canonical"), Timestamp: hexutil.Uint64(baseTimestamp + n)}
	if parent, ok := c.byHeight[n-1]; ok {
		h.ParentHash = parent.Hash
	}
	return c.store(h)
}

func head(n uint64, hash, parent common.Hash) *chain.BlockHead {
	return &chain.BlockHead{Number: hexutil.Uint64(n), Hash: hash, ParentHash: parent, Timestamp: hexutil.Uint64(baseTimestamp + n)}
}

type mockSubscription struct {
	errCh chan error
}

func (m *mockSubscription) Unsubscribe() { close(m.errCh) }

func (m *mockSubscription) Err() <-chan error { return m.errCh }

// sinkCall is one WriteRange invocation as the fake sink saw it.
type sinkCall struct {
	From, To uint64
	Blocks   []logstore.Block
	Logs     []types.Log
}

// fakeSink records every WriteRange and runs the step registered for that
// call's ordinal (push more heads, cancel the run, fail the write), which is
// how tests script what happens between confirmed batches.
type fakeSink struct {
	calls   []sinkCall
	steps   []func(from, to uint64) error
	stored  map[uint64]common.Hash // block hashes the warehouse holds (seeded + written)
	rewinds []uint64
}

func (s *fakeSink) WriteRange(_ context.Context, from, to uint64, blocks []logstore.Block, logs []types.Log) error {
	s.calls = append(s.calls, sinkCall{From: from, To: to, Blocks: slices.Clone(blocks), Logs: slices.Clone(logs)})
	if s.stored == nil {
		s.stored = map[uint64]common.Hash{}
	}
	for _, b := range blocks {
		s.stored[b.Number] = b.Hash
	}
	if i := len(s.calls) - 1; i < len(s.steps) && s.steps[i] != nil {
		return s.steps[i](from, to)
	}
	return nil
}

func (s *fakeSink) StoredBlockHash(_ context.Context, n uint64) (common.Hash, bool, error) {
	h, ok := s.stored[n]
	return h, ok, nil
}

func (s *fakeSink) Rewind(_ context.Context, to uint64) error {
	s.rewinds = append(s.rewinds, to)
	for n := range s.stored {
		if n > to {
			delete(s.stored, n)
		}
	}
	return nil
}

// seedStored pretends blocks [from, to] were backfilled with the chain's
// canonical hashes, so a fork-point walk has persisted hashes to compare.
func (s *fakeSink) seedStored(c *headChain, from, to uint64) {
	if s.stored == nil {
		s.stored = map[uint64]common.Hash{}
	}
	for n := from; n <= to; n++ {
		s.stored[n] = c.canonical(n).Hash
	}
}

func (s *fakeSink) ranges() [][2]uint64 {
	var out [][2]uint64
	for _, c := range s.calls {
		out = append(out, [2]uint64{c.From, c.To})
	}
	return out
}

// thenPush queues heads for the next loop iteration after a write.
func thenPush(push func(*chain.BlockHead), heads ...*chain.BlockHead) func(uint64, uint64) error {
	return func(uint64, uint64) error {
		for _, h := range heads {
			push(h)
		}
		return nil
	}
}

// thenCancel ends the run after a write.
func thenCancel(cancel context.CancelFunc) func(uint64, uint64) error {
	return func(uint64, uint64) error {
		cancel()
		return nil
	}
}

func thenFail(err error) func(uint64, uint64) error {
	return func(uint64, uint64) error { return err }
}

// logsFor matches an eth_getLogs query for exactly [from, to].
func logsFor(from, to uint64) gomock.Matcher {
	return gomock.Cond(func(x any) bool {
		q, ok := x.(ethereum.FilterQuery)
		return ok && q.FromBlock != nil && q.ToBlock != nil && q.FromBlock.Uint64() == from && q.ToBlock.Uint64() == to
	})
}

// fixture wires a mock client whose newHeads subscription has the given heads
// queued at subscribe time, a fake sink, and a push function for delivering
// later heads (call it from a sink step or a mock callback — heads pushed while
// the subscriber is mid-batch are read afterwards, so tests can pin the
// write-per-head sequence without racing the head coalescing).
type fixture struct {
	ctrl   *gomock.Controller
	client *mocks.MockEthClient
	sink   *fakeSink
	push   func(*chain.BlockHead)
	subErr chan error
}

func newFixture(t *testing.T, initial ...*chain.BlockHead) *fixture {
	t.Helper()
	f := &fixture{ctrl: gomock.NewController(t), sink: &fakeSink{}, subErr: make(chan error, 1)}
	f.client = mocks.NewMockEthClient(f.ctrl)
	var headCh chan<- *chain.BlockHead
	f.push = func(h *chain.BlockHead) { headCh <- h }
	f.client.EXPECT().
		SubscribeNewHead(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, ch chan<- *chain.BlockHead) (ethereum.Subscription, error) {
			headCh = ch
			for _, h := range initial {
				f.push(h)
			}
			return &mockSubscription{errCh: f.subErr}, nil
		})
	return f
}

// run builds the subscriber over the fixture's client and sink and runs it.
func (f *fixture) run(ctx context.Context, cfg ingestion.Config, from uint64) error {
	clock := mocks.NewMockClock(f.ctrl)
	clock.EXPECT().Sleep(gomock.Any()).AnyTimes()
	fetcher := ingestion.NewFetcher(f.client, clock, 0)
	return ingestion.NewSubscriber(cfg, f.client, fetcher, f.sink).Run(ctx, from)
}

// expectLogs pins one eth_getLogs call for [from, to] returning logs.
func (f *fixture) expectLogs(from, to uint64, logs ...types.Log) *gomock.Call {
	return f.client.EXPECT().FilterLogs(gomock.Any(), logsFor(from, to)).Return(logs, nil)
}

// serveLogs answers any eth_getLogs query with the logs of the blocks it covers.
func (f *fixture) serveLogs(byBlock map[uint64][]types.Log) {
	f.client.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			var logs []types.Log
			for b := q.FromBlock.Uint64(); b <= q.ToBlock.Uint64(); b++ {
				logs = append(logs, byBlock[b]...)
			}
			return logs, nil
		})
}

// serveHeads answers any eth_getBlockByNumber with the chain's canonical head,
// for tests that do not pin the metadata fetches of a catch-up.
func (f *fixture) serveHeads(c *headChain) {
	f.client.EXPECT().HeadByNumber(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, n uint64) (*chain.BlockHead, error) { return c.canonical(n), nil })
}

// expectHead pins one eth_getBlockByNumber(n) returning h.
func (f *fixture) expectHead(n uint64, h *chain.BlockHead) *gomock.Call {
	return f.client.EXPECT().HeadByNumber(gomock.Any(), n).Return(h, nil)
}

// expectMetadataHeads pins the ascending metadata fetches a catch-up over
// [from, to] makes for heights no subscription head covered.
func (f *fixture) expectMetadataHeads(c *headChain, from, to uint64) []*gomock.Call {
	var calls []*gomock.Call
	for n := from; n <= to; n++ {
		calls = append(calls, f.expectHead(n, c.canonical(n)))
	}
	return calls
}

func inOrder(calls ...*gomock.Call) {
	args := make([]any, len(calls))
	for i, c := range calls {
		args[i] = c
	}
	gomock.InOrder(args...)
}

func transferLog(block uint64, index uint) types.Log {
	return types.Log{
		BlockNumber: block,
		Index:       index,
		Topics:      []common.Hash{eventset.Transfer, {}, {}, {}},
	}
}

func blockOf(h *chain.BlockHead) logstore.Block {
	return logstore.Block{Number: uint64(h.Number), Hash: h.Hash, Timestamp: uint64(h.Timestamp)}
}

// TestRun_WritesEachHeadBlock pins the steady-state contract: with no gap and
// no lag, every head triggers exactly one fetch and one write for that block,
// with the block's metadata taken from the received head (no
// eth_getBlockByNumber) and the kept logs attached.
func TestRun_WritesEachHeadBlock(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	h100 := c.next(100)
	f := newFixture(t, h100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h101 := c.next(101)
	log100, log101 := transferLog(100, 3), transferLog(101, 0)
	inOrder(
		f.expectLogs(100, 100, log100),
		f.expectLogs(101, 101, log101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, h101), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []sinkCall{
		{From: 100, To: 100, Blocks: []logstore.Block{blockOf(h100)}, Logs: []types.Log{log100}},
		{From: 101, To: 101, Blocks: []logstore.Block{blockOf(h101)}, Logs: []types.Log{log101}},
	}, f.sink.calls)
}

// TestRun_FillsGapToHead pins the resume contract: when the first head is
// ahead of fromBlock (restart or socket drop), the whole gap is written before
// live blocks continue from head+1, with metadata for the unreceived heights
// fetched by number.
func TestRun_FillsGapToHead(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(105))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f.serveLogs(nil)
	f.serveHeads(c)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, c.next(106)), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{MaxCatchupBlocks: 10}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 105}, {106, 106}}, f.sink.ranges())

	var numbers []uint64
	for _, b := range f.sink.calls[0].Blocks {
		numbers = append(numbers, b.Number)
		assert.Equal(t, c.canonical(b.Number).Hash, b.Hash, "block %d carries the canonical hash", b.Number)
		assert.Equal(t, baseTimestamp+b.Number, b.Timestamp, "block %d carries the head timestamp", b.Number)
	}
	require.Equal(t, []uint64{100, 101, 102, 103, 104, 105}, numbers)
}

// TestRun_CatchupIsBatched pins that a long gap is fetched and written in
// bounded batches of ten, in order, each batch committed before the next is
// fetched.
func TestRun_CatchupIsBatched(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(125))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log105, log115 := transferLog(105, 0), transferLog(115, 0)
	inOrder(
		f.expectLogs(100, 109, log105),
		f.expectLogs(110, 119, log115),
		f.expectLogs(120, 125),
	)
	f.serveHeads(c)
	f.sink.steps = []func(uint64, uint64) error{nil, nil, thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 109}, {110, 119}, {120, 125}}, f.sink.ranges())
	require.Equal(t, []types.Log{log105}, f.sink.calls[0].Logs)
	require.Equal(t, []types.Log{log115}, f.sink.calls[1].Logs)
	require.Empty(t, f.sink.calls[2].Logs)
	require.Len(t, f.sink.calls[2].Blocks, 6)
}

// TestRun_CoalescesQueuedHeads pins that heads queued during a slow batch
// collapse into one range instead of one fetch per head.
func TestRun_CoalescesQueuedHeads(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100), c.next(101), c.next(102))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f.expectLogs(100, 102)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 102}}, f.sink.ranges())
}

// TestRun_WritesOnlyConfirmedBlocks pins the confirmation lag: with K=2, heads
// 100..102 confirm only block 100; head 103 confirms 101. Nothing at or above
// head-K is ever fetched.
func TestRun_WritesOnlyConfirmedBlocks(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100), c.next(101), c.next(102))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inOrder(f.expectLogs(100, 100), f.expectLogs(101, 101))
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, c.next(103)), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{ConfirmationBlocks: 2}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_KeepsOnlyWarehouseShapedLogs pins that eventset.Keep runs before the
// sink: a three-topic (ERC-20) Transfer fetched by the topic0 filter is
// dropped, the four-topic (ERC-721) one is written.
func TestRun_KeepsOnlyWarehouseShapedLogs(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	erc20 := types.Log{BlockNumber: 100, Index: 0, Topics: []common.Hash{eventset.Transfer, {}, {}}}
	erc721 := transferLog(100, 1)
	f.expectLogs(100, 100, erc20, erc721)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, f.sink.calls, 1)
	require.Equal(t, []types.Log{erc721}, f.sink.calls[0].Logs)
}

// TestRun_DuplicateHeadAtWrittenHeightIsIgnored pins that a repeated
// notification of an already-written head is neither a reorg nor a re-write.
func TestRun_DuplicateHeadAtWrittenHeightIsIgnored(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	h100 := c.next(100)
	f := newFixture(t, h100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inOrder(f.expectLogs(100, 100), f.expectLogs(101, 101))
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, h100, c.next(101)), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_FutureStartBlockIsHardLowerBound pins that a start block ahead of the
// chain is honored literally: heads below it are ignored (not treated as
// replaced written blocks), and writing starts exactly there.
func TestRun_FutureStartBlockIsHardLowerBound(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(400), c.next(401), c.next(500))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h501 := c.next(501)
	below := c.fork(499) // a replacement below the start block: ignored
	inOrder(f.expectLogs(500, 500), f.expectLogs(501, 501))
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, below, h501), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 500)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{500, 500}, {501, 501}}, f.sink.ranges())
}

// TestRun_WritesEveryBatchAndStopsOnLateFetchFailure pins per-batch
// durability: each batch is written (cursor included) before the next is
// fetched, so a failure in a later batch leaves the earlier ones written and
// returns the fetch error naming the batch.
func TestRun_WritesEveryBatchAndStopsOnLateFetchFailure(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(125))

	fetchErr := errors.New("provider 503")
	inOrder(
		f.expectLogs(100, 109),
		f.expectLogs(110, 119),
		f.client.EXPECT().FilterLogs(gomock.Any(), logsFor(120, 125)).Return(nil, fetchErr),
	)
	f.serveHeads(c)

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorIs(t, err, fetchErr)
	require.Contains(t, err.Error(), "fetch ingestion logs for blocks 120-125")
	require.Equal(t, [][2]uint64{{100, 109}, {110, 119}}, f.sink.ranges(), "batches before the failure were written; the failed one was not")
}

// TestRun_SinkErrorStops pins that a rejected write ends the run with the
// sink's error, after the earlier writes went through in order.
func TestRun_SinkErrorStops(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(105))

	log103 := transferLog(103, 0)
	inOrder(f.expectLogs(100, 105, log103), f.expectLogs(106, 106))
	f.serveHeads(c)
	sinkErr := errors.New("database closed")
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, c.next(106)), thenFail(sinkErr)}

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorIs(t, err, sinkErr)
	require.Contains(t, err.Error(), "write blocks 106-106")
	require.Equal(t, [][2]uint64{{100, 105}, {106, 106}}, f.sink.ranges())
	require.Equal(t, []types.Log{log103}, f.sink.calls[0].Logs)
}

// TestRun_CatchupTooLargeFails pins the cost guard: a gap wider than
// MaxCatchupBlocks is a fatal, named error before any logs are fetched.
func TestRun_CatchupTooLargeFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(1_000_000))

	err := f.run(context.Background(), ingestion.Config{MaxCatchupBlocks: 50_000}, 1)
	require.ErrorIs(t, err, ingestion.ErrCatchupTooLarge)
	require.Contains(t, err.Error(), "need blocks 1-1000000")
	require.Empty(t, f.sink.calls)
}

// TestRun_CatchupBoundCoversPendingWindow pins that the bound is measured on
// the whole gap to the tip, not just the confirmed range: with lag 2 and max
// 10, a tip 12 blocks past the start is rejected even though only 10 would be
// written now, while a tip 10 past the start is accepted — and the written
// boundary's head, absent from the subscription, is fetched by number.
func TestRun_CatchupBoundCoversPendingWindow(t *testing.T) {
	t.Parallel()
	cfg := ingestion.Config{MaxCatchupBlocks: 10, ConfirmationBlocks: 2}

	t.Run("gap of max+lag is rejected", func(t *testing.T) {
		t.Parallel()
		c := &headChain{}
		f := newFixture(t, c.next(111))

		err := f.run(context.Background(), cfg, 100)
		require.ErrorIs(t, err, ingestion.ErrCatchupTooLarge)
		require.Contains(t, err.Error(), "need blocks 100-111 (12 blocks, max 10)")
	})

	t.Run("gap of exactly max is accepted", func(t *testing.T) {
		t.Parallel()
		c := &headChain{}
		f := newFixture(t, c.next(109))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		boundary := head(107, common.HexToHash("0x107"), common.HexToHash("0x106"))
		inOrder(append([]*gomock.Call{f.expectHead(107, boundary), f.expectLogs(100, 107)}, f.expectMetadataHeads(c, 100, 106)...)...)
		f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

		err := f.run(ctx, cfg, 100)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, [][2]uint64{{100, 107}}, f.sink.ranges())
		require.Equal(t, blockOf(boundary), f.sink.calls[0].Blocks[7], "the boundary block carries the fetched head")
	})
}

// TestRun_UnboundedCatchupWhenZero pins that MaxCatchupBlocks=0 disables the
// guard (the range is still walked in batches).
func TestRun_UnboundedCatchupWhenZero(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(1_000))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f.expectLogs(1, 10)
	f.serveHeads(c)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{1, 10}}, f.sink.ranges())
}

func TestRun_FetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100))

	fetchErr := errors.New("rpc down")
	f.client.EXPECT().FilterLogs(gomock.Any(), logsFor(100, 100)).Return(nil, fetchErr)

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorIs(t, err, fetchErr)
	require.Contains(t, err.Error(), "fetch ingestion logs for blocks 100-100")
	require.Empty(t, f.sink.calls)
}

func TestRun_SubscribeErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := mocks.NewMockEthClient(ctrl)
	subErr := errors.New("dial failed")
	client.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).Return(nil, subErr)
	clock := mocks.NewMockClock(ctrl)

	sub := ingestion.NewSubscriber(ingestion.Config{}, client, ingestion.NewFetcher(client, clock, 0), &fakeSink{})
	err := sub.Run(context.Background(), 100)
	require.ErrorIs(t, err, subErr)
	require.Contains(t, err.Error(), "failed to subscribe to new heads")
}

func TestRun_SubscriptionErrorFails(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	transportErr := errors.New("websocket closed 1006")
	f.subErr <- transportErr

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorIs(t, err, transportErr)
	require.Contains(t, err.Error(), "new heads subscription error")
}

func TestRun_ContextCancelReturnsCtxErr(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.sink.calls)
}
