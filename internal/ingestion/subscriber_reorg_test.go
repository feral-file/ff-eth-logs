package ingestion_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/ingestion"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

// lagged is the confirmation-lag configuration every reorg test uses unless
// it says otherwise: two blocks behind the tip.
var lagged = ingestion.Config{ConfirmationBlocks: 2}

// TestRun_ShallowReorgWithinLagIsAbsorbed pins the reorg strategy: a
// replacement head above the written range (inside the lag) changes nothing
// already written and triggers no re-fetch; writing simply continues on the
// new canonical chain once it is confirmed.
func TestRun_ShallowReorgWithinLagIsAbsorbed(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100), c.next(101), c.next(102))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 100 written; then 102 is replaced (fork) and 103 builds on the fork.
	// Tip 103 confirms 101 only — no re-write of 100, 102 not yet written.
	inOrder(f.expectLogs(100, 100), f.expectLogs(101, 101))
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, c.fork(102), c.next(103)), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_DeepReorgIsReportedNotReplayed pins that a replacement of an
// already-written height is never re-fetched: the subscriber reports it and
// continues from the next unwritten height.
func TestRun_DeepReorgIsReportedNotReplayed(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fork100 := c.fork(100)
	inOrder(
		f.expectLogs(100, 100),
		// 101's parent is the fork, not the written 100: reconcile confirms 100 was replaced.
		f.expectHead(100, fork100),
		// Only 101 is fetched; the replaced 100 is reported, not replayed.
		f.expectLogs(101, 101),
	)
	// 100 written (no lag); then 100 itself is replaced and 101 builds on the fork.
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, fork100, c.next(101)), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_TipOnlyReorgReconcilesToWrittenBoundary pins reconcile: a reorg
// announced only by a later tip (B103 whose parent is an unseen B102) walks
// canonical heads down by number until the retained chain matches — here at
// the written block 100 — replacing stale retained heads, with no re-fetch of
// anything written, and the replacement B101 is what gets written.
func TestRun_TipOnlyReorgReconcilesToWrittenBoundary(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	_ = a101
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b101 := head(101, common.HexToHash("0xb101"), a100.Hash) // rejoins the written chain at 100
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := c.orphanTip(103, b102.Hash)
	inOrder(
		f.expectLogs(100, 100),
		f.expectHead(102, b102),
		f.expectHead(101, b101),
		// retained 100 == b101's parent: walk stops; 101 is confirmed by tip 103.
		f.expectLogs(101, 101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
	require.Equal(t, []logstore.Block{blockOf(b101)}, f.sink.calls[1].Blocks, "the reconciled head is the block metadata")
}

// TestRun_StaleTipDoesNotShortenLag pins that a delayed head whose parent the
// node no longer considers canonical is discarded: it must not be retained or
// raise the confirmation tip, so the lag keeps its full depth for the
// canonical chain (which then confirms 101 only when A103 arrives).
func TestRun_StaleTipDoesNotShortenLag(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	staleB103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	a103 := c.next(103) // canonical, on A102
	inOrder(
		f.expectLogs(100, 100),
		// B103's parent disagrees with retained A102; the node confirms A102 is canonical.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				f.push(a103) // arrives after the stale tip was processed
				return a102, nil
			}),
		// No fetch of 101 on the stale tip; A103 confirms it.
		f.expectLogs(101, 101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, staleB103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_ReorgAfterSingleHeadCatchupReachesBoundary pins the initial-gap
// case: the first head (A105, lag 2) writes 100..103 with no received head at
// 103, so the boundary hash is fetched and retained (and 100..102 metadata is
// fetched by number); a reorg announced only by B106 then bridges the
// unreceived 104 and reaches 103, where the replaced written block is
// reported — and writing continues from 104 by number, never replaying 103.
func TestRun_ReorgAfterSingleHeadCatchupReachesBoundary(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(105))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a103 := head(103, common.HexToHash("0xa103"), common.HexToHash("0xa102"))
	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xa102"))
	b104 := head(104, common.HexToHash("0xb104"), b103.Hash)
	b105 := head(105, common.HexToHash("0xb105"), b104.Hash)
	b106 := c.orphanTip(106, b105.Hash)
	calls := []*gomock.Call{f.expectHead(103, a103), f.expectLogs(100, 103)} // written boundary retained
	calls = append(calls, f.expectMetadataHeads(c, 100, 102)...)             // logs first, then metadata
	calls = append(calls,
		f.expectHead(105, b105), // retained A105 replaced
		f.expectHead(104, b104), // bridged (never received)
		f.expectHead(103, b103), // written boundary replaced: reported
		f.expectLogs(104, 104),
	)
	inOrder(calls...)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b106), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 103}, {104, 104}}, f.sink.ranges())
	require.Equal(t, []logstore.Block{blockOf(b104)}, f.sink.calls[1].Blocks, "104 is written from the bridged canonical head")
}

// TestRun_ReorgAboveBoundaryAfterCatchupIsAbsorbed is the shallow
// counterpart: the bridged walk rejoins the retained chain at the boundary,
// so nothing is reported and writing simply continues.
func TestRun_ReorgAboveBoundaryAfterCatchupIsAbsorbed(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(105))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a103 := head(103, common.HexToHash("0xa103"), common.HexToHash("0xa102"))
	b104 := head(104, common.HexToHash("0xb104"), a103.Hash) // rejoins at the boundary
	b105 := head(105, common.HexToHash("0xb105"), b104.Hash)
	b106 := c.orphanTip(106, b105.Hash)
	calls := []*gomock.Call{f.expectHead(103, a103), f.expectLogs(100, 103)}
	calls = append(calls, f.expectMetadataHeads(c, 100, 102)...)
	calls = append(calls,
		f.expectHead(105, b105),
		f.expectHead(104, b104),
		// 103 retained == b104's parent: rejoin, no fetch of 103.
		f.expectLogs(104, 104),
	)
	inOrder(calls...)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b106), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 103}, {104, 104}}, f.sink.ranges())
}

// TestRun_QueuedDoubleReorgRejectsStaleHead pins the three-branch case:
// retained A102, incoming B103 (parent B102), while the node holds a third
// branch C102. B103's ancestry is not canonical, so it is discarded and
// nothing is written; the retained chain is refreshed to C102, and the
// canonical C103 later confirms 101 without any further reconciliation fetch.
func TestRun_QueuedDoubleReorgRejectsStaleHead(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	_ = a100
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	c102 := head(102, common.HexToHash("0xc102"), a101.Hash)
	c103 := head(103, common.HexToHash("0xc103"), c102.Hash)
	inOrder(
		f.expectLogs(100, 100),
		// B103's parent matches neither retained A102 nor canonical C102: stale.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				f.push(c103)
				return c102, nil
			}),
		// Only the canonical C103 (parent == refreshed C102) confirms 101.
		f.expectLogs(101, 101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges(), "no write between the stale head and the canonical confirmation")
}

// TestRun_ReplacementBelowTipResetsConfirmationDepth pins that accepting a
// replacement below the tip drops the stale descendants and measures the lag
// from the replacement branch: queued A103, A104, B101, B102, B103 (lag 2)
// must confirm only 101 — from B103 — never 101..102 from the stale A104.
func TestRun_ReplacementBelowTipResetsConfirmationDepth(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	a103, a104 := c.next(103), c.next(104)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b101 := head(101, common.HexToHash("0xb101"), a100.Hash) // replaces A101 on the written 100
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := head(103, common.HexToHash("0xb103"), b102.Hash)
	// With A104 still counted as tip this would be [101,102]; the replacement
	// branch's tip B103 confirms 101 only.
	inOrder(f.expectLogs(100, 100), f.expectLogs(101, 101))
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, a103, a104, b101, b102, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
	require.Equal(t, []logstore.Block{blockOf(b101)}, f.sink.calls[1].Blocks, "101 is written from the replacement branch")
}

// TestRun_StaleHeadWalkReplacementResetsDepth pins the stale-return path:
// queued stale A103/A104 raise the tip to 104, then B103 (on unknown B102) is
// rejected because canonical C102 differs — but the walk replaced retained
// A102 with C102, so everything above 102 is dropped and the tip falls to
// 102. Nothing may be written until C102's own descendants confirm.
func TestRun_StaleHeadWalkReplacementResetsDepth(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	a103, a104 := c.next(103), c.next(104)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	c102 := head(102, common.HexToHash("0xc102"), a101.Hash)
	c103 := head(103, common.HexToHash("0xc103"), c102.Hash)
	inOrder(
		f.expectLogs(100, 100),
		// B103's parent matches neither A102 nor canonical C102: stale — but
		// A102 was replaced by C102, so A103/A104 are dropped and tip = 102.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				f.push(c103)
				return c102, nil
			}),
		// With the stale A104 tip this would have been [101,102]; C103 confirms 101 only.
		f.expectLogs(101, 101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, a103, a104, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_TipOnlyDeepReorgStopsAtLastWritten pins the bound of the walk: it
// reaches the last written height (100), finds it replaced (the deep reorg
// signal), and does not walk further or replay anything.
func TestRun_TipOnlyDeepReorgStopsAtLastWritten(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100), c.next(101), c.next(102))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b100 := head(100, common.HexToHash("0xb100"), common.HexToHash("0x99"))
	b101 := head(101, common.HexToHash("0xb101"), b100.Hash)
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := c.orphanTip(103, b102.Hash)
	inOrder(
		f.expectLogs(100, 100),
		f.expectHead(102, b102),
		f.expectHead(101, b101),
		f.expectHead(100, b100), // written 100 replaced: reported
		// no HeadByNumber(99); writing continues from 101 by number.
		f.expectLogs(101, 101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_ReconcileFetchErrorFails pins that a failing canonical-head fetch
// during reconciliation is fatal and names the height.
func TestRun_ReconcileFetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(100), c.next(101), c.next(102))

	headErr := errors.New("rpc down")
	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	inOrder(
		f.expectLogs(100, 100),
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).Return(nil, headErr),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b103)}

	err := f.run(context.Background(), lagged, 100)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "reconcile reorg at height 102")
	require.Equal(t, [][2]uint64{{100, 100}}, f.sink.ranges())
}

// The canonical-head fetches (written boundary, catch-up metadata, reconcile
// walk) are all fatal on failure and name what they were fetching.

func TestRun_BoundaryHeadFetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(105))

	headErr := errors.New("rpc down")
	f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(103)).Return(nil, headErr)

	err := f.run(context.Background(), ingestion.Config{ConfirmationBlocks: 2}, 100)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "fetch emitted boundary head 103")
}

func TestRun_MetadataFetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(102))

	headErr := errors.New("rpc down")
	inOrder(
		f.expectLogs(100, 102),
		f.expectHead(100, c.canonical(100)),
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).Return(nil, headErr),
	)

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "fetch block 101 metadata")
	require.Empty(t, f.sink.calls)
}
