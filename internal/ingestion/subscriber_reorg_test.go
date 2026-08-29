package ingestion_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
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
	anchor := c.anchor(100)
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 100 written; then 102 is replaced (fork) and 103 builds on the fork.
	// Tip 103 confirms 101 only — no re-write of 100, 102 not yet written.
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100), f.expectHead(100, a100),
		f.expectLogs(101, 101), f.expectHead(101, a101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, c.fork(102), c.next(103)), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_DeepReorgRewindsToVerifiedAncestor pins the recovery for a
// replacement of an already-written height: the fork point is verified
// against the persisted hashes (99 still canonical), the warehouse is rewound
// to it, and the replaced block is re-fetched from the canonical branch.
func TestRun_DeepReorgRewindsToVerifiedAncestor(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	canonical99 := c.canonical(99)
	c.last = canonical99 // 100 descends from the backfilled 99
	a100 := c.next(100)
	f := newFixture(t, a100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 99) // backfilled history below the start block
	fork100 := c.fork(100)       // also descends from the canonical 99
	h101 := c.next(101)          // builds on fork100
	inOrder(
		f.expectHead(99, canonical99), // resume point verified at start: 99 is canonical
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // no logs: 100 re-read after the fetch, still what was written
		// fork100 arrives for the written height 100: the node confirms it is canonical...
		f.expectHead(100, fork100),
		// ...and 99 is the verified common ancestor (persisted hash == canonical).
		f.expectHead(99, canonical99),
		// After the rewind the stream restarts at 100 and refetches 100-101 together.
		f.expectLogs(100, 101),
		f.expectHead(100, fork100), // logless blocks re-read after the fetch
		f.expectHead(101, h101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, fork100, h101), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{99}, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 100}, {100, 101}}, f.sink.ranges())
	require.Equal(t, fork100.Hash, f.sink.calls[1].Blocks[0].Hash, "block 100 is rewritten from the canonical branch")
}

// TestRun_TipOnlyReorgReconcilesToWrittenBoundary pins reconcile: a reorg
// announced only by a later tip (B103 whose parent is an unseen B102) walks
// canonical heads down by number until the retained chain matches — here at
// the written block 100 — replacing stale retained heads, with no re-fetch of
// anything written, and the replacement B101 is what gets written.
func TestRun_TipOnlyReorgReconcilesToWrittenBoundary(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	_ = a101
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b101 := head(101, common.HexToHash("0xb101"), a100.Hash) // rejoins the written chain at 100
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := c.orphanTip(103, b102.Hash)
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
		f.expectHead(102, b102),
		f.expectHead(101, b101),
		// retained 100 == b101's parent: walk stops; 101 is confirmed by tip 103.
		f.expectLogs(101, 101),
		f.expectHead(101, b101), // logless: re-read after the fetch
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
	anchor := c.anchor(100)
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	staleB103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	a103 := c.next(103) // canonical, on A102
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
		// B103's parent disagrees with retained A102; the node confirms A102 is canonical.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				f.push(a103) // arrives after the stale tip was processed
				return a102, nil
			}),
		// No fetch of 101 on the stale tip; A103 confirms it.
		f.expectLogs(101, 101),
		f.expectHead(101, a101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, staleB103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_ReorgAfterSingleHeadCatchupReachesBoundary pins the initial-gap
// case: the first head (A105, lag 2) is bridged down to the resume anchor by
// number — 104..100 fetched and retained canonical, the written boundary 103
// among them — and writes 100..103; a reorg announced only by B106 then walks
// the retained 105 and 104 and reaches 103, where the written boundary is
// found replaced. The fork point is verified against the persisted hashes
// (102 is still canonical), the warehouse is rewound to 102, and 103..104 are
// rewritten from the canonical branch.
func TestRun_ReorgAfterSingleHeadCatchupReachesBoundary(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	c.extend(104)
	f := newFixture(t, c.next(105))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	canonical102 := c.canonical(102)
	b103 := head(103, common.HexToHash("0xb103"), canonical102.Hash)
	b104 := head(104, common.HexToHash("0xb104"), b103.Hash)
	b105 := head(105, common.HexToHash("0xb105"), b104.Hash)
	b106 := c.orphanTip(106, b105.Hash)
	calls := []*gomock.Call{f.expectHead(99, anchor)}
	calls = append(calls, f.expectBridgeHeads(c, 104, 100)...) // A105 bridged down to the anchor: 104..100 retained
	calls = append(calls, f.expectLogs(100, 103))
	calls = append(calls, f.expectRecheckHeads(c, 100, 103)...) // no logs: every block re-read after the fetch
	calls = append(calls,
		f.expectHead(105, b105),         // retained A105 replaced
		f.expectHead(104, b104),         // bridged (never received)
		f.expectHead(103, b103),         // written boundary replaced: deep reorg
		f.expectHead(102, canonical102), // fork-point walk: persisted 102 == canonical → rewind to 102
		f.expectHead(105, b105),         // b106 re-recorded against the fresh window {102}
		f.expectHead(104, b104),
		f.expectHead(103, b103),
		f.expectLogs(103, 104),  // tip 106, lag 2 → 103..104 rewritten
		f.expectHead(103, b103), // logless: re-read after the fetch
		f.expectHead(104, b104),
	)
	inOrder(calls...)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b106), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{102}, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 103}, {103, 104}}, f.sink.ranges())
	require.Equal(t, []logstore.Block{blockOf(b103), blockOf(b104)}, f.sink.calls[1].Blocks, "103..104 are written from the canonical branch")
}

// TestRun_ReorgAboveBoundaryAfterCatchupIsAbsorbed is the shallow
// counterpart: the bridged walk rejoins the retained chain at the boundary,
// so nothing is reported and writing simply continues.
func TestRun_ReorgAboveBoundaryAfterCatchupIsAbsorbed(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	c.extend(104)
	f := newFixture(t, c.next(105))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a103 := c.canonical(103)
	b104 := head(104, common.HexToHash("0xb104"), a103.Hash) // rejoins at the boundary
	b105 := head(105, common.HexToHash("0xb105"), b104.Hash)
	b106 := c.orphanTip(106, b105.Hash)
	calls := []*gomock.Call{f.expectHead(99, anchor)}
	calls = append(calls, f.expectBridgeHeads(c, 104, 100)...) // A105 bridged down to the anchor
	calls = append(calls, f.expectLogs(100, 103))
	calls = append(calls, f.expectRecheckHeads(c, 100, 103)...)
	calls = append(calls,
		f.expectHead(105, b105),
		f.expectHead(104, b104),
		// 103 retained == b104's parent: rejoin, no fetch of 103.
		f.expectLogs(104, 104),
		f.expectHead(104, b104), // logless: re-read after the fetch
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
	anchor := c.anchor(100)
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	c102 := head(102, common.HexToHash("0xc102"), a101.Hash)
	c103 := head(103, common.HexToHash("0xc103"), c102.Hash)
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
		// B103's parent matches neither retained A102 nor canonical C102: stale.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				f.push(c103)
				return c102, nil
			}),
		// Only the canonical C103 (parent == refreshed C102) confirms 101.
		f.expectLogs(101, 101),
		f.expectHead(101, a101),
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
	anchor := c.anchor(100)
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
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100), f.expectHead(100, a100),
		f.expectLogs(101, 101), f.expectHead(101, b101), // 101 is re-read as the replacement it was written from
	)
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
	anchor := c.anchor(100)
	a100, a101, a102 := c.next(100), c.next(101), c.next(102)
	a103, a104 := c.next(103), c.next(104)
	f := newFixture(t, a100, a101, a102)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	c102 := head(102, common.HexToHash("0xc102"), a101.Hash)
	c103 := head(103, common.HexToHash("0xc103"), c102.Hash)
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
		// B103's parent matches neither A102 nor canonical C102: stale — but
		// A102 was replaced by C102, so A103/A104 are dropped and tip = 102.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				f.push(c103)
				return c102, nil
			}),
		// With the stale A104 tip this would have been [101,102]; C103 confirms 101 only.
		f.expectLogs(101, 101),
		f.expectHead(101, a101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, a103, a104, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_TipOnlyDeepReorgRewindsToForkPoint pins the case the runbook used
// to get wrong: the reconcile walk finds the last written block (100)
// replaced, but the real fork is verified against persisted hashes — here 99
// is still canonical — so the rewind lands on 99 and 100-101 are refetched
// from the canonical branch, with nothing stale left inside coverage.
func TestRun_TipOnlyDeepReorgRewindsToForkPoint(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	canonical99 := c.canonical(99)
	c.last = canonical99 // 100 descends from the backfilled 99
	a100 := c.next(100)
	f := newFixture(t, a100, c.next(101), c.next(102))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 99)

	b100 := head(100, common.HexToHash("0xb100"), canonical99.Hash)
	b101 := head(101, common.HexToHash("0xb101"), b100.Hash)
	b102 := head(102, common.HexToHash("0xb102"), b101.Hash)
	b103 := c.orphanTip(103, b102.Hash)
	inOrder(
		f.expectHead(99, canonical99), // resume point verified at start
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch, still A100
		// reconcile walks down to the written 100 and finds it replaced
		f.expectHead(102, b102),
		f.expectHead(101, b101),
		f.expectHead(100, b100),
		// fork-point walk: 99 persisted == canonical → ancestor 99, rewind
		f.expectHead(99, canonical99),
		// b103 is re-recorded against the fresh window {99}: bridge walk refetches 102..100
		f.expectHead(102, b102),
		f.expectHead(101, b101),
		f.expectHead(100, b100),
		// tip 103 with lag 2 → 100-101 rewritten from the canonical branch
		f.expectLogs(100, 101),
		f.expectHead(100, b100), // logless: re-read after the fetch
		f.expectHead(101, b101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b103), thenCancel(cancel)}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{99}, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 100}, {100, 101}}, f.sink.ranges())
	require.Equal(t, b100.Hash, f.sink.calls[1].Blocks[0].Hash)
	require.Equal(t, b101.Hash, f.sink.calls[1].Blocks[1].Hash)
}

// TestRun_DeepReorgBelowCoverageIsFatal pins the bound: when no persisted
// block matches the canonical chain (the fork reaches below what the
// warehouse holds) ingestion stops with a named error instead of guessing.
func TestRun_DeepReorgBelowCoverageIsFatal(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	a100 := c.next(100)
	f := newFixture(t, a100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fork100 := c.fork(100) // also descends from the canonical 99
	inOrder(
		f.expectHead(99, anchor), // retained as the bridge — but never written, so it cannot verify a fork point
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
		f.expectHead(100, fork100),
		// nothing stored below 100: the walk cannot verify an ancestor
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, fork100)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorContains(t, err, "replaced written block 100")
	require.ErrorContains(t, err, "block 99 is not stored, the fork reaches below warehouse coverage")
	require.Empty(t, f.sink.rewinds)
}

// TestRun_StaleHeadAtWrittenHeightIsIgnored pins that a late head for a
// written height whose hash is neither ours nor canonical is a stale branch,
// not a reorg: nothing is rewound.
func TestRun_StaleHeadAtWrittenHeightIsIgnored(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	f := newFixture(t, c.next(100))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	written100 := c.canonical(100)
	h101 := c.next(101)
	stale := head(100, common.HexToHash("0x5a1e"), common.HexToHash("0x99"))
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),
		f.expectHead(100, written100), // logless: re-read after the fetch
		f.expectHead(100, written100), // node still holds what we wrote
		f.expectLogs(101, 101),
		f.expectHead(101, h101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, stale, h101), thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 100}, {101, 101}}, f.sink.ranges())
}

// TestRun_ReconcileFetchErrorFails pins that a failing canonical-head fetch
// during reconciliation is fatal and names the height.
func TestRun_ReconcileFetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	a100 := c.next(100)
	f := newFixture(t, a100, c.next(101), c.next(102))

	headErr := errors.New("rpc down")
	b103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xb102"))
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(102)).Return(nil, headErr),
	)
	f.sink.steps = []func(uint64, uint64) error{thenPush(f.push, b103)}

	err := f.run(context.Background(), lagged, 100)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "reconcile reorg at height 102")
	require.Equal(t, [][2]uint64{{100, 100}}, f.sink.ranges())
}

// The canonical-head fetches (written boundary, catch-up metadata, reconcile
// walk) are all fatal on failure and name what they were fetching. With the
// resume anchor retained, the bridge walk normally covers every unreceived
// height, so the boundary and metadata fetches by number only happen for a
// gap the walk left: a stale head's walk stops at the first canonical block
// that disagrees with it, and a canonical tip then rejoins there, leaving the
// heights below unretained.

func TestRun_BoundaryHeadFetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	stale105 := head(105, common.HexToHash("0xb105"), common.HexToHash("0xdead"))
	c.extend(104)
	f := newFixture(t, stale105, c.next(105))

	headErr := errors.New("rpc down")
	inOrder(
		f.expectHead(99, anchor),
		f.expectHead(104, c.canonical(104)), // the stale 105 is walked to 104, disagrees, and is dropped
		// the canonical 105 rejoins at the retained 104; the boundary 103 was never bridged
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(103)).Return(nil, headErr),
	)

	err := f.run(context.Background(), ingestion.Config{ConfirmationBlocks: 2}, 100)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "fetch emitted boundary head 103")
}

func TestRun_MetadataFetchErrorFails(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	stale103 := head(103, common.HexToHash("0xb103"), common.HexToHash("0xdead"))
	c.extend(102)
	f := newFixture(t, stale103, c.next(103))

	headErr := errors.New("rpc down")
	// Metadata is read before the logs: the strict mock fails on any FilterLogs.
	inOrder(
		f.expectHead(99, anchor),
		f.expectHead(102, c.canonical(102)), // the stale 103 is walked to 102, disagrees, and is dropped
		// the canonical 103 rejoins at 102; 100..101 are fetched as batch metadata
		f.expectHead(100, c.canonical(100)),
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).Return(nil, headErr),
	)

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "fetch block 101 metadata")
	require.Empty(t, f.sink.calls)
}

// TestRun_RestartSpanningDeepReorgRewinds pins the case no in-memory head can
// catch: the process resumes at cursor+1 (101) after a reorg replaced the
// written 100 while it was down. The persisted hash of 100 disagrees with the
// canonical header at start, the verified ancestor is 99, the store is
// rewound to it, and 100..101 are written from the canonical chain.
func TestRun_RestartSpanningDeepReorgRewinds(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	canonical99 := c.canonical(99)
	canonical100 := c.canonical(100)
	tip101 := c.orphanTip(101, canonical100.Hash)
	f := newFixture(t, tip101)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 99)
	f.sink.stored[100] = common.HexToHash("0x01d100") // what was written before the restart: now orphaned

	inOrder(
		f.expectHead(100, canonical100), // resume check: persisted 100 != canonical
		f.expectHead(99, canonical99),   // verified ancestor
		// tip 101 bridges 100 against the fresh window {99}
		f.expectHead(100, canonical100),
		f.expectLogs(100, 101),
		f.expectHead(100, canonical100), // logless: re-read after the fetch
		f.expectHead(101, tip101),
	)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 101)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{99}, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 101}}, f.sink.ranges())
	require.Equal(t, canonical100.Hash, f.sink.calls[0].Blocks[0].Hash)
}

// TestRun_ResumePointStillCanonicalSeedsBridge pins the happy restart: the
// persisted cursor block matches the chain, nothing is rewound, and the
// retained canonical head lets the first tip reconcile through it.
func TestRun_ResumePointStillCanonicalSeedsBridge(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	canonical100 := c.canonical(100)
	tip101 := c.orphanTip(101, canonical100.Hash)
	f := newFixture(t, tip101)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 100)

	inOrder(
		f.expectHead(100, canonical100), // resume check: match, retained as the bridge
		f.expectLogs(101, 101),
		f.expectHead(101, tip101), // logless: re-read after the fetch
	)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 101)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{101, 101}}, f.sink.ranges())
}

// TestRun_CatchupBoundPreemptsBridgeWalk pins that a stale cursor is refused
// before reconciliation spends a single header RPC on it: with the resume
// bridge retained (99), a far tip would otherwise be walked down to 99 by
// eth_getBlockByNumber before max_catchup_blocks was consulted.
func TestRun_CatchupBoundPreemptsBridgeWalk(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	canonical99 := c.canonical(99)
	f := newFixture(t, c.orphanTip(5_000, common.HexToHash("0xfar")))
	f.sink.seedStored(c, 90, 99)
	// Only the resume check may touch the chain; the strict mock fails on any other HeadByNumber.
	f.expectHead(99, canonical99)

	err := f.run(context.Background(), ingestion.Config{MaxCatchupBlocks: 100}, 100)
	require.ErrorIs(t, err, ingestion.ErrCatchupTooLarge)
	require.Contains(t, err.Error(), "need blocks 100-5000")
	require.Empty(t, f.sink.calls)
}

// TestRun_StartupStaleHighHeadCannotShortenLag pins the anchor's purpose on
// an empty warehouse: a stale head above the start (B102, parent unknown to
// the node) queued at subscribe time is walked down to the resume anchor
// like any other — the node's 101 is not B102's parent, so B102 is dropped
// before it can become the tip — and the canonical A100 alone leaves nothing
// confirmed under lag 2. Block 100 is written exactly once, only after the
// canonical 101 and 102 arrive; had B102 counted as tip, the write would have
// happened first, before those heads were ever pushed.
func TestRun_StartupStaleHighHeadCannotShortenLag(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	staleB102 := head(102, common.HexToHash("0xb102"), common.HexToHash("0xdead"))
	a100 := c.next(100)
	f := newFixture(t, staleB102, a100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a101, a102 := c.next(101), c.next(102)

	canonicalTipPushed := false
	inOrder(
		f.expectHead(99, anchor), // nothing stored: the anchor is still fetched and retained as the bridge
		// B102's walk: the bridge is present, so 101 is fetched; its hash is not
		// B102's claimed parent, so the walk stops and B102 is stale. 100 is not
		// fetched for it. The canonical tip arrives only now, after B102 was judged.
		f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(101)).DoAndReturn(
			func(context.Context, uint64) (*chain.BlockHead, error) {
				canonicalTipPushed = true
				f.push(a101)
				f.push(a102)
				return a101, nil
			}),
		// A100 rejoins at the anchor without a fetch; tip 100 confirms nothing.
		// A102 (through the retained 101) confirms 100.
		f.expectLogs(100, 100),
		f.expectHead(100, a100), // logless: re-read after the fetch
	)
	f.sink.steps = []func(uint64, uint64) error{func(from, to uint64) error {
		require.True(t, canonicalTipPushed, "block 100 was confirmed before the canonical 101/102 arrived: the stale 102 acted as tip")
		cancel()
		return nil
	}}

	err := f.run(ctx, lagged, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 100}}, f.sink.ranges(), "100 written once; 101 and 102 stay inside the lag")
	require.Equal(t, []logstore.Block{blockOf(a100)}, f.sink.calls[0].Blocks, "100 is written from the canonical head")
}

// logAt builds a warehouse-shaped log for the block h, carrying its hash the
// way a real eth_getLogs answer does.
func logAt(h *chain.BlockHead) types.Log {
	l := transferLog(uint64(h.Number), 0)
	l.BlockHash = h.Hash
	return l
}

// TestRun_BatchRefetchedWhenLogsDisagreeWithHeaders pins the guard against a
// reorg between the metadata and the log fetch: the logs for 100 carry a
// block hash the retained head does not, so the retained head is dropped,
// refetched canonical, and the batch is fetched again before anything is
// written — and what is written pairs the logs with their own block.
func TestRun_BatchRefetchedWhenLogsDisagreeWithHeaders(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	stale100 := c.next(100) // the head the subscription delivered (the metadata of the first attempt)
	f := newFixture(t, stale100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canonical100 := head(100, common.HexToHash("0xc100"), stale100.ParentHash) // what the node holds by the time logs are fetched
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100, logAt(canonical100)), // logs already come from the new block
		// disagreement → retained 100 dropped; the batch starts over: 100 refetched canonical, then the logs again
		f.expectHead(100, canonical100),
		f.expectLogs(100, 100, logAt(canonical100)),
	)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}}, f.sink.ranges())
	require.Equal(t, canonical100.Hash, f.sink.calls[0].Blocks[0].Hash)
	require.Equal(t, canonical100.Hash, f.sink.calls[0].Logs[0].BlockHash)
}

// TestRun_BatchRefetchedWhenLoglessBlockMovedDuringFetch pins the guard the
// logs cannot provide: block 100 (retained from the subscription as A) returns
// no log at all because the warehouse event only exists on branch B, which
// the node switched to during the fetch. The post-fetch re-read of the
// logless block sees B, so the batch starts over — 100 refetched canonical
// (B), logs fetched again (now carrying B's event) — and what is written is
// B's hash with B's log, never A's hash with no log.
func TestRun_BatchRefetchedWhenLoglessBlockMovedDuringFetch(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	a100 := c.next(100)
	f := newFixture(t, a100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b100 := head(100, common.HexToHash("0xb100"), a100.ParentHash) // same parent: a one-block replacement
	inOrder(
		f.expectHead(99, anchor),
		f.expectLogs(100, 100),  // branch A: no warehouse event, so the log fetch cannot vouch for 100
		f.expectHead(100, b100), // the post-fetch re-read: 100 moved to B
		// retained A100 dropped; the batch starts over with B100 as metadata
		f.expectHead(100, b100),
		f.expectLogs(100, 100, logAt(b100)), // branch B has the event
	)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 100}}, f.sink.ranges())
	require.Equal(t, b100.Hash, f.sink.calls[0].Blocks[0].Hash, "100 is written from the branch the logs came from")
	require.Len(t, f.sink.calls[0].Logs, 1, "the event that exists only on B is written with it")
}

// TestRun_BatchGivesUpWhenChainKeepsMoving pins the bound of that retry: a
// batch whose logs and headers never agree is not written and ingestion
// stops with a named error rather than looping.
func TestRun_BatchGivesUpWhenChainKeepsMoving(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	anchor := c.anchor(100)
	f := newFixture(t, c.next(100))
	other := transferLog(100, 0)
	other.BlockHash = common.HexToHash("0x07e4")
	f.expectHead(99, anchor)
	f.client.EXPECT().FilterLogs(gomock.Any(), logsFor(100, 100)).Return([]types.Log{other}, nil).Times(3)
	f.client.EXPECT().HeadByNumber(gomock.Any(), uint64(100)).Return(c.canonical(100), nil).Times(2)

	err := f.run(context.Background(), ingestion.Config{}, 100)
	require.ErrorContains(t, err, "kept changing between log and header fetches for batch 100-100 (3 attempts)")
	require.Empty(t, f.sink.calls)
}

// chainServer answers HeadByNumber from a headChain, with per-height
// overrides a test can change between calls, and records every fetched
// height so a test can assert what was NOT walked.
type chainServer struct {
	c        *headChain
	override map[uint64][]*chain.BlockHead // consumed in order, then canonical
	fetched  []uint64
	onFetch  func(n uint64) // runs after each fetch is recorded (e.g. to swap the chain)
}

func (s *chainServer) install(f *fixture) {
	f.client.EXPECT().HeadByNumber(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(_ context.Context, n uint64) (*chain.BlockHead, error) {
			s.fetched = append(s.fetched, n)
			defer func() {
				if s.onFetch != nil {
					s.onFetch(n)
				}
			}()
			if q := s.override[n]; len(q) > 0 {
				h := q[0]
				s.override[n] = q[1:]
				return h, nil
			}
			return s.c.canonical(n), nil
		})
}

func (s *chainServer) count(n uint64) int {
	k := 0
	for _, x := range s.fetched {
		if x == n {
			k++
		}
	}
	return k
}

func (s *chainServer) fetchedBetween(lo, hi uint64) []uint64 {
	var out []uint64
	for _, x := range s.fetched {
		if x >= lo && x <= hi {
			out = append(out, x)
		}
	}
	return out
}

// TestRun_FarHeadIsVerifiedByNumberNotWalked pins the restart-after-a-gap
// path: the first head sits far above the anchor (gap > maxBridgeWalk), so
// reconcile verifies that head with one eth_getBlockByNumber instead of
// walking every intermediate header; the gap's headers are fetched by
// number, batch by batch, during the catch-up.
func TestRun_FarHeadIsVerifiedByNumberNotWalked(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	c.anchor(100)
	c.extend(300)
	far := c.canonical(300)
	f := newFixture(t, far)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 99)
	srv := &chainServer{c: c, override: map[uint64][]*chain.BlockHead{}}
	srv.install(f)
	f.serveLogs(nil)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 109}}, f.sink.ranges())
	require.Equal(t, c.canonical(100).Hash, f.sink.calls[0].Blocks[0].Hash)
	require.Equal(t, 1, srv.count(300), "the far head is verified by number once")
	require.Empty(t, srv.fetchedBetween(110, 299), "no header of the gap is walked before the first batch")
}

// TestRun_FarStaleHeadIsDropped pins the other half: a far head the node
// does not hold canonical is dropped after that single check, and writing
// waits for a head that is.
func TestRun_FarStaleHeadIsDropped(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	c.anchor(100)
	c.extend(300)
	stale := head(300, common.HexToHash("0x57a1e"), common.HexToHash("0xdead"))
	f := newFixture(t, stale, c.canonical(300))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 99)
	srv := &chainServer{c: c, override: map[uint64][]*chain.BlockHead{}}
	srv.install(f)
	f.serveLogs(nil)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 109}}, f.sink.ranges())
	require.Equal(t, c.canonical(100).Hash, f.sink.calls[0].Blocks[0].Hash)
}

// TestRun_BatchBoundaryDiscontinuityRewinds pins the continuity check at
// the batch boundary: the first block of a catch-up batch does not descend
// from the written block below it (the chain replaced that block right
// after the resume check), so the deep-reorg recovery runs against the
// persisted hashes, rewinds to the verified ancestor 99, and the replanned
// range rewrites 100.. from the new canonical chain.
func TestRun_BatchBoundaryDiscontinuityRewinds(t *testing.T) {
	t.Parallel()

	old := &headChain{}
	old.anchor(101) // canonical 100 is the resume point (cursor = 100)
	old.extend(300)
	// The post-reorg chain shares block 99 and diverges from 100 on.
	reorged := &headChain{seq: 10_000}
	reorged.store(old.canonical(99))
	reorged.last = old.canonical(99)
	reorged.extend(300)
	far := reorged.canonical(300)

	f := newFixture(t, far)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(old, 90, 100) // 100 stored with the old hash, which the node still reports at start
	srv := &chainServer{c: old, override: map[uint64][]*chain.BlockHead{}}
	srv.onFetch = func(n uint64) {
		if n == 100 && srv.c == old {
			srv.c = reorged // the reorg lands right after the resume check
		}
	}
	srv.install(f)
	f.serveLogs(nil)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}
	// After the rewind nothing is queued; deliver the far head again so the
	// range is replanned from 100.
	go func() {
		for len(f.sink.rewinds) == 0 {
			time.Sleep(5 * time.Millisecond)
		}
		f.push(far)
	}()

	err := f.run(ctx, ingestion.Config{}, 101)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []uint64{99}, f.sink.rewinds)
	require.Equal(t, [][2]uint64{{100, 109}}, f.sink.ranges())
	require.Equal(t, reorged.canonical(100).Hash, f.sink.calls[0].Blocks[0].Hash, "100 is rewritten from the new canonical chain")
}

// TestRun_MidBatchDiscontinuityRefetches pins the continuity check inside a
// batch: a header that does not descend from the one fetched for the height
// below means the chain moved between the two fetches, so the headers are
// refetched and the batch is written once, linked end to end.
func TestRun_MidBatchDiscontinuityRefetches(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	c.anchor(100)
	c.extend(300)
	far := c.canonical(300)
	f := newFixture(t, far)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.sink.seedStored(c, 90, 99)
	srv := &chainServer{c: c, override: map[uint64][]*chain.BlockHead{}}
	srv.override[105] = []*chain.BlockHead{head(105, common.HexToHash("0x105x"), common.HexToHash("0xdead"))}
	srv.install(f)
	f.serveLogs(nil)
	f.sink.steps = []func(uint64, uint64) error{thenCancel(cancel)}

	err := f.run(ctx, ingestion.Config{}, 100)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{100, 109}}, f.sink.ranges())
	require.Equal(t, 3, srv.count(105), "105: the mismatching fetch, the refetch, and the post-log re-read of a logless block")
	for i, b := range f.sink.calls[0].Blocks {
		require.Equal(t, c.canonical(100+uint64(i)).Hash, b.Hash)
	}
}
