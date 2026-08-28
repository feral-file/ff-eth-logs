package ingestion_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-eth-logs/internal/ingestion"
	"github.com/feral-file/ff-eth-logs/internal/mocks"
)

// fakeStore is the runner's warehouse: a scripted cursor plus the fake sink.
type fakeStore struct {
	fakeSink
	cursor    uint64
	hasCursor bool
	cursorErr error
	cursorHit int
}

func (s *fakeStore) Cursor(context.Context) (uint64, bool, error) {
	s.cursorHit++
	return s.cursor, s.hasCursor, s.cursorErr
}

// runnerFixture wires a fixture whose first head is `head` and a store, and
// scripts the sink to cancel the run after its first write, so each start
// path is observed through the first written range.
func runnerFixture(t *testing.T, headHeight uint64, store *fakeStore) (*fixture, *headChain, context.Context) {
	t.Helper()
	c := &headChain{}
	f := newFixture(t, c.next(headHeight))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store.steps = []func(uint64, uint64) error{thenCancel(cancel)}
	f.client.EXPECT().ChainID(gomock.Any()).Return(uint64(1), nil)
	f.serveLogs(nil)
	f.serveHeads(c)
	return f, c, ctx
}

// TestRun_StartBlockOverridesCursor pins that an explicit start_block wins
// unconditionally: the cursor is not consulted and writing starts there. The
// tight catch-up bound would reject a start from the (older) cursor.
func TestRun_StartBlockOverridesCursor(t *testing.T) {
	t.Parallel()

	store := &fakeStore{cursor: 200, hasCursor: true}
	f, _, ctx := runnerFixture(t, 500, store)

	err := ingestion.Run(ctx, ingestion.RunConfig{ChainID: 1, Config: ingestion.Config{MaxCatchupBlocks: 10}, StartBlock: 500}, f.client, store)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{500, 500}}, store.ranges())
	require.Zero(t, store.cursorHit, "start_block must not read the cursor")
}

// TestRun_ResumesFromCursorPlusOne pins the restart path: the cursor is the
// last written block, so writing resumes at cursor+1. eth_blockNumber is never
// called (the strict mock would fail on it).
func TestRun_ResumesFromCursorPlusOne(t *testing.T) {
	t.Parallel()

	store := &fakeStore{cursor: 199, hasCursor: true}
	f, _, ctx := runnerFixture(t, 205, store)

	err := ingestion.Run(ctx, ingestion.RunConfig{ChainID: 1, Config: ingestion.Config{MaxCatchupBlocks: 10}}, f.client, store)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{200, 205}}, store.ranges())
	require.Equal(t, 1, store.cursorHit)
}

// TestRun_StartsAtHeadWithoutCursor pins the first-run path: with no cursor
// and no start_block, ingestion starts at the current chain head and leaves
// history to the backfill.
func TestRun_StartsAtHeadWithoutCursor(t *testing.T) {
	t.Parallel()

	store := &fakeStore{}
	f, _, ctx := runnerFixture(t, 300, store)
	f.client.EXPECT().BlockNumber(gomock.Any()).Return(uint64(300), nil)

	err := ingestion.Run(ctx, ingestion.RunConfig{ChainID: 1}, f.client, store)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, [][2]uint64{{300, 300}}, store.ranges())
}

func TestRun_CursorErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := mocks.NewMockEthClient(ctrl)
	cursorErr := errors.New("connection refused")
	store := &fakeStore{cursorErr: cursorErr}
	client.EXPECT().ChainID(gomock.Any()).Return(uint64(1), nil)

	err := ingestion.Run(context.Background(), ingestion.RunConfig{ChainID: 1}, client, store)
	require.ErrorIs(t, err, cursorErr)
	require.Contains(t, err.Error(), "read ingestion cursor")
	require.Empty(t, store.calls)
}

func TestRun_HeadLookupErrorFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := mocks.NewMockEthClient(ctrl)
	headErr := errors.New("rpc down")
	client.EXPECT().ChainID(gomock.Any()).Return(uint64(1), nil)
	client.EXPECT().BlockNumber(gomock.Any()).Return(uint64(0), headErr)
	store := &fakeStore{}

	err := ingestion.Run(context.Background(), ingestion.RunConfig{ChainID: 1}, client, store)
	require.ErrorIs(t, err, headErr)
	require.Contains(t, err.Error(), "read chain head for first start")
	require.Empty(t, store.calls)
}

// TestRun_PropagatesSubscriberErrors pins that the runner returns the
// subscriber's fatal errors unchanged (here: the catch-up bound), so the
// supervisor sees the named cause.
func TestRun_PropagatesSubscriberErrors(t *testing.T) {
	t.Parallel()

	c := &headChain{}
	f := newFixture(t, c.next(1_000))
	f.client.EXPECT().ChainID(gomock.Any()).Return(uint64(1), nil)
	store := &fakeStore{cursor: 99, hasCursor: true}

	err := ingestion.Run(context.Background(), ingestion.RunConfig{ChainID: 1, Config: ingestion.Config{MaxCatchupBlocks: 100}}, f.client, store)
	require.ErrorIs(t, err, ingestion.ErrCatchupTooLarge)
	require.Contains(t, err.Error(), "need blocks 100-1000")
}

// TestRun_RefusesWrongChain pins that a provider on another chain (or one
// that cannot answer eth_chainId) never gets to read the cursor or write.
func TestRun_RefusesWrongChain(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client := mocks.NewMockEthClient(ctrl)
	client.EXPECT().ChainID(gomock.Any()).Return(uint64(11155111), nil)
	store := &fakeStore{cursor: 10, hasCursor: true}

	err := ingestion.Run(context.Background(), ingestion.RunConfig{ChainID: 1}, client, store)
	require.EqualError(t, err, "provider chain id 11155111 does not match configured ethereum.chain_id 1")
	require.Zero(t, store.cursorHit)
	require.Empty(t, store.calls)

	rpcErr := errors.New("rpc down")
	client.EXPECT().ChainID(gomock.Any()).Return(uint64(0), rpcErr)
	err = ingestion.Run(context.Background(), ingestion.RunConfig{ChainID: 1}, client, store)
	require.ErrorIs(t, err, rpcErr)
	require.Contains(t, err.Error(), "read provider chain id")
}
