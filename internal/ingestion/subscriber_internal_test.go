package ingestion

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
	"github.com/feral-file/ff-eth-logs/internal/mocks"
)

type pruneSink struct{ writes int }

func (s *pruneSink) WriteRange(context.Context, uint64, uint64, []logstore.Block, []types.Log) error {
	s.writes++
	return nil
}
func (s *pruneSink) StoredBlockHash(context.Context, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, nil
}
func (s *pruneSink) Rewind(context.Context, uint64) error { return nil }

// TestIngestRangePrunesRetainedHeadsPerBatch pins that a long catch-up does
// not retain one header per block of the gap: after every committed batch
// the stream position advances and the headers below it are forgotten, so
// the retained set stays at the batch boundary regardless of range length.
func TestIngestRangePrunesRetainedHeadsPerBatch(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	client := mocks.NewMockEthClient(ctrl)
	byNumber := map[uint64]*chain.BlockHead{}
	client.EXPECT().HeadByNumber(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(_ context.Context, n uint64) (*chain.BlockHead, error) { return canonicalHead(byNumber, n), nil })
	client.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
	clock := mocks.NewMockClock(ctrl)
	clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	sink := &pruneSink{}
	s := NewSubscriber(Config{}, client, NewFetcher(client, clock, 0), sink)
	st := &streamState{next: 100, lowerBound: 100, heads: map[uint64]*chain.BlockHead{99: canonicalHead(byNumber, 99)}}
	require.NoError(t, s.ingestRange(context.Background(), st, 100, 159))

	require.Equal(t, 6, sink.writes)
	require.Equal(t, uint64(160), st.next)
	require.Len(t, st.heads, 1, "only the last committed boundary is retained, not one header per block")
	_, ok := st.heads[159]
	require.True(t, ok)
}

// canonicalHead synthesizes a chained canonical head per height.
func canonicalHead(byNumber map[uint64]*chain.BlockHead, n uint64) *chain.BlockHead {
	if h, ok := byNumber[n]; ok {
		return h
	}
	h := &chain.BlockHead{Number: hexutil.Uint64(n), Hash: common.BigToHash(big.NewInt(int64(n)))} //nolint:gosec // test heights
	if n > 0 {
		h.ParentHash = canonicalHead(byNumber, n-1).Hash
	}
	byNumber[n] = h
	return h
}
