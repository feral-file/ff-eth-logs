package ingestion_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/ingestion"
	"github.com/feral-file/ff-eth-logs/internal/mocks"
)

func newTestFetcher(t *testing.T) (*ingestion.Fetcher, *mocks.MockEthClient) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes()
	return ingestion.NewFetcher(mockEth, mockClock, 0), mockEth
}

func blockRange(q ethereum.FilterQuery) (uint64, uint64) {
	return q.FromBlock.Uint64(), q.ToBlock.Uint64()
}

func positions(logs []types.Log) [][2]uint64 {
	var got [][2]uint64
	for _, l := range logs {
		got = append(got, [2]uint64{l.BlockNumber, uint64(l.Index)})
	}
	return got
}

// TestFetchIngestionLogs_UsesFullTopicFilterAndSortsByBlockIndex pins the
// selection contract: one query, no address scope, topic0 = the warehouse
// event set (standard NFT signatures plus CryptoPunks), and the result ordered
// by (block, log index) whatever the provider returned.
func TestFetchIngestionLogs_UsesFullTopicFilterAndSortsByBlockIndex(t *testing.T) {
	t.Parallel()

	fetcher, mockEth := newTestFetcher(t)
	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := blockRange(q)
			require.Equal(t, uint64(100), from)
			require.Equal(t, uint64(105), to)
			require.Nil(t, q.Addresses, "ingestion is chain-wide, never address-scoped")
			require.Len(t, q.Topics, 1, "topic0-only filter")
			require.Equal(t, eventset.Topics(), q.Topics[0])
			require.Contains(t, q.Topics[0], eventset.Transfer)
			require.Contains(t, q.Topics[0], eventset.TransferSingle)
			require.Contains(t, q.Topics[0], eventset.MetadataUpdate)
			require.Contains(t, q.Topics[0], eventset.PunkTransfer, "registry custom signatures must be included")
			return []types.Log{
				{BlockNumber: 103, Index: 2},
				{BlockNumber: 101, Index: 9},
				{BlockNumber: 103, Index: 0},
			}, nil
		})

	logs, err := fetcher.FetchIngestionLogs(context.Background(), 100, 105)
	require.NoError(t, err)
	require.Equal(t, []types.Log{{BlockNumber: 101, Index: 9}, {BlockNumber: 103, Index: 0}, {BlockNumber: 103, Index: 2}}, logs)
}

// TestFetchIngestionLogs_DenseBlockFallsBackToReceipts pins the result-cap
// path: when the provider refuses a single block ("query returned more than
// 10000 results" even for one block), that block is read from its receipts
// with the identical topic filter and spliced in order between the blocks on
// either side, instead of failing the subscription.
func TestFetchIngestionLogs_DenseBlockFallsBackToReceipts(t *testing.T) {
	t.Parallel()

	fetcher, mockEth := newTestFetcher(t)
	capErr := errors.New("query returned more than 10000 results")
	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := blockRange(q)
			if from <= 101 && 101 <= to {
				return nil, capErr // any window touching block 101 is over the cap
			}
			var logs []types.Log
			for b := from; b <= to; b++ {
				logs = append(logs, types.Log{BlockNumber: b, Index: 0, Topics: []common.Hash{eventset.Transfer}})
			}
			return logs, nil
		}).
		AnyTimes()

	unrelated := common.HexToHash("0x1111")
	mockEth.EXPECT().
		BlockReceipts(gomock.Any(), uint64(101)).
		Return([]*types.Receipt{
			{Logs: []*types.Log{
				{BlockNumber: 101, Index: 0, Topics: []common.Hash{unrelated}},
				{BlockNumber: 101, Index: 1, Topics: []common.Hash{eventset.Transfer, {}, {}}}, // ERC-20-shaped: still selected, as eth_getLogs would
			}},
			{Logs: []*types.Log{
				{BlockNumber: 101, Index: 2, Topics: []common.Hash{eventset.TransferSingle}},
				{BlockNumber: 101, Index: 3, Topics: nil},
			}},
		}, nil)

	logs, err := fetcher.FetchIngestionLogs(context.Background(), 100, 102)
	require.NoError(t, err)
	require.Equal(t, [][2]uint64{{100, 0}, {101, 1}, {101, 2}, {102, 0}}, positions(logs),
		"neighbors via eth_getLogs, dense block via receipts filtered to the ingestion topics, in chain order")
}

// TestFetchIngestionLogs_DenseBlockAtRangeEdges pins that a dense block sitting
// at either end of the range is served without querying an empty neighbor.
func TestFetchIngestionLogs_DenseBlockAtRangeEdges(t *testing.T) {
	t.Parallel()

	fetcher, mockEth := newTestFetcher(t)
	capErr := errors.New("query returns too many logs, narrow your filter: 20000")
	var queried [][2]uint64
	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := blockRange(q)
			queried = append(queried, [2]uint64{from, to})
			if from <= 50 && 50 <= to {
				return nil, capErr
			}
			return []types.Log{{BlockNumber: from, Index: 4, Topics: []common.Hash{eventset.URI}}}, nil
		}).
		AnyTimes()
	mockEth.EXPECT().BlockReceipts(gomock.Any(), uint64(50)).
		Return([]*types.Receipt{{Logs: []*types.Log{{BlockNumber: 50, Index: 7, Topics: []common.Hash{eventset.Transfer}}}}}, nil).
		Times(2)

	logs, err := fetcher.FetchIngestionLogs(context.Background(), 50, 51)
	require.NoError(t, err)
	require.Equal(t, [][2]uint64{{50, 7}, {51, 4}}, positions(logs))

	logs, err = fetcher.FetchIngestionLogs(context.Background(), 49, 50)
	require.NoError(t, err)
	require.Equal(t, [][2]uint64{{49, 4}, {50, 7}}, positions(logs))

	for _, r := range queried {
		require.LessOrEqual(t, r[0], r[1], "never queries an inverted range around the dense block")
	}
}

// TestFetchIngestionLogs_DenseBlockReceiptFailureIsReported pins that a
// failing receipts fetch surfaces as an error naming the block (the caller's
// restart path), not as a silently empty block.
func TestFetchIngestionLogs_DenseBlockReceiptFailureIsReported(t *testing.T) {
	t.Parallel()

	fetcher, mockEth := newTestFetcher(t)
	mockEth.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, errors.New("query returned more than 10000 results")).AnyTimes()
	receiptErr := errors.New("rpc: receipts unavailable")
	mockEth.EXPECT().BlockReceipts(gomock.Any(), uint64(50)).Return(nil, receiptErr)

	_, err := fetcher.FetchIngestionLogs(context.Background(), 50, 50)
	require.ErrorIs(t, err, receiptErr)
	require.Contains(t, err.Error(), "dense block 50")
}

// TestFetchIngestionLogs_OtherErrorsPropagate pins that a non-cap provider
// failure is returned as-is (wrapped) instead of triggering the receipts path.
func TestFetchIngestionLogs_OtherErrorsPropagate(t *testing.T) {
	t.Parallel()

	fetcher, mockEth := newTestFetcher(t)
	rpcErr := errors.New("execution reverted")
	mockEth.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, rpcErr)

	logs, err := fetcher.FetchIngestionLogs(context.Background(), 10, 20)
	require.ErrorIs(t, err, rpcErr)
	require.Nil(t, logs)
}
