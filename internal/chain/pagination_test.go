package chain_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/mocks"
)

type blockRange struct {
	from uint64
	to   uint64
}

func mergeBlockRanges(ranges []blockRange) []blockRange {
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].from == ranges[j].from {
			return ranges[i].to < ranges[j].to
		}
		return ranges[i].from < ranges[j].from
	})
	merged := []blockRange{ranges[0]}
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if r.from <= last.to+1 {
			if r.to > last.to {
				last.to = r.to
			}
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

func requireContiguousCoverage(t *testing.T, ranges []blockRange, fromBlock, toBlock uint64) {
	t.Helper()
	merged := mergeBlockRanges(ranges)
	require.NotEmpty(t, merged)
	require.Equal(t, fromBlock, merged[0].from, "coverage starts at wrong block")
	require.Equal(t, toBlock, merged[len(merged)-1].to, "coverage ends at wrong block")
	for i := 1; i < len(merged); i++ {
		require.Equal(t, merged[i-1].to+1, merged[i].from,
			"gap between ranges [%d-%d] and [%d-%d]",
			merged[i-1].from, merged[i-1].to, merged[i].from, merged[i].to)
	}
}

func query(from, to uint64) ethereum.FilterQuery {
	return ethereum.FilterQuery{FromBlock: new(big.Int).SetUint64(from), ToBlock: new(big.Int).SetUint64(to)}
}

// newMocks returns a strict client and a clock that tolerates any sleep.
func newMocks(t *testing.T) (*mocks.MockEthClient, *mocks.MockClock) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes()
	return mocks.NewMockEthClient(ctrl), mockClock
}

func TestPaginator_SingleBlockRange(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	const blockNum uint64 = 12_345_678
	expectedLog := types.Log{Address: common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"), BlockNumber: blockNum, Index: 1}
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			require.Equal(t, blockNum, q.FromBlock.Uint64())
			require.Equal(t, blockNum, q.ToBlock.Uint64())
			require.Equal(t, []common.Address{expectedLog.Address}, q.Addresses, "the query's other fields pass through")
			return []types.Log{expectedLog}, nil
		})

	q := query(blockNum, blockNum)
	q.Addresses = []common.Address{expectedLog.Address}
	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, []types.Log{expectedLog}, logs)
}

func TestPaginator_RequiresExplicitRange(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	p := chain.NewPaginator(mockClient, mockClock, 0)
	for _, q := range []ethereum.FilterQuery{{}, {FromBlock: big.NewInt(1)}, {ToBlock: big.NewInt(1)}} {
		_, err := p.FilterLogs(context.Background(), q)
		require.ErrorContains(t, err, "explicit fromBlock and toBlock")
	}
}

func TestPaginator_ReturnsLogOnOuterPageBoundaryBlock(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	const boundaryBlock uint64 = 1_000_000
	boundaryLog := types.Log{BlockNumber: boundaryBlock, Index: 7}
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			if q.FromBlock.Uint64() <= boundaryBlock && q.ToBlock.Uint64() >= boundaryBlock {
				return []types.Log{boundaryLog}, nil
			}
			return nil, nil
		})

	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(0, 2_000_000))
	require.NoError(t, err)
	require.Equal(t, []types.Log{boundaryLog}, logs)
}

func TestPaginator_ContiguousCoverageAcrossOuterWindows(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	const fromBlock, toBlock uint64 = 0, 2_500_000
	var queried []blockRange
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			queried = append(queried, blockRange{from: q.FromBlock.Uint64(), to: q.ToBlock.Uint64()})
			return nil, nil
		})

	_, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(fromBlock, toBlock))
	require.NoError(t, err)
	requireContiguousCoverage(t, queried, fromBlock, toBlock)
	require.Len(t, queried, 3, "1M default step: three outer windows")
}

func TestPaginator_ReturnsLogsFromMultipleOuterPages(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	logsByBlock := map[uint64]types.Log{
		100:       {BlockNumber: 100, Index: 1},
		1_000_100: {BlockNumber: 1_000_100, Index: 2},
		2_000_200: {BlockNumber: 2_000_200, Index: 3},
	}
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			var logs []types.Log
			for block, log := range logsByBlock {
				if q.FromBlock.Uint64() <= block && q.ToBlock.Uint64() >= block {
					logs = append(logs, log)
				}
			}
			return logs, nil
		})

	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(0, 2_500_000))
	require.NoError(t, err)
	require.Len(t, logs, len(logsByBlock))
	for i := 1; i < len(logs); i++ {
		require.Less(t, logs[i-1].BlockNumber, logs[i].BlockNumber, "outer windows append in ascending order")
	}
}

// resultCapped returns a FilterLogs stub that rejects windows wider than
// maxSpan blocks with a result-count error and records every query.
func resultCapped(maxSpan uint64, queried *[]blockRange) func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	return func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
		from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
		*queried = append(*queried, blockRange{from: from, to: to})
		if to-from+1 > maxSpan {
			return nil, fmt.Errorf("query returned more than 10000 results")
		}
		return nil, nil
	}
}

func TestPaginator_AdaptiveHalvingStillCoversRange(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	const fromBlock, toBlock uint64 = 0, 10_000
	var queried []blockRange
	mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(resultCapped(1_000, &queried))

	_, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(fromBlock, toBlock))
	require.NoError(t, err)
	var accepted []blockRange
	for _, r := range queried {
		if r.to-r.from+1 <= 1_000 {
			accepted = append(accepted, r)
		}
	}
	requireContiguousCoverage(t, accepted, fromBlock, toBlock)
}

// TestPaginator_RampsStepGraduallyAfterSuccess pins the ramp: after the
// halving cascade finds an accepted step, the next window is twice that step,
// not a reset to the 1M default (which would re-pay the whole cascade), and
// every rejection sleeps once.
func TestPaginator_RampsStepGraduallyAfterSuccess(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	sleeps := 0
	mockClock.EXPECT().Sleep(time.Second).AnyTimes().Do(func(time.Duration) { sleeps++ })
	var queried []blockRange
	mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(resultCapped(1_000, &queried))

	_, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(0, 10_000))
	require.NoError(t, err)

	span := func(r blockRange) uint64 { return r.to - r.from + 1 }
	first := -1
	rejections := 0
	for i, r := range queried {
		if span(r) > 1_000 {
			rejections++
		} else if first < 0 {
			first = i
		}
	}
	require.Greater(t, first, 0, "the default step is rejected before the first success")
	require.Equal(t, uint64(976), span(queried[first]), "1M halved ten times")
	require.Equal(t, 2*span(queried[first]), span(queried[first+1]), "the next window doubles the accepted step")
	require.Equal(t, rejections, sleeps, "one back-off sleep per rejection")
}

// TestPaginator_OneBlockTooManyResultsReturnsOverflow pins that when a
// single-block query is still over the cap, the walk reports that block as a
// SingleBlockOverflowError instead of partial success, so the caller can serve
// it from receipts.
func TestPaginator_OneBlockTooManyResultsReturnsOverflow(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	capErr := errors.New("query returned more than 10000 results")
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			if q.FromBlock.Uint64() <= 100 && 100 <= q.ToBlock.Uint64() {
				return nil, capErr
			}
			return nil, nil
		})

	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(100, 100))
	require.Nil(t, logs)
	var overflow *chain.SingleBlockOverflowError
	require.ErrorAs(t, err, &overflow)
	require.Equal(t, uint64(100), overflow.Block)
	require.ErrorIs(t, err, capErr)
	require.Contains(t, err.Error(), "too many results in single block 100")
}

func TestPaginator_OtherErrorsAreFatal(t *testing.T) {
	t.Parallel()

	mockClient, mockClock := newMocks(t)
	rpcErr := errors.New("execution reverted")
	mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, rpcErr)

	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(5, 10))
	require.Nil(t, logs)
	require.ErrorIs(t, err, rpcErr)
	require.Contains(t, err.Error(), "failed to get logs for range 5-10")
}

// spanCapped returns a FilterLogs stub for a provider that caps the queried
// block span ("range N exceeds limit of M") and tracks whether the walk ever
// probes above the cap again after a success.
type spanCapped struct {
	limit                  uint64
	successes              []blockRange
	totalCalls             int
	rejectionsAfterSuccess int
}

func (s *spanCapped) filterLogs(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	s.totalCalls++
	from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
	if span := to - from + 1; span > s.limit {
		if len(s.successes) > 0 {
			s.rejectionsAfterSuccess++
		}
		return nil, fmt.Errorf("range %d exceeds limit of %d", span, s.limit)
	}
	s.successes = append(s.successes, blockRange{from: from, to: to})
	return nil, nil
}

func sleepTotal(ctrl *gomock.Controller) (*mocks.MockClock, *time.Duration) {
	var total time.Duration
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes().Do(func(d time.Duration) { total += d })
	return mockClock, &total
}

// TestPaginator_RangeCappedProviderSplitsAndCompletes pins that a span cap is
// recognized, the window is split down to an accepted span, the whole range
// is covered, and the walk never probes above the discovered cap again —
// every above-cap probe is a guaranteed rejection plus a one-second sleep.
func TestPaginator_RangeCappedProviderSplitsAndCompletes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock, slept := sleepTotal(ctrl)
	provider := &spanCapped{limit: 16}
	mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(provider.filterLogs)

	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(0, 1000))
	require.NoError(t, err)
	require.Empty(t, logs)
	requireContiguousCoverage(t, provider.successes, 0, 1000)
	require.Zero(t, provider.rejectionsAfterSuccess, "pagination probed above the discovered range cap after a success")
	require.LessOrEqual(t, *slept, 20*time.Second, "pagination slept beyond the initial halving cascade")
	require.Less(t, provider.totalCalls, 120, "pagination is re-paying rejected probes after successes")
}

// TestPaginator_CapHoistedAcrossOuterWindows pins that the cap discovered in
// the first outer window is carried to the remaining ones instead of being
// re-probed (and re-slept) per window.
func TestPaginator_CapHoistedAcrossOuterWindows(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock, slept := sleepTotal(ctrl)
	provider := &spanCapped{limit: 500_000}
	mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(provider.filterLogs)

	logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(context.Background(), query(0, 3_999_999))
	require.NoError(t, err)
	require.Empty(t, logs)
	requireContiguousCoverage(t, provider.successes, 0, 3_999_999)
	require.Zero(t, provider.rejectionsAfterSuccess, "a later outer window probed above the cap discovered earlier in the walk")
	require.LessOrEqual(t, *slept, 2*time.Second, "pagination re-paid the halving cascade in later outer windows")
	require.Less(t, provider.totalCalls, 16, "pagination made rejected probes beyond the initial discovery")
}

// TestPaginator_SpanCapSeedsStepWithoutProbing pins the cost contract of a
// configured span cap: no window is wider than the cap (so no cascade and no
// sleeps — the strict clock fails the test on any Sleep), and outer windows
// align with the inner step so the range takes exactly ceil(blocks/(cap+1))
// calls.
func TestPaginator_SpanCapSeedsStepWithoutProbing(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl) // no Sleep expectation: any sleep fails
	const spanCap uint64 = 16
	var queried []blockRange
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
			require.LessOrEqual(t, to-from, spanCap, "window wider than the configured span cap")
			queried = append(queried, blockRange{from: from, to: to})
			return nil, nil
		})

	// 110 blocks at cap+1 = 17 per window -> exactly 7 calls.
	logs, err := chain.NewPaginator(mockClient, mockClock, spanCap).FilterLogs(context.Background(), query(0, 109))
	require.NoError(t, err)
	require.Empty(t, logs)
	requireContiguousCoverage(t, queried, 0, 109)
	require.Len(t, queried, 7, "a span-cap-seeded walk must cover the range in exactly ceil(blocks/(cap+1)) calls")
}

// TestPaginator_DeadlineReturnsPartialLogs pins that a context deadline mid-walk
// returns the logs gathered so far together with the context error, on both
// the "next window" check and the "call failed under a dead context" path.
func TestPaginator_DeadlineReturnsPartialLogs(t *testing.T) {
	t.Parallel()

	t.Run("deadline elapses between windows", func(t *testing.T) {
		t.Parallel()
		mockClient, mockClock := newMocks(t)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		first := types.Log{BlockNumber: 5, Index: 0}
		mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
				<-ctx.Done() // the provider answers only after the deadline passed
				return []types.Log{first}, nil
			})

		logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(ctx, query(0, 2_000_000))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, []types.Log{first}, logs, "partial logs are returned with the error")
	})

	t.Run("call fails under a dead context", func(t *testing.T) {
		t.Parallel()
		mockClient, mockClock := newMocks(t)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		first := types.Log{BlockNumber: 5, Index: 0}
		gomock.InOrder(
			mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return([]types.Log{first}, nil),
			mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}),
		)

		logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(ctx, query(0, 2_000_000))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, []types.Log{first}, logs)
	})

	t.Run("cancellation without a deadline", func(t *testing.T) {
		t.Parallel()
		mockClient, mockClock := newMocks(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		first := types.Log{BlockNumber: 5, Index: 0}
		mockClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
				cancel()
				return []types.Log{first}, nil
			})

		logs, err := chain.NewPaginator(mockClient, mockClock, 0).FilterLogs(ctx, query(0, 2_000_000))
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, []types.Log{first}, logs)
	})
}
