package rpcapi

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

// fakeWarehouse records the query the API resolved and returns canned logs.
type fakeWarehouse struct {
	head    uint64
	empty   bool
	headErr error
	logs    []types.Log
	logsErr error
	blocks  map[common.Hash]logstore.Block
	gotQ    *logstore.Query
	gotLim  int
}

func (f *fakeWarehouse) Head(context.Context) (uint64, bool, error) {
	return f.head, !f.empty, f.headErr
}

func (f *fakeWarehouse) FilterLogs(_ context.Context, q logstore.Query, limit int) ([]types.Log, error) {
	f.gotQ, f.gotLim = &q, limit
	return f.logs, f.logsErr
}

func (f *fakeWarehouse) BlockByHash(_ context.Context, h common.Hash) (logstore.Block, bool, error) {
	b, ok := f.blocks[h]
	return b, ok, nil
}

func transferCrit(from, to int64) FilterCriteria {
	return FilterCriteria{FromBlock: big.NewInt(from), ToBlock: big.NewInt(to), Topics: [][]common.Hash{{eventset.Transfer}}}
}

func TestGetLogs_ResolvesRangeAndTags(t *testing.T) {
	ctx := context.Background()
	wh := &fakeWarehouse{head: 100}
	api := NewAPI(wh, Config{ChainID: 1, MaxResults: 50})

	_, err := api.GetLogs(ctx, transferCrit(10, 20))
	require.NoError(t, err)
	assert.Equal(t, uint64(10), wh.gotQ.FromBlock)
	assert.Equal(t, uint64(20), wh.gotQ.ToBlock)
	assert.Equal(t, 50, wh.gotLim)

	// Missing bounds default to latest, which is the warehouse head.
	_, err = api.GetLogs(ctx, FilterCriteria{Topics: [][]common.Hash{{eventset.Transfer}}})
	require.NoError(t, err)
	assert.Equal(t, uint64(100), wh.gotQ.FromBlock)
	assert.Equal(t, uint64(100), wh.gotQ.ToBlock)

	for _, tag := range []rpc.BlockNumber{rpc.LatestBlockNumber, rpc.SafeBlockNumber, rpc.FinalizedBlockNumber} {
		_, err = api.GetLogs(ctx, transferCrit(rpc.EarliestBlockNumber.Int64(), tag.Int64()))
		require.NoError(t, err, tag)
		assert.Equal(t, uint64(0), wh.gotQ.FromBlock)
		assert.Equal(t, uint64(100), wh.gotQ.ToBlock)
	}
}

func TestGetLogs_GethErrorsAndEmpty(t *testing.T) {
	ctx := context.Background()
	wh := &fakeWarehouse{head: 100}
	api := NewAPI(wh, Config{ChainID: 1})

	_, err := api.GetLogs(ctx, transferCrit(20, 10))
	assert.EqualError(t, err, "invalid block range params")

	_, err = api.GetLogs(ctx, transferCrit(rpc.PendingBlockNumber.Int64(), 10))
	assert.EqualError(t, err, "pending logs are not supported")

	_, err = api.GetLogs(ctx, transferCrit(-7, 10))
	assert.EqualError(t, err, "negative block number")

	// from > to after tag resolution (to = earliest) is [] on geth, and the
	// store must not be touched.
	wh.gotQ = nil
	logs, err := api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(50), ToBlock: big.NewInt(rpc.EarliestBlockNumber.Int64()), Topics: [][]common.Hash{{eventset.Transfer}}})
	require.NoError(t, err)
	assert.NotNil(t, logs)
	assert.Empty(t, logs)
	assert.Nil(t, wh.gotQ)

	five := make([][]common.Hash, 5)
	_, err = api.GetLogs(ctx, FilterCriteria{Topics: five})
	assert.EqualError(t, err, "exceed max topics")
}

func TestGetLogs_ScopeErrors(t *testing.T) {
	ctx := context.Background()
	wh := &fakeWarehouse{head: 100}
	api := NewAPI(wh, Config{ChainID: 1})

	var scope *ScopeError
	_, err := api.GetLogs(ctx, transferCrit(10, 101))
	require.ErrorAs(t, err, &scope)
	assert.Equal(t, -32000, scope.ErrorCode())
	assert.Equal(t, "out of warehouse scope: blocks 10-101 extend above the warehouse head 100", err.Error())

	_, err = api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(10), ToBlock: big.NewInt(20)})
	require.ErrorAs(t, err, &scope)
	assert.Contains(t, err.Error(), "topics[0]")

	_, err = api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(10), ToBlock: big.NewInt(20), Topics: [][]common.Hash{{eventset.Transfer, common.HexToHash("0x1")}}})
	require.ErrorAs(t, err, &scope)
	assert.Contains(t, err.Error(), "is not a warehouse event signature")

	// Scope messages must not trip a range-cap / result-cap classifier.
	for _, banned := range []string{"range", "limit", "too many"} {
		assert.NotContains(t, err.Error(), banned)
	}

	wh.empty = true
	_, err = api.GetLogs(ctx, transferCrit(10, 20))
	require.ErrorAs(t, err, &scope)
	_, err = api.BlockNumber(ctx)
	require.ErrorAs(t, err, &scope)
}

func TestGetLogs_BlockHash(t *testing.T) {
	ctx := context.Background()
	h := common.HexToHash("0xaa")
	wh := &fakeWarehouse{head: 100, blocks: map[common.Hash]logstore.Block{h: {Number: 42, Hash: h}}}
	api := NewAPI(wh, Config{ChainID: 1})

	_, err := api.GetLogs(ctx, FilterCriteria{BlockHash: &h, Topics: [][]common.Hash{{eventset.Transfer}}})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), wh.gotQ.FromBlock)
	assert.Equal(t, uint64(42), wh.gotQ.ToBlock)

	unknown := common.HexToHash("0xbb")
	_, err = api.GetLogs(ctx, FilterCriteria{BlockHash: &unknown, Topics: [][]common.Hash{{eventset.Transfer}}})
	assert.EqualError(t, err, "unknown block")
}

func TestGetLogs_TooManyResultsMimicsInfura(t *testing.T) {
	wh := &fakeWarehouse{head: 100, logsErr: logstore.ErrTooManyResults}
	api := NewAPI(wh, Config{ChainID: 1, MaxResults: 10000})
	_, err := api.GetLogs(context.Background(), transferCrit(1, 2))
	assert.EqualError(t, err, "query returned more than 10000 results")

	wh.logsErr = errors.New("boom")
	_, err = api.GetLogs(context.Background(), transferCrit(1, 2))
	assert.EqualError(t, err, "boom")
}

func TestBlockNumberAndChainID(t *testing.T) {
	api := NewAPI(&fakeWarehouse{head: 123}, Config{ChainID: 1})
	n, err := api.BlockNumber(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(123), uint64(n))
	assert.Equal(t, "0x1", api.ChainId().String())

	_, err = NewAPI(&fakeWarehouse{headErr: errors.New("db down")}, Config{}).BlockNumber(context.Background())
	assert.EqualError(t, err, "db down")
}
