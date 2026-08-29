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
	start   uint64
	empty   bool
	headErr error
	logs    []types.Log
	logsErr error
	blocks  map[common.Hash]logstore.Block
	gotQ    *logstore.Query
	gotLim  int
}

func (f *fakeWarehouse) Read(_ context.Context, fn func(logstore.View) error) error { return fn(f) }

func (f *fakeWarehouse) Coverage(context.Context) (logstore.Coverage, bool, error) {
	return logstore.Coverage{Start: f.start, Head: f.head}, !f.empty, f.headErr
}

func (f *fakeWarehouse) FilterLogs(_ context.Context, q logstore.Query, limit int) ([]types.Log, error) {
	f.gotQ, f.gotLim = &q, limit
	return f.logs, f.logsErr
}

func (f *fakeWarehouse) BlockByHash(_ context.Context, h common.Hash) (logstore.Block, bool, error) {
	b, ok := f.blocks[h]
	return b, ok, nil
}

// transfer4 is the exact Transfer filter: four positions, so only 4-topic
// logs match on a node as well.
func transfer4() [][]common.Hash { return [][]common.Hash{{eventset.Transfer}, nil, nil, nil} }

func transferCrit(from, to int64) FilterCriteria {
	return FilterCriteria{FromBlock: big.NewInt(from), ToBlock: big.NewInt(to), Topics: transfer4()}
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
	_, err = api.GetLogs(ctx, FilterCriteria{Topics: transfer4()})
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
	logs, err := api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(50), ToBlock: big.NewInt(rpc.EarliestBlockNumber.Int64()), Topics: transfer4()})
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

	_, err = api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(10), ToBlock: big.NewInt(20), Topics: [][]common.Hash{{eventset.Transfer, common.HexToHash("0x1")}, nil, nil, nil}})
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

// TestGetLogs_BelowCoverageStart pins that a warehouse holding only a tail
// (fresh database ingesting from the tip) refuses history it never loaded
// instead of answering [] — the routing client must go to the vendor.
func TestGetLogs_BelowCoverageStart(t *testing.T) {
	ctx := context.Background()
	wh := &fakeWarehouse{start: 90, head: 100}
	api := NewAPI(wh, Config{ChainID: 1})

	var scope *ScopeError
	_, err := api.GetLogs(ctx, transferCrit(0, 100))
	require.ErrorAs(t, err, &scope)
	assert.Equal(t, "out of warehouse scope: blocks 0-100 extend below the warehouse coverage start 90", err.Error())

	_, err = api.GetLogs(ctx, transferCrit(rpc.EarliestBlockNumber.Int64(), 95))
	require.ErrorAs(t, err, &scope, "earliest resolves to 0, below coverage")

	_, err = api.GetLogs(ctx, transferCrit(90, 100))
	require.NoError(t, err)
	assert.Equal(t, uint64(90), wh.gotQ.FromBlock)

	// A range that is empty after resolution stays [] rather than an error.
	logs, err := api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(50), ToBlock: big.NewInt(rpc.EarliestBlockNumber.Int64()), Topics: transfer4()})
	require.NoError(t, err)
	assert.Empty(t, logs)
}

// TestGetLogs_CryptoPunksMustPinAddress pins that a CryptoPunks signature is
// only servable when the address selector is exactly the CryptoPunks
// contract: the same signatures from other contracts are not stored.
func TestGetLogs_CryptoPunksMustPinAddress(t *testing.T) {
	ctx := context.Background()
	wh := &fakeWarehouse{head: 100}
	api := NewAPI(wh, Config{ChainID: 1})
	punks := [][]common.Hash{{eventset.PunkTransfer, eventset.Transfer}, nil, nil, nil}
	other := common.HexToAddress("0x1")

	var scope *ScopeError
	_, err := api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2), Topics: punks})
	require.ErrorAs(t, err, &scope)
	assert.Contains(t, err.Error(), "CryptoPunks signatures are stored only for")

	_, err = api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2), Topics: punks, Addresses: []common.Address{eventset.CryptoPunksAddress, other}})
	require.ErrorAs(t, err, &scope)

	_, err = api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2), Topics: punks, Addresses: []common.Address{eventset.CryptoPunksAddress}})
	require.NoError(t, err)

	// Standard signatures need no address pin.
	_, err = api.GetLogs(ctx, FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2), Topics: transfer4(), Addresses: []common.Address{other}})
	require.NoError(t, err)
}

// TestGetLogs_ShapeScope pins that a filter is served only when its position
// count makes the stored shape the only shape a node could return: the
// Transfer family needs four positions (wildcards allowed) because a node
// would also return 3-topic ERC-20 and 1-topic pre-standard Transfers; the
// metadata/URI signatures cannot be pinned by any filter and are refused;
// CryptoPunks signatures are stored in every shape and pass with the address.
func TestGetLogs_ShapeScope(t *testing.T) {
	ctx := context.Background()
	api := NewAPI(&fakeWarehouse{head: 100}, Config{ChainID: 1})
	crit := func(topics [][]common.Hash, addrs ...common.Address) FilterCriteria {
		return FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2), Topics: topics, Addresses: addrs}
	}
	var scope *ScopeError

	for _, sig := range []common.Hash{eventset.Transfer, eventset.TransferSingle, eventset.TransferBatch} {
		_, err := api.GetLogs(ctx, crit([][]common.Hash{{sig}}))
		require.ErrorAs(t, err, &scope, sig.Hex())
		assert.Contains(t, err.Error(), "needs a 4-position topics filter")
		_, err = api.GetLogs(ctx, crit([][]common.Hash{{sig}, nil, {common.HexToHash("0xee")}}))
		require.ErrorAs(t, err, &scope, "3 positions still admit 3-topic logs on a node")
		_, err = api.GetLogs(ctx, crit([][]common.Hash{{sig}, nil, nil, nil}))
		require.NoError(t, err, sig.Hex())
	}
	for _, sig := range []common.Hash{eventset.MetadataUpdate, eventset.BatchMetadataUpdate, eventset.URI} {
		_, err := api.GetLogs(ctx, crit([][]common.Hash{{sig}}))
		require.ErrorAs(t, err, &scope, sig.Hex())
		assert.Contains(t, err.Error(), "stored only in its standard shape")
		_, err = api.GetLogs(ctx, crit([][]common.Hash{{eventset.Transfer, sig}, nil, nil, nil}))
		require.ErrorAs(t, err, &scope, "mixed with Transfer it is still refused")
	}
	_, err := api.GetLogs(ctx, crit([][]common.Hash{{eventset.PunkTransfer}}, eventset.CryptoPunksAddress))
	require.NoError(t, err, "every CryptoPunks shape is stored, so one position is exact")
}

func TestGetLogs_BlockHash(t *testing.T) {
	ctx := context.Background()
	h := common.HexToHash("0xaa")
	wh := &fakeWarehouse{head: 100, blocks: map[common.Hash]logstore.Block{h: {Number: 42, Hash: h}}}
	api := NewAPI(wh, Config{ChainID: 1})

	_, err := api.GetLogs(ctx, FilterCriteria{BlockHash: &h, Topics: transfer4()})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), wh.gotQ.FromBlock)
	assert.Equal(t, uint64(42), wh.gotQ.ToBlock)

	unknown := common.HexToHash("0xbb")
	_, err = api.GetLogs(ctx, FilterCriteria{BlockHash: &unknown, Topics: transfer4()})
	assert.EqualError(t, err, "unknown block")

	// A stored block outside the published interval (rows left by an
	// interrupted backfill, or above a rewound head) is refused, not served.
	wh.start, wh.gotQ = 50, nil
	_, err = api.GetLogs(ctx, FilterCriteria{BlockHash: &h, Topics: transfer4()})
	var scope *ScopeError
	require.ErrorAs(t, err, &scope)
	assert.Equal(t, "out of warehouse scope: block 42 (0x00000000000000000000000000000000000000000000000000000000000000aa) is outside the warehouse coverage 50-100", err.Error())
	assert.Nil(t, wh.gotQ)
	wh.start, wh.head = 0, 41
	_, err = api.GetLogs(ctx, FilterCriteria{BlockHash: &h, Topics: transfer4()})
	require.ErrorAs(t, err, &scope)
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
