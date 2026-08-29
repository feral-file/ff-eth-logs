//go:build integration

package rpcapi

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
	"github.com/feral-file/ff-eth-logs/internal/testdb"
)

// TestServerRoundTripWithEthclient drives the server with go-ethereum's own
// client — the library the indexer uses — so wire shapes, hex encoding and
// error propagation are checked end to end rather than by JSON inspection.
func TestServerRoundTripWithEthclient(t *testing.T) {
	ctx := context.Background()
	store := logstore.NewFromPool(testdb.Open(t))
	owner := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ee")
	blocks := []logstore.Block{{Number: 5, Hash: common.HexToHash("0x55"), Timestamp: 1000}, {Number: 6, Hash: common.HexToHash("0x66"), Timestamp: 1012}}
	logs := []types.Log{
		{BlockNumber: 5, Index: 1, Address: common.HexToAddress("0xc0"), Topics: []common.Hash{eventset.Transfer, common.HexToHash("0x1"), owner, common.HexToHash("0x9")}, Data: []byte{}, TxHash: common.HexToHash("0xf5"), TxIndex: 2},
		{BlockNumber: 6, Index: 0, Address: common.HexToAddress("0xc0"), Topics: []common.Hash{eventset.Transfer, owner, common.HexToHash("0x2")}, Data: []byte{}, TxHash: common.HexToHash("0xf6")},
		{BlockNumber: 6, Index: 1, Address: common.HexToAddress("0xc0"), Topics: []common.Hash{eventset.Transfer, common.HexToHash("0x3"), common.HexToHash("0x4"), common.HexToHash("0x9")}, Data: []byte{}, TxHash: common.HexToHash("0xf7")},
	}
	require.NoError(t, store.WriteRange(ctx, 5, 6, blocks, logs))

	api := NewAPI(store, Config{ChainID: 1, MaxResults: 1})
	srv, err := NewServer(ServerConfig{Host: "127.0.0.1"}, api)
	require.NoError(t, err)
	ts := httptest.NewServer(srv.http.Handler)
	defer ts.Close()

	client, err := ethclient.Dial(ts.URL)
	require.NoError(t, err)
	defer client.Close()

	head, err := client.BlockNumber(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), head)
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), chainID.Int64())

	got, err := client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(5), ToBlock: big.NewInt(6),
		Topics: [][]common.Hash{{eventset.Transfer}, nil, {owner}, nil}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	want := logs[0]
	want.BlockHash, want.BlockTimestamp = blocks[0].Hash, blocks[0].Timestamp
	assert.Equal(t, want, got[0])

	// The 4-position filter excludes the 3-topic log, as on a node; a
	// 3-position one would not on a node, so the warehouse refuses it.
	got, err = client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(5), ToBlock: big.NewInt(6),
		Topics: [][]common.Hash{{eventset.Transfer}, {common.HexToHash("0x3")}, nil, nil}})
	require.NoError(t, err)
	assert.Len(t, got, 1, "4-topic logs only; the 3-topic one is excluded as on a node")
	_, err = client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(5), ToBlock: big.NewInt(6),
		Topics: [][]common.Hash{{eventset.Transfer}, nil, {owner}}})
	require.ErrorContains(t, err, "needs a 4-position topics filter")

	// max_results=1: the two 4-topic matches produce the Infura-style error.
	_, err = client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(5), ToBlock: big.NewInt(6), Topics: [][]common.Hash{{eventset.Transfer}, nil, nil, {common.HexToHash("0x9")}}})
	require.EqualError(t, err, "query returned more than 1 results")

	// Scope errors reach the client with code -32000.
	_, err = client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(5), ToBlock: big.NewInt(7), Topics: [][]common.Hash{{eventset.Transfer}, nil, nil, nil}})
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, -32000, rpcErr.ErrorCode())
	assert.Equal(t, "out of warehouse scope: blocks 5-7 extend above the warehouse head 6", rpcErr.Error())

	// History below the covered interval is refused, not answered with [].
	_, err = client.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: big.NewInt(0), ToBlock: big.NewInt(6), Topics: [][]common.Hash{{eventset.Transfer}, nil, nil, nil}})
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, "out of warehouse scope: blocks 0-6 extend below the warehouse coverage start 5", rpcErr.Error())

	// blockHash queries resolve through eth_blocks.
	h := blocks[1].Hash
	got, err = client.FilterLogs(ctx, ethereum.FilterQuery{BlockHash: &h, Topics: [][]common.Hash{{eventset.Transfer}, nil, nil, nil}})
	require.NoError(t, err)
	assert.Len(t, got, 1)
	assert.Equal(t, uint64(6), got[0].BlockNumber)

	// Unknown methods get geth's -32601 text; invalid params get -32602.
	raw := client.Client()
	var out json.RawMessage
	err = raw.CallContext(ctx, &out, "eth_getBalance", "0x0", "latest")
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, -32601, rpcErr.ErrorCode())
	assert.Equal(t, "the method eth_getBalance does not exist/is not available", rpcErr.Error())
	err = raw.CallContext(ctx, &out, "eth_getLogs", map[string]any{"topics": []any{"0x01"}})
	require.ErrorAs(t, err, &rpcErr)
	assert.Equal(t, -32602, rpcErr.ErrorCode())
	assert.True(t, strings.HasPrefix(rpcErr.Error(), "invalid argument 0:"), rpcErr.Error())

	// Batches work (geth rpc.Server handles them).
	batch := []rpc.BatchElem{
		{Method: "eth_blockNumber", Result: new(string)},
		{Method: "eth_chainId", Result: new(string)},
	}
	require.NoError(t, raw.BatchCallContext(ctx, batch))
	assert.Equal(t, "0x6", *batch[0].Result.(*string))
	assert.Equal(t, "0x1", *batch[1].Result.(*string))

	// Health page.
	resp, err := http.Get(ts.URL + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	var health map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&health))
	assert.Equal(t, "ok", health["status"])
	assert.Equal(t, float64(6), health["head"])
	assert.Equal(t, float64(5), health["coverage_start"])
}
