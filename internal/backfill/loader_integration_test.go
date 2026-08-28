//go:build integration

package backfill

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-eth-logs/internal/logstore"
	"github.com/feral-file/ff-eth-logs/internal/testdb"
)

// exportLog mirrors the BigQuery export's logs schema (every column optional,
// hex fields as raw bytes, block_timestamp in microseconds).
type exportLog struct {
	BlockNumber    *int64 `parquet:"block_number,optional"`
	LogIndex       *int64 `parquet:"log_index,optional"`
	TxIndex        *int64 `parquet:"tx_index,optional"`
	TxHash         []byte `parquet:"tx_hash,optional"`
	Address        []byte `parquet:"address,optional"`
	Topic0         []byte `parquet:"topic0,optional"`
	Topic1         []byte `parquet:"topic1,optional"`
	Topic2         []byte `parquet:"topic2,optional"`
	Topic3         []byte `parquet:"topic3,optional"`
	Data           []byte `parquet:"data,optional"`
	BlockTimestamp *int64 `parquet:"block_timestamp,optional"`
}

type exportBlock struct {
	Number    *int64 `parquet:"number,optional"`
	BlockHash []byte `parquet:"block_hash,optional"`
	Ts        *int64 `parquet:"ts,optional"`
}

func i64(v int64) *int64 { return &v }

func writeParquet[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
	f, err := os.Create(path)
	require.NoError(t, err)
	w := parquet.NewGenericWriter[T](f)
	_, err = w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
}

func TestLoaderEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Open(t)
	dir := t.TempDir()
	h := func(b byte) []byte { return common.HexToHash(string(rune(b))).Bytes() }

	// Two partitions, files deliberately unsorted; part 1 split across two files.
	writeParquet(t, filepath.Join(dir, "logs", "part=000", "logs-000000000000.parquet"), []exportLog{
		{BlockNumber: i64(12), LogIndex: i64(4), TxIndex: i64(1), TxHash: h('a'), Address: common.HexToAddress("0x1").Bytes(), Topic0: h('t'), Topic1: h('1'), Topic2: h('2'), Topic3: h('3'), Data: []byte{}, BlockTimestamp: i64(1)},
		{BlockNumber: i64(7), LogIndex: i64(0), TxIndex: i64(0), TxHash: h('b'), Address: common.HexToAddress("0x1").Bytes(), Topic0: h('t'), Data: []byte{9}, BlockTimestamp: i64(1)},
	})
	writeParquet(t, filepath.Join(dir, "logs", "part=001", "logs-000000000000.parquet"), []exportLog{
		{BlockNumber: i64(1_000_005), LogIndex: i64(2), TxIndex: i64(0), TxHash: h('c'), Address: common.HexToAddress("0x2").Bytes(), Topic0: h('t'), Topic1: h('1'), Data: nil, BlockTimestamp: i64(1)},
	})
	writeParquet(t, filepath.Join(dir, "logs", "part=001", "logs-000000000001.parquet"), []exportLog{
		{BlockNumber: i64(1_000_001), LogIndex: i64(0), TxIndex: i64(0), TxHash: h('d'), Address: common.HexToAddress("0x2").Bytes(), Topic0: h('t'), Data: []byte{}, BlockTimestamp: i64(1)},
	})
	writeParquet(t, filepath.Join(dir, "blocks", "blocks-000000000000.parquet"), []exportBlock{
		{Number: i64(7), BlockHash: h('7'), Ts: i64(70)}, {Number: i64(12), BlockHash: h('9'), Ts: i64(120)},
		{Number: i64(1_000_001), BlockHash: h('x'), Ts: i64(1)}, {Number: i64(1_000_005), BlockHash: h('y'), Ts: i64(5)},
	})

	l := New(pool, dir)
	require.NoError(t, l.Prepare(ctx))
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'eth_logs' AND indexname LIKE 'eth_logs_t%'`).Scan(&n))
	assert.Equal(t, 0, n, "secondary indexes dropped for the bulk load")

	require.NoError(t, l.Logs(ctx))
	require.NoError(t, l.Blocks(ctx))
	require.NoError(t, l.Finish(ctx))

	store := logstore.NewFromPool(pool)
	head, ok, err := store.Cursor(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(1_000_005), head)

	logs, err := store.FilterLogs(ctx, logstore.Query{FromBlock: 0, ToBlock: 2_000_000}, 0)
	require.NoError(t, err)
	require.Len(t, logs, 4)
	assert.Equal(t, []uint64{7, 12, 1_000_001, 1_000_005}, []uint64{logs[0].BlockNumber, logs[1].BlockNumber, logs[2].BlockNumber, logs[3].BlockNumber})
	assert.Len(t, logs[1].Topics, 4)
	assert.Len(t, logs[0].Topics, 1)
	assert.Equal(t, []byte{}, logs[2].Data, "NULL data lands as empty bytes")
	assert.Equal(t, uint64(120), logs[1].BlockTimestamp)
	assert.Equal(t, common.BytesToHash(h('9')), logs[1].BlockHash)

	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'eth_logs'`).Scan(&n))
	assert.Equal(t, 5, n, "PK + four secondary indexes recreated")

	// Every stage is idempotent: a second run changes nothing.
	require.NoError(t, l.Logs(ctx))
	require.NoError(t, l.Blocks(ctx))
	require.NoError(t, l.Finish(ctx))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs`).Scan(&n))
	assert.Equal(t, 4, n)
}
