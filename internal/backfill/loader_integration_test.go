//go:build integration

package backfill

import (
	"context"
	"encoding/json"
	"math/big"
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

// gapFill produces the blocks strictly between the two runs the test's logs
// sit in, so eth_blocks is contiguous without hand-writing a million rows in
// one slice literal.
func gapFill(from, to int64) []exportBlock {
	out := make([]exportBlock, 0, to-from+1)
	for n := from; n <= to; n++ {
		out = append(out, exportBlock{Number: i64(n), BlockHash: common.BigToHash(big.NewInt(n)).Bytes(), Ts: i64(n * 10)})
	}
	return out
}

// writeManifest describes the test export the way the real one is described
// from BigQuery + GCS: interval, per-partition rows, per-file size and MD5.
func writeManifest(t *testing.T, dir string, first, last uint64, parts map[string]int64) {
	t.Helper()
	m := Manifest{Export: "test", Source: "test"}
	m.Blocks.First, m.Blocks.Last, m.Blocks.Rows = first, last, int64(last-first+1)
	m.Logs.Parts = parts
	for _, n := range parts {
		m.Logs.Rows += n
	}
	m.Files = map[string]ManifestFile{}
	for _, pattern := range []string{filepath.Join(dir, "*", "*.parquet"), filepath.Join(dir, "*", "*", "*.parquet")} {
		files, err := filepath.Glob(pattern)
		require.NoError(t, err)
		for _, path := range files {
			size, sum, err := fileSizeAndMD5(path)
			require.NoError(t, err)
			rel, err := filepath.Rel(dir, path)
			require.NoError(t, err)
			m.Files[filepath.ToSlash(rel)] = ManifestFile{Size: size, MD5: sum}
		}
	}
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, ManifestName), raw, 0o600))
}

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
	// The blocks export must be one contiguous run (finish checks it).
	var blocks []exportBlock
	for n := int64(7); n <= 1_000_005; n++ {
		if n > 12 && n < 1_000_001 {
			continue
		}
		blocks = append(blocks, exportBlock{Number: i64(n), BlockHash: common.BigToHash(big.NewInt(n)).Bytes(), Ts: i64(n * 10)})
	}
	writeParquet(t, filepath.Join(dir, "blocks", "blocks-000000000000.parquet"), blocks)
	writeParquet(t, filepath.Join(dir, "blocks", "blocks-000000000001.parquet"), gapFill(13, 1_000_000))
	// The live blocks table advanced after the logs extract: the export holds
	// blocks above the manifest's end. They must not be loaded.
	writeParquet(t, filepath.Join(dir, "blocks", "blocks-000000000002.parquet"), gapFill(1_000_006, 1_000_010))

	l := New(pool, dir)
	// No manifest: finish and the blocks stage refuse before touching anything.
	require.ErrorContains(t, l.Finish(ctx), "manifest.json is required at the export root")
	require.ErrorContains(t, l.Blocks(ctx), "manifest.json is required at the export root")
	require.ErrorContains(t, l.Logs(ctx), "manifest.json is required at the export root")
	writeManifest(t, dir, 7, 1_000_005, map[string]int64{"000": 2, "001": 2})

	require.NoError(t, l.Prepare(ctx))
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'eth_logs' AND indexname LIKE 'eth_logs_t%'`).Scan(&n))
	assert.Equal(t, 0, n, "secondary indexes dropped for the bulk load")

	// finish refuses to publish before every stage's data is present.
	err := l.Finish(ctx)
	require.ErrorContains(t, err, "no blocks loaded")
	require.NoError(t, l.Blocks(ctx))
	err = l.Finish(ctx)
	require.ErrorContains(t, err, "part 000 has 0 rows in the database, manifest.json says 2")
	_, ok, err := logstore.NewFromPool(pool).Coverage(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "cursor must stay unset until the load is verified")

	// Corruption-and-retry recovery: an altered file is refused BEFORE it is
	// loaded (nothing lands in the database), and after the file is restored
	// the stage loads it; a partition that holds a wrong row count (an
	// interrupted load, or rows from a since-replaced file) is reloaded rather
	// than skipped as "done".
	part0 := filepath.Join(dir, "logs", "part=000", "logs-000000000000.parquet")
	good, err := os.ReadFile(part0)
	require.NoError(t, err)
	bad := append(append([]byte{}, good[:len(good)-8]...), 1, 2, 3, 4, 5, 6, 7, 8) // a copy: good must stay intact for the restore
	require.NoError(t, os.WriteFile(part0, bad, 0o600))
	require.ErrorContains(t, l.Logs(ctx), "logs/part=000/logs-000000000000.parquet differs from manifest.json")
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs`).Scan(&n))
	assert.Equal(t, 0, n, "the altered file was never loaded")
	require.NoError(t, os.WriteFile(part0, good, 0o600))
	require.NoError(t, l.Logs(ctx))
	_, err = pool.Exec(ctx, `DELETE FROM eth_logs WHERE block_number = 7`)
	require.NoError(t, err)
	require.NoError(t, l.Logs(ctx), "a partition with a wrong count is cleared and reloaded")
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs WHERE block_number < 1000000`).Scan(&n))
	assert.Equal(t, 2, n)

	// A directory missing from the copy (not merely empty) blocks finish even
	// though eth_blocks is contiguous and every present directory matches.
	require.NoError(t, os.Rename(filepath.Join(dir, "logs", "part=001"), filepath.Join(dir, "part=001.aside")))
	err = l.Finish(ctx)
	require.ErrorContains(t, err, "export is missing logs/part=001 for blocks 1000000-1999999")
	require.NoError(t, os.Rename(filepath.Join(dir, "part=001.aside"), filepath.Join(dir, "logs", "part=001")))

	// A manifest that claims more logs than the export holds (a partial
	// export copied with a manifest from the full one) is caught by the
	// footer count before the database is consulted.
	writeManifest(t, dir, 7, 1_000_005, map[string]int64{"000": 2, "001": 3})
	require.ErrorContains(t, l.Finish(ctx), "part 001 files hold 2 rows, manifest.json says 3")
	// A manifest for a different block interval than what was loaded.
	writeManifest(t, dir, 7, 1_000_006, map[string]int64{"000": 2, "001": 2})
	require.ErrorContains(t, l.Finish(ctx), "manifest.json says 1000000 rows for 7-1000006")
	// A truncated or altered file is caught by its checksum.
	writeManifest(t, dir, 7, 1_000_005, map[string]int64{"000": 2, "001": 2})
	victim := filepath.Join(dir, "blocks", "blocks-000000000000.parquet")
	original, err := os.ReadFile(victim)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(victim, original[:len(original)-1], 0o600))
	require.ErrorContains(t, l.Finish(ctx), "differs from manifest.json")
	require.NoError(t, os.WriteFile(victim, original, 0o600))

	require.NoError(t, l.Finish(ctx))

	store := logstore.NewFromPool(pool)
	cov, ok, err := store.Coverage(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, logstore.Coverage{Start: 7, Head: 1_000_005}, cov)
	var above int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_blocks WHERE number > 1000005`).Scan(&above))
	assert.Equal(t, 0, above, "blocks above the manifest end are trimmed at load, so coverage cannot widen past the logs extract")

	logs, err := store.FilterLogs(ctx, logstore.Query{FromBlock: 0, ToBlock: 2_000_000}, 0)
	require.NoError(t, err)
	require.Len(t, logs, 4)
	assert.Equal(t, []uint64{7, 12, 1_000_001, 1_000_005}, []uint64{logs[0].BlockNumber, logs[1].BlockNumber, logs[2].BlockNumber, logs[3].BlockNumber})
	assert.Len(t, logs[1].Topics, 4)
	assert.Len(t, logs[0].Topics, 1)
	assert.Equal(t, []byte{}, logs[2].Data, "NULL data lands as empty bytes")
	assert.Equal(t, uint64(120), logs[1].BlockTimestamp)
	assert.Equal(t, common.BigToHash(big.NewInt(12)), logs[1].BlockHash)

	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM pg_indexes WHERE tablename = 'eth_logs'`).Scan(&n))
	assert.Equal(t, 5, n, "PK + four secondary indexes recreated")

	// Every stage is idempotent: a second run changes nothing.
	require.NoError(t, l.Logs(ctx))
	require.NoError(t, l.Blocks(ctx))
	require.NoError(t, l.Finish(ctx))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs`).Scan(&n))
	assert.Equal(t, 4, n)
}
