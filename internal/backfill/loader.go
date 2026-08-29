// Package backfill loads the one-off BigQuery export (Parquet, one directory
// per 1 M blocks — see docs/operations.md) into the warehouse.
//
// Stages, each idempotent and resumable:
//
//	prepare  drop the secondary indexes (bulk-loading 400 M rows through four
//	         random-order btrees would take days; sorted PK inserts are fine)
//	logs     per part=NNN directory: COPY every Parquet file into an unlogged
//	         staging table, then INSERT ... ORDER BY (block_number, log_index)
//	         into eth_logs so the heap is chain-ordered; skip directories whose
//	         range already has rows
//	blocks   COPY blocks-*.parquet into eth_blocks (skip if already populated)
//	finish   verify the load against manifest.json (interval, per-partition
//	         rows, file checksums), recreate the indexes, ANALYZE, publish
//	         coverage [manifest first block, manifest last block]
//
// Sorting happens in Postgres (external sort, bounded by work_mem) because
// the busiest 1 M-block partition holds ~60 M rows — far too many to sort in
// the loader's memory.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
	"go.uber.org/zap"

	"github.com/feral-file/ff-eth-logs/internal/logger"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

// Loader runs the stages against one database from one export directory.
type Loader struct {
	pool *pgxpool.Pool
	dir  string
}

// New creates a Loader over the export root (the directory containing
// logs/part=NNN/ and blocks/).
func New(pool *pgxpool.Pool, dir string) *Loader { return &Loader{pool: pool, dir: dir} }

// Prepare drops the secondary indexes so the bulk load only maintains the
// primary key.
func (l *Loader) Prepare(ctx context.Context) error {
	for _, idx := range logstore.SecondaryIndexes {
		if _, err := l.pool.Exec(ctx, "DROP INDEX IF EXISTS "+idx.Name); err != nil {
			return fmt.Errorf("drop index %s: %w", idx.Name, err)
		}
		logger.InfoCtx(ctx, "Dropped index for bulk load", zap.String("index", idx.Name))
	}
	return nil
}

// Finish verifies the load, recreates the indexes, analyzes, and publishes
// coverage [oldest block, newest block] so tail ingestion resumes right after
// the export and the API starts answering.
//
// Reason: the stages can be run individually and in any order, so the cursor
// must not be derived from whatever happens to be in eth_blocks — nor from
// the files alone, which can only be compared with themselves. Finish checks
// the database, the Parquet footers and the local files against the export
// manifest (see Manifest) before it writes the cursor row. Until then the
// API reports the warehouse as empty rather than serving a partial history.
func (l *Loader) Finish(ctx context.Context) error {
	cov, err := l.verifyLoaded(ctx)
	if err != nil {
		return fmt.Errorf("backfill is not complete, cursor not set: %w", err)
	}
	for _, idx := range logstore.SecondaryIndexes {
		start := time.Now()
		if _, err := l.pool.Exec(ctx, idx.DDL); err != nil && !isDuplicateRelation(err) {
			return fmt.Errorf("create index %s: %w", idx.Name, err)
		}
		logger.InfoCtx(ctx, "Index ready", zap.String("index", idx.Name), zap.Duration("took", time.Since(start)))
	}
	for _, table := range []string{"eth_logs", "eth_blocks"} {
		if _, err := l.pool.Exec(ctx, "ANALYZE "+table); err != nil {
			return fmt.Errorf("analyze %s: %w", table, err)
		}
	}
	if err := logstore.NewFromPool(l.pool).SetCoverage(ctx, cov); err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Backfill finished; coverage published", zap.Uint64("start", cov.Start), zap.Uint64("head", cov.Head))
	return nil
}

// verifyLoaded checks the database and the local copy against the export
// manifest and returns the coverage to publish: eth_blocks must be exactly
// the manifest's contiguous interval, every partition the interval implies
// must hold the manifest's row count in the database and in the Parquet
// footers, and every file must match its recorded size and checksum.
func (l *Loader) verifyLoaded(ctx context.Context) (logstore.Coverage, error) {
	m, err := readManifest(l.dir)
	if err != nil {
		return logstore.Coverage{}, err
	}
	var lo, hi, n *int64
	if err := l.pool.QueryRow(ctx, `SELECT MIN(number), MAX(number), COUNT(*) FROM eth_blocks`).Scan(&lo, &hi, &n); err != nil {
		return logstore.Coverage{}, fmt.Errorf("read blocks: %w", err)
	}
	if lo == nil {
		return logstore.Coverage{}, errors.New("no blocks loaded; run the blocks stage first")
	}
	if uint64(*lo) != m.Blocks.First || uint64(*hi) != m.Blocks.Last || *n != m.Blocks.Rows { //nolint:gosec // non-negative
		return logstore.Coverage{}, fmt.Errorf("eth_blocks holds %d rows for blocks %d-%d, %s says %d rows for %d-%d",
			*n, *lo, *hi, ManifestName, m.Blocks.Rows, m.Blocks.First, m.Blocks.Last)
	}
	for part := m.Blocks.First / logstore.PartitionBlocks; part <= m.Blocks.Last/logstore.PartitionBlocks; part++ {
		if err := l.verifyPart(ctx, m, part); err != nil {
			return logstore.Coverage{}, err
		}
	}
	if err := m.verifyFiles(l.dir); err != nil {
		return logstore.Coverage{}, err
	}
	return logstore.Coverage{Start: m.Blocks.First, Head: m.Blocks.Last}, nil
}

// verifyPart checks one partition against the manifest: the directory must
// exist, its Parquet footers and the database must both hold the manifest's
// row count, and no log may sit above the newest block.
func (l *Loader) verifyPart(ctx context.Context, m *Manifest, part uint64) error {
	want, err := m.partRows(part)
	if err != nil {
		return err
	}
	dir := filepath.Join(l.dir, filepath.FromSlash(manifestPartDir(part)))
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("export is missing %s for blocks %d-%d; copy the full export",
			manifestPartDir(part), part*logstore.PartitionBlocks, part*logstore.PartitionBlocks+logstore.PartitionBlocks-1)
	}
	files, err := parquetFiles(dir)
	if err != nil {
		return err
	}
	var footer int64
	for _, file := range files {
		n, err := parquetRowCount(file)
		if err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		footer += n
	}
	if footer != want {
		return fmt.Errorf("part %03d files hold %d rows, %s says %d; the copy is incomplete", part, footer, ManifestName, want)
	}
	lo := part * logstore.PartitionBlocks
	var got int64
	var maxBlock *int64
	if err := l.pool.QueryRow(ctx, `SELECT COUNT(*), MAX(block_number) FROM eth_logs WHERE block_number BETWEEN $1 AND $2`,
		int64(lo), int64(lo+logstore.PartitionBlocks-1)).Scan(&got, &maxBlock); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("count partition %d: %w", part, err)
	}
	if got != want {
		return fmt.Errorf("part %03d has %d rows in the database, %s says %d; run the logs stage", part, got, ManifestName, want)
	}
	if maxBlock != nil && uint64(*maxBlock) > m.Blocks.Last { //nolint:gosec // non-negative
		return fmt.Errorf("part %03d has logs at block %d above the manifest's newest block %d", part, *maxBlock, m.Blocks.Last)
	}
	return nil
}

// parquetRowCount reads a file's row count from its footer.
func parquetRowCount(path string) (int64, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied export directory
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, fmt.Errorf("open parquet: %w", err)
	}
	return pf.NumRows(), nil
}

func isDuplicateRelation(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

// Logs loads every logs/part=NNN directory in order.
func (l *Loader) Logs(ctx context.Context) error {
	parts, err := filepath.Glob(filepath.Join(l.dir, "logs", "part=*"))
	if err != nil {
		return err
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return fmt.Errorf("no logs/part=* directories under %s", l.dir)
	}
	for _, dir := range parts {
		if err := l.loadPart(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}

// loadPart loads one part directory into its partition.
func (l *Loader) loadPart(ctx context.Context, dir string) error {
	part, err := strconv.ParseUint(strings.TrimPrefix(filepath.Base(dir), "part="), 10, 64)
	if err != nil {
		return fmt.Errorf("part directory %s: %w", dir, err)
	}
	lo := part * logstore.PartitionBlocks
	hi := lo + logstore.PartitionBlocks - 1
	files, err := parquetFiles(dir)
	if err != nil {
		return err
	}
	var exists bool
	if err := l.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM eth_logs WHERE block_number BETWEEN $1 AND $2)`,
		int64(lo), int64(hi)).Scan(&exists); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("check partition %d: %w", part, err)
	}
	if exists {
		logger.InfoCtx(ctx, "Partition already loaded, skipping", zap.Uint64("part", part))
		return nil
	}
	start := time.Now()
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, logstore.PartitionDDL(lo)); err != nil {
		return fmt.Errorf("ensure partition for part %d: %w", part, err)
	}
	// One transaction per partition: staging is unlogged and temporary to the
	// transaction, and a failure leaves the partition empty for a clean retry.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE staging_logs (LIKE eth_logs) ON COMMIT DROP`); err != nil {
		return fmt.Errorf("create staging: %w", err)
	}
	var rows int64
	for _, file := range files {
		n, err := copyParquet(ctx, tx, file, "staging_logs", logColumns, logRow)
		if err != nil {
			return fmt.Errorf("load %s: %w", file, err)
		}
		rows += n
	}
	if _, err := tx.Exec(ctx, `INSERT INTO eth_logs SELECT * FROM staging_logs ORDER BY block_number, log_index`); err != nil {
		return fmt.Errorf("insert part %d: %w", part, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Partition loaded", zap.Uint64("part", part), zap.Int("files", len(files)),
		zap.Int64("rows", rows), zap.Duration("took", time.Since(start)))
	return nil
}

// Blocks loads blocks/blocks-*.parquet into eth_blocks.
func (l *Loader) Blocks(ctx context.Context) error {
	files, err := parquetFiles(filepath.Join(l.dir, "blocks"))
	if err != nil {
		return err
	}
	var exists bool
	if err := l.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM eth_blocks)`).Scan(&exists); err != nil {
		return fmt.Errorf("check blocks: %w", err)
	}
	if exists {
		logger.InfoCtx(ctx, "Blocks already loaded, skipping")
		return nil
	}
	start := time.Now()
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var rows int64
	for i, file := range files {
		n, err := copyParquet(ctx, tx, file, "eth_blocks", blockColumns, blockRow)
		if err != nil {
			return fmt.Errorf("load %s: %w", file, err)
		}
		rows += n
		if (i+1)%500 == 0 {
			logger.InfoCtx(ctx, "Blocks load progress", zap.Int("files", i+1), zap.Int("of", len(files)), zap.Int64("rows", rows))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Blocks loaded", zap.Int("files", len(files)), zap.Int64("rows", rows), zap.Duration("took", time.Since(start)))
	return nil
}

func parquetFiles(dir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .parquet files in %s", dir)
	}
	sort.Strings(files)
	return files, nil
}

var (
	logColumns   = []string{"block_number", "log_index", "tx_index", "tx_hash", "address", "topic0", "topic1", "topic2", "topic3", "data"}
	blockColumns = []string{"number", "hash", "ts"}
)

// rowMapper turns one Parquet row into a COPY row, given the column index
// of every named Parquet column.
type rowMapper func(cols map[string]int, row parquet.Row) []any

// logRow maps the export's logs schema. Absent topics arrive as NULL values
// and are passed through as nil; block_timestamp is ignored (eth_blocks
// carries it).
func logRow(c map[string]int, r parquet.Row) []any {
	return []any{
		r[c["block_number"]].Int64(), int32(r[c["log_index"]].Int64()), int32(r[c["tx_index"]].Int64()), //nolint:gosec // fit int32
		bytesOrNil(r[c["tx_hash"]]), bytesOrNil(r[c["address"]]),
		bytesOrNil(r[c["topic0"]]), bytesOrNil(r[c["topic1"]]), bytesOrNil(r[c["topic2"]]), bytesOrNil(r[c["topic3"]]),
		bytesOrEmpty(r[c["data"]]),
	}
}

func blockRow(c map[string]int, r parquet.Row) []any {
	return []any{r[c["number"]].Int64(), bytesOrNil(r[c["block_hash"]]), r[c["ts"]].Int64()}
}

func bytesOrNil(v parquet.Value) any {
	if v.IsNull() {
		return nil
	}
	return v.ByteArray()
}

func bytesOrEmpty(v parquet.Value) []byte {
	if v.IsNull() {
		return []byte{}
	}
	return v.ByteArray()
}

// copyParquet streams one Parquet file into table via COPY.
func copyParquet(ctx context.Context, tx pgx.Tx, path, table string, columns []string, mapRow rowMapper) (int64, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied export directory
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, fmt.Errorf("open parquet: %w", err)
	}
	cols := map[string]int{}
	for _, col := range pf.Schema().Columns() {
		leaf, _ := pf.Schema().Lookup(col...)
		cols[strings.Join(col, ".")] = leaf.ColumnIndex
	}
	var total int64
	for _, rg := range pf.RowGroups() {
		src := &parquetSource{ctx: ctx, rows: rg.Rows(), cols: cols, mapRow: mapRow, buf: make([]parquet.Row, 1024)}
		n, err := tx.CopyFrom(ctx, pgx.Identifier{table}, columns, src)
		_ = src.rows.Close()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// parquetSource adapts a parquet.Rows iterator to pgx.CopyFromSource.
type parquetSource struct {
	ctx    context.Context
	rows   parquet.Rows
	cols   map[string]int
	mapRow rowMapper
	buf    []parquet.Row
	n, i   int
	err    error
}

func (s *parquetSource) Next() bool {
	if s.err != nil {
		return false
	}
	s.i++
	if s.i < s.n {
		return true
	}
	if s.ctx.Err() != nil {
		s.err = s.ctx.Err()
		return false
	}
	n, err := s.rows.ReadRows(s.buf)
	if n == 0 {
		if err != nil && !errors.Is(err, io.EOF) {
			s.err = err
		}
		return false
	}
	s.n, s.i = n, 0
	return true
}

func (s *parquetSource) Values() ([]any, error) { return s.mapRow(s.cols, s.buf[s.i]), nil }

func (s *parquetSource) Err() error { return s.err }
