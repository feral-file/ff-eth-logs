// Package backfill loads the one-off BigQuery export (Parquet, one directory
// per 1 M blocks — see docs/operations.md) into the warehouse.
//
// Stages, each idempotent and resumable:
//
//	prepare  drop the secondary indexes (bulk-loading 400 M rows through four
//	         random-order btrees would take days; sorted PK inserts are fine)
//	logs     per partition in the manifest interval: verify the directory's
//	         files against manifest.json, COPY them into a temp staging table,
//	         then INSERT ... ORDER BY (block_number, log_index) into eth_logs
//	         so the heap is chain-ordered; a partition already holding exactly
//	         the manifest's rows is skipped, any other count is cleared and
//	         reloaded
//	blocks   verify blocks-*.parquet against the manifest, COPY the rows
//	         inside the manifest interval into eth_blocks (same skip/reload rule)
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

// Lock takes the warehouse writer lock for the duration of a stage. Every
// stage must run under it: serve's tail ingestion holds the same lock for
// its lifetime, so a backfill against a live service fails immediately
// with logstore.ErrWriterBusy instead of racing it.
func (l *Loader) Lock(ctx context.Context) (func(), error) {
	// Writer lock (excludes ingestion and rewind) and maintenance lock
	// (excludes API readers — an API-only replica takes no writer lock, and a
	// partition reloaded under a still-published coverage would serve a
	// hybrid history), both on ONE pool connection so the stages' own
	// queries still have a connection at the documented two-connection floor.
	return logstore.NewFromPool(l.pool).AcquireBackfillLocks(ctx)
}

// unitInterval is the block range of a partition that the manifest covers:
// the partition clipped to [Blocks.First, Blocks.Last]. Every count, delete
// and verification uses it, so rows the tail wrote above the manifest end
// (in the same physical partition) are neither counted nor deleted.
func unitInterval(m *Manifest, part uint64) (lo, hi uint64) {
	lo = max(part*logstore.PartitionBlocks, m.Blocks.First)
	hi = min(part*logstore.PartitionBlocks+logstore.PartitionBlocks-1, m.Blocks.Last)
	return lo, hi
}

// preflightCoverage refuses a backfill whose manifest interval could never
// be published: finish merges the verified interval into the existing
// coverage and requires the two to touch, so a warehouse that started as a
// tail-only deployment far above the export end must be recreated before a
// historical backfill — better to learn that before the load than after.
func (l *Loader) preflightCoverage(ctx context.Context) error {
	m, err := readManifest(l.dir)
	if err != nil {
		return err
	}
	cov, ok, err := logstore.NewFromPool(l.pool).Coverage(ctx)
	if err != nil {
		return err
	}
	if ok && (m.Blocks.First > cov.Head+1 || m.Blocks.Last+1 < cov.Start) {
		return fmt.Errorf("%w: the export covers blocks %d-%d but the warehouse already publishes %d-%d (a tail-only start); recreate the database (make clean) and run the backfill before starting ingestion",
			logstore.ErrCoverageGap, m.Blocks.First, m.Blocks.Last, cov.Start, cov.Head)
	}
	return nil
}

// Prepare drops the secondary indexes so the bulk load only maintains the
// primary key.
func (l *Loader) Prepare(ctx context.Context) error {
	if err := l.preflightCoverage(ctx); err != nil {
		return err
	}
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
	if err := l.preflightCoverage(ctx); err != nil {
		return err
	}
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
	// Scoped to the manifest interval: rows the tail wrote above it (or a
	// previous export left below it) are outside this verification.
	var lo, hi, n *int64
	if err := l.pool.QueryRow(ctx, `SELECT MIN(number), MAX(number), COUNT(*) FROM eth_blocks WHERE number BETWEEN $1 AND $2`,
		int64(m.Blocks.First), int64(m.Blocks.Last)).Scan(&lo, &hi, &n); err != nil { //nolint:gosec // fits int64
		return logstore.Coverage{}, fmt.Errorf("read blocks: %w", err)
	}
	if lo == nil {
		return logstore.Coverage{}, errors.New("no blocks loaded; run the blocks stage first")
	}
	if uint64(*lo) != m.Blocks.First || uint64(*hi) != m.Blocks.Last || *n != m.Blocks.Rows { //nolint:gosec // non-negative
		return logstore.Coverage{}, fmt.Errorf("eth_blocks holds %d rows for blocks %d-%d, %s says %d rows for %d-%d",
			*n, *lo, *hi, ManifestName, m.Blocks.Rows, m.Blocks.First, m.Blocks.Last)
	}
	if err := l.verifyUnitLoadedFrom(ctx, m, blocksUnit, "blocks/"); err != nil {
		return logstore.Coverage{}, err
	}
	for part := m.Blocks.First / logstore.PartitionBlocks; part <= m.Blocks.Last/logstore.PartitionBlocks; part++ {
		if err := l.verifyPart(ctx, m, part); err != nil {
			return logstore.Coverage{}, err
		}
		if err := l.verifyUnitLoadedFrom(ctx, m, manifestPartDir(part), manifestPartDir(part)+"/"); err != nil {
			return logstore.Coverage{}, err
		}
	}
	if err := m.verifyFiles(l.dir); err != nil {
		return logstore.Coverage{}, err
	}
	return logstore.Coverage{Start: m.Blocks.First, Head: m.Blocks.Last}, nil
}

// verifyUnitLoadedFrom refuses to publish a unit whose rows came from an
// export other than the one described by the current manifest: counts,
// ranges and current-file checksums cannot tell a same-count replacement
// apart, the recorded fingerprint can.
func (l *Loader) verifyUnitLoadedFrom(ctx context.Context, m *Manifest, unit, prefix string) error {
	recorded, err := l.loadedUnit(ctx, unit)
	if err != nil {
		return err
	}
	if recorded != m.unitFingerprint(prefix) {
		return fmt.Errorf("%s was loaded from a different export than %s describes (or never recorded); rerun its stage", unit, ManifestName)
	}
	return nil
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
	lo, hi := unitInterval(m, part)
	var got int64
	if err := l.pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs WHERE block_number BETWEEN $1 AND $2`,
		int64(lo), int64(hi)).Scan(&got); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("count partition %d: %w", part, err)
	}
	if got != want {
		return fmt.Errorf("part %03d has %d rows in the database, %s says %d; run the logs stage", part, got, ManifestName, want)
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

// loadedUnit returns the fingerprint recorded for a unit, or "" when the
// unit was never loaded (or was loaded before fingerprints existed).
func (l *Loader) loadedUnit(ctx context.Context, unit string) (string, error) {
	var fp string
	err := l.pool.QueryRow(ctx, `SELECT fingerprint FROM backfill_units WHERE unit = $1`, unit).Scan(&fp)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read backfill unit %s: %w", unit, err)
	}
	return fp, nil
}

// recordUnit stores the fingerprint a unit was loaded from, inside tx so it
// commits with the rows.
func recordUnit(ctx context.Context, tx pgx.Tx, unit, fingerprint string, rows int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO backfill_units (unit, fingerprint, rows_loaded, loaded_at) VALUES ($1, $2, $3, now())
		ON CONFLICT (unit) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, rows_loaded = EXCLUDED.rows_loaded, loaded_at = now()`, unit, fingerprint, rows)
	if err != nil {
		return fmt.Errorf("record backfill unit %s: %w", unit, err)
	}
	return nil
}

func isDuplicateRelation(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

// Logs loads every partition the manifest interval implies, in order.
func (l *Loader) Logs(ctx context.Context) error {
	if err := l.preflightCoverage(ctx); err != nil {
		return err
	}
	m, err := readManifest(l.dir)
	if err != nil {
		return err
	}
	for part := m.Blocks.First / logstore.PartitionBlocks; part <= m.Blocks.Last/logstore.PartitionBlocks; part++ {
		if err := l.loadPart(ctx, m, part); err != nil {
			return err
		}
	}
	return nil
}

// loadPart loads one partition. Its files are verified against the manifest
// before a row is read; a partition whose database rows already equal the
// manifest count is skipped, and one with any other non-zero count (a load
// interrupted, or rows from a file that was since replaced) is cleared and
// reloaded — "has rows" is never taken as "done".
func (l *Loader) loadPart(ctx context.Context, m *Manifest, part uint64) error {
	want, err := m.partRows(part)
	if err != nil {
		return err
	}
	if err := m.verifyFilesUnder(l.dir, manifestPartDir(part)+"/"); err != nil {
		return err
	}
	dir := filepath.Join(l.dir, filepath.FromSlash(manifestPartDir(part)))
	lo, hi := unitInterval(m, part)
	files, err := parquetFiles(dir)
	if err != nil {
		return err
	}
	var have int64
	if err := l.pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs WHERE block_number BETWEEN $1 AND $2`,
		int64(lo), int64(hi)).Scan(&have); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("count partition %d: %w", part, err)
	}
	unit := manifestPartDir(part)
	fingerprint := m.unitFingerprint(unit + "/")
	recorded, err := l.loadedUnit(ctx, unit)
	if err != nil {
		return err
	}
	if have == want && recorded == fingerprint {
		logger.InfoCtx(ctx, "Partition already loaded from this export, skipping", zap.Uint64("part", part), zap.Int64("rows", have))
		return nil
	}
	if have != 0 {
		logger.WarnCtx(ctx, "Partition was loaded from different content or holds a different row count; reloading it",
			zap.Uint64("part", part), zap.Int64("rows", have), zap.Int64("manifestRows", want), zap.Bool("sameExport", recorded == fingerprint))
	}
	start := time.Now()
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, logstore.PartitionDDL(part*logstore.PartitionBlocks)); err != nil {
		return fmt.Errorf("ensure partition for part %d: %w", part, err)
	}
	// One transaction per partition: staging is unlogged and temporary to the
	// transaction, and a failure leaves the partition empty for a clean retry.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, `DELETE FROM eth_logs WHERE block_number BETWEEN $1 AND $2`, int64(lo), int64(hi)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("clear partition %d: %w", part, err)
	}
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
	if rows != want {
		return fmt.Errorf("part %03d files hold %d rows, %s says %d; the copy is incomplete", part, rows, ManifestName, want)
	}
	var outside int64
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM staging_logs WHERE block_number < $1 OR block_number > $2`, int64(lo), int64(hi)).Scan(&outside); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("check staged rows of part %d: %w", part, err)
	}
	if outside != 0 {
		return fmt.Errorf("part %03d files hold %d rows outside blocks %d-%d", part, outside, lo, hi)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO eth_logs SELECT * FROM staging_logs ORDER BY block_number, log_index`); err != nil {
		return fmt.Errorf("insert part %d: %w", part, err)
	}
	if err := recordUnit(ctx, tx, unit, fingerprint, rows); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Partition loaded", zap.Uint64("part", part), zap.Int("files", len(files)),
		zap.Int64("rows", rows), zap.Duration("took", time.Since(start)))
	return nil
}

// Blocks loads blocks/blocks-*.parquet into eth_blocks, keeping only the
// manifest's interval.
//
// Reason: the blocks export is taken from the live blocks table, which can
// have advanced past the logs extract between the two steps of the export
// script. Loading those newer blocks would let a manifest that adopted them
// publish coverage for blocks whose logs were never extracted; trimming to
// the manifest interval here makes the manifest — not the longer blocks
// export — the authority on what is covered.
func (l *Loader) Blocks(ctx context.Context) error {
	if err := l.preflightCoverage(ctx); err != nil {
		return err
	}
	m, err := readManifest(l.dir)
	if err != nil {
		return err
	}
	if err := m.verifyFilesUnder(l.dir, "blocks/"); err != nil {
		return err
	}
	files, err := parquetFiles(filepath.Join(l.dir, "blocks"))
	if err != nil {
		return err
	}
	var have int64
	if err := l.pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_blocks WHERE number BETWEEN $1 AND $2`,
		int64(m.Blocks.First), int64(m.Blocks.Last)).Scan(&have); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("check blocks: %w", err)
	}
	fingerprint := m.unitFingerprint("blocks/")
	recorded, err := l.loadedUnit(ctx, blocksUnit)
	if err != nil {
		return err
	}
	if have == m.Blocks.Rows && recorded == fingerprint {
		logger.InfoCtx(ctx, "Blocks already loaded from this export, skipping", zap.Int64("rows", have))
		return nil
	}
	if have != 0 {
		logger.WarnCtx(ctx, "eth_blocks was loaded from different content or holds a different row count; reloading",
			zap.Int64("rows", have), zap.Int64("manifestRows", m.Blocks.Rows), zap.Bool("sameExport", recorded == fingerprint))
	}
	start := time.Now()
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.Exec(ctx, `DELETE FROM eth_blocks WHERE number BETWEEN $1 AND $2`, int64(m.Blocks.First), int64(m.Blocks.Last)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("clear blocks: %w", err)
	}
	var rows int64
	inInterval := func(_ map[string]int, r parquet.Row, c map[string]int) bool {
		n := r[c["number"]].Int64()
		return n >= 0 && uint64(n) >= m.Blocks.First && uint64(n) <= m.Blocks.Last //nolint:gosec // checked non-negative
	}
	for i, file := range files {
		n, err := copyParquetWhere(ctx, tx, file, "eth_blocks", blockColumns, blockRow, inInterval)
		if err != nil {
			return fmt.Errorf("load %s: %w", file, err)
		}
		rows += n
		if (i+1)%500 == 0 {
			logger.InfoCtx(ctx, "Blocks load progress", zap.Int("files", i+1), zap.Int("of", len(files)), zap.Int64("rows", rows))
		}
	}
	if err := recordUnit(ctx, tx, blocksUnit, fingerprint, rows); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Blocks loaded", zap.Int("files", len(files)), zap.Int64("rows", rows), zap.Duration("took", time.Since(start)))
	return nil
}

// rollback aborts tx with its own short context (see logstore.rollback for
// why the request context must not be used).
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
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

// blocksUnit is the backfill_units key of the blocks stage.
const blocksUnit = "blocks"

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

// rowFilter decides whether one Parquet row is copied.
type rowFilter func(cols map[string]int, row parquet.Row, c map[string]int) bool

// copyParquet streams one Parquet file into table via COPY.
func copyParquet(ctx context.Context, tx pgx.Tx, path, table string, columns []string, mapRow rowMapper) (int64, error) {
	return copyParquetWhere(ctx, tx, path, table, columns, mapRow, nil)
}

// copyParquetWhere is copyParquet with an optional row filter.
func copyParquetWhere(ctx context.Context, tx pgx.Tx, path, table string, columns []string, mapRow rowMapper, keep rowFilter) (int64, error) {
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
		// The export writes a zero-row file for an empty shard (one empty row
		// group); parquet-go's row reader seeks past the end of such a group
		// ("Seek: invalid offset"), so it is skipped rather than read.
		if rg.NumRows() == 0 {
			continue
		}
		src := &parquetSource{ctx: ctx, rows: rg.Rows(), cols: cols, mapRow: mapRow, keep: keep, buf: make([]parquet.Row, 1024)}
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
	keep   rowFilter
	buf    []parquet.Row
	n, i   int
	err    error
}

func (s *parquetSource) Next() bool {
	for {
		if !s.advance() {
			return false
		}
		if s.keep == nil || s.keep(s.cols, s.buf[s.i], s.cols) {
			return true
		}
	}
}

// advance moves to the next buffered row, refilling the buffer from the
// row group when it is exhausted.
func (s *parquetSource) advance() bool {
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
