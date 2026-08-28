package logstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Block is one eth_blocks row: the metadata eth_getLogs needs per log
// (blockHash, blockTimestamp) stored once per block instead of per row.
type Block struct {
	Number    uint64
	Hash      common.Hash
	Timestamp uint64
}

// Coverage is the contiguous block interval the warehouse holds completely:
// every block in [Start, Head] has its eth_blocks row and every warehouse
// log. It is the only range the API may answer from.
//
// Reason: the head alone is not enough — a fresh database that starts tail
// ingestion at the chain tip has a head but no history, and a client asking
// for genesis..head would get a confident, empty, wrong answer. Persisting
// the lower bound makes "not loaded" distinguishable from "no logs".
type Coverage struct {
	Start uint64
	Head  uint64
}

// ErrCoverageGap is returned when a write would leave a hole in the covered
// interval, or a rewind would empty it.
var ErrCoverageGap = errors.New("write is not contiguous with warehouse coverage")

// Store is a pgx pool over the warehouse database.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to dsn and verifies the connection.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open warehouse database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping warehouse database: %w", err)
	}
	return &Store{pool: pool}, nil
}

// NewFromPool wraps an existing pool (tests).
func NewFromPool(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the underlying pool for the backfill loader.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// querier is the subset of pgx.Tx / pgxpool.Pool the cursor reads use.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// readCoverage reads the single cursor row; ok=false before any write.
func readCoverage(ctx context.Context, q querier, lock bool) (Coverage, bool, error) {
	sql := `SELECT coverage_start, block_number FROM ingest_cursor WHERE id = 1`
	if lock {
		sql += ` FOR UPDATE`
	}
	var start, head int64
	err := q.QueryRow(ctx, sql).Scan(&start, &head)
	if errors.Is(err, pgx.ErrNoRows) {
		return Coverage{}, false, nil
	}
	if err != nil {
		return Coverage{}, false, fmt.Errorf("read cursor: %w", err)
	}
	return Coverage{Start: uint64(start), Head: uint64(head)}, true, nil //nolint:gosec // non-negative
}

// Coverage returns the covered interval, or ok=false when the warehouse is empty.
func (s *Store) Coverage(ctx context.Context) (Coverage, bool, error) {
	return readCoverage(ctx, s.pool, false)
}

// Cursor returns the last written block (the coverage head), or ok=false
// before any write. Tail ingestion resumes at Cursor+1.
func (s *Store) Cursor(ctx context.Context) (uint64, bool, error) {
	cov, ok, err := readCoverage(ctx, s.pool, false)
	return cov.Head, ok, err
}

// WriteRange stores blocks and logs for [from, to] and extends coverage to
// include them, in one transaction.
//
// Reason: the cursor must never be ahead of the data. A crash between a log
// write and the cursor update would otherwise serve a gap as "empty block"
// forever. Replays are idempotent: the range is deleted first, so a batch
// re-fetched after a restart overwrites rather than duplicates.
// Constraints: the range must touch the existing coverage (from ≤ head+1 and
// to+1 ≥ start), otherwise ErrCoverageGap — a start_block that jumps ahead
// of the cursor would leave a hole the API could not detect. Blocks must
// cover every height in [from, to] — the reader joins eth_blocks for
// blockHash/blockTimestamp and a missing row would drop that block's logs.
func (s *Store) WriteRange(ctx context.Context, from, to uint64, blocks []Block, logs []types.Log) error {
	if uint64(len(blocks)) != to-from+1 {
		return fmt.Errorf("write range %d-%d: got %d block rows, want %d", from, to, len(blocks), to-from+1)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cov, ok, err := readCoverage(ctx, tx, true)
	if err != nil {
		return err
	}
	if ok && (from > cov.Head+1 || to+1 < cov.Start) {
		return fmt.Errorf("%w: range %d-%d vs coverage %d-%d (rewind, or unset ethereum.start_block)",
			ErrCoverageGap, from, to, cov.Start, cov.Head)
	}
	if !ok {
		cov = Coverage{Start: from, Head: to}
	}
	cov.Start, cov.Head = min(cov.Start, from), max(cov.Head, to)

	if err := ensurePartitions(ctx, tx, from, to); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM eth_logs WHERE block_number BETWEEN $1 AND $2`, int64(from), int64(to)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("clear logs %d-%d: %w", from, to, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM eth_blocks WHERE number BETWEEN $1 AND $2`, int64(from), int64(to)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("clear blocks %d-%d: %w", from, to, err)
	}
	if err := copyBlocks(ctx, tx, blocks); err != nil {
		return err
	}
	if err := copyLogs(ctx, tx, logs); err != nil {
		return err
	}
	if err := setCoverage(ctx, tx, cov); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ensurePartitions creates any eth_logs partition [from, to] touches that the
// init script did not pre-create (it stops at block 40 M). A batch spans at
// most two partitions, and the existence probe is a catalog lookup, so this
// costs nothing in steady state and only takes the parent lock on rollover.
func ensurePartitions(ctx context.Context, tx pgx.Tx, from, to uint64) error {
	for lo := from / PartitionBlocks * PartitionBlocks; lo <= to; lo += PartitionBlocks {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, PartitionName(lo)).Scan(&exists); err != nil {
			return fmt.Errorf("probe partition %s: %w", PartitionName(lo), err)
		}
		if exists {
			continue
		}
		if _, err := tx.Exec(ctx, PartitionDDL(lo)); err != nil {
			return fmt.Errorf("create partition %s: %w", PartitionName(lo), err)
		}
	}
	return nil
}

// Rewind deletes every block above `to` and moves the head to `to`, so the
// next start re-fetches from to+1. It is the operator response to a reorg
// deeper than the confirmation lag (docs/operations.md) and the way to
// re-ingest a range that is suspected stale. It refuses to move forward, and
// refuses to go below the coverage start (that would empty the warehouse —
// drop and re-backfill instead).
func (s *Store) Rewind(ctx context.Context, to uint64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	cov, ok, err := readCoverage(ctx, tx, true)
	if err != nil {
		return err
	}
	if !ok || cov.Head <= to {
		return fmt.Errorf("rewind to %d: cursor is not above it (nothing to rewind)", to)
	}
	if to < cov.Start {
		return fmt.Errorf("%w: rewind to %d is below the coverage start %d", ErrCoverageGap, to, cov.Start)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM eth_logs WHERE block_number > $1`, int64(to)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("delete logs above %d: %w", to, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM eth_blocks WHERE number > $1`, int64(to)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("delete blocks above %d: %w", to, err)
	}
	if err := setCoverage(ctx, tx, Coverage{Start: cov.Start, Head: to}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// setCoverage upserts the single cursor row inside tx.
func setCoverage(ctx context.Context, tx pgx.Tx, cov Coverage) error {
	_, err := tx.Exec(ctx, `INSERT INTO ingest_cursor (id, coverage_start, block_number, updated_at) VALUES (1, $1, $2, now())
		ON CONFLICT (id) DO UPDATE SET coverage_start = EXCLUDED.coverage_start, block_number = EXCLUDED.block_number, updated_at = now()`,
		int64(cov.Start), int64(cov.Head)) //nolint:gosec // fit int64
	if err != nil {
		return fmt.Errorf("set coverage %d-%d: %w", cov.Start, cov.Head, err)
	}
	return nil
}

// SetCoverage publishes a coverage interval outside a write. Only the
// backfill uses it, after it has verified that every block and every export
// partition in the interval is loaded.
func (s *Store) SetCoverage(ctx context.Context, cov Coverage) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setCoverage(ctx, tx, cov); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// blockColumns / logColumns are the COPY column lists, shared with the backfill.
var (
	blockColumns = []string{"number", "hash", "ts"}
	logColumns   = []string{"block_number", "log_index", "tx_index", "tx_hash", "address", "topic0", "topic1", "topic2", "topic3", "data"}
)

func copyBlocks(ctx context.Context, tx pgx.Tx, blocks []Block) error {
	rows := make([][]any, len(blocks))
	for i, b := range blocks {
		rows[i] = BlockRow(b)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"eth_blocks"}, blockColumns, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy %d blocks: %w", len(blocks), err)
	}
	return nil
}

func copyLogs(ctx context.Context, tx pgx.Tx, logs []types.Log) error {
	if len(logs) == 0 {
		return nil
	}
	rows := make([][]any, len(logs))
	for i := range logs {
		rows[i] = LogRow(&logs[i])
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"eth_logs"}, logColumns, pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("copy %d logs: %w", len(logs), err)
	}
	return nil
}

// BlockRow is the eth_blocks COPY row for b.
func BlockRow(b Block) []any {
	return []any{int64(b.Number), b.Hash.Bytes(), int64(b.Timestamp)} //nolint:gosec // fits int64
}

// LogRow is the eth_logs COPY row for l: absent topics are NULL, data is the
// raw bytes (empty, not NULL, for ERC-721 Transfer).
func LogRow(l *types.Log) []any {
	topics := make([]any, 4)
	for i := range topics {
		if i < len(l.Topics) {
			topics[i] = l.Topics[i].Bytes()
		}
	}
	data := l.Data
	if data == nil {
		data = []byte{}
	}
	return []any{int64(l.BlockNumber), int32(l.Index), int32(l.TxIndex), l.TxHash.Bytes(), l.Address.Bytes(), //nolint:gosec // fit
		topics[0], topics[1], topics[2], topics[3], data}
}
