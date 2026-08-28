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

// Cursor returns the last written block, or ok=false before any write.
func (s *Store) Cursor(ctx context.Context) (uint64, bool, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT block_number FROM ingest_cursor WHERE id = 1`).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read cursor: %w", err)
	}
	return uint64(n), true, nil //nolint:gosec // block numbers are non-negative
}

// Head is the warehouse head: the cursor, or ok=false when empty. It is what
// eth_blockNumber reports and the bound every served range is checked against.
func (s *Store) Head(ctx context.Context) (uint64, bool, error) { return s.Cursor(ctx) }

// WriteRange stores blocks and logs for [from, to] and moves the cursor to
// `to` in one transaction.
//
// Reason: the cursor must never be ahead of the data. A crash between a log
// write and the cursor update would otherwise serve a gap as "empty block"
// forever. Replays are idempotent: the range is deleted first, so a batch
// re-fetched after a restart overwrites rather than duplicates.
// Constraints: blocks must cover every height in [from, to] — the reader
// joins eth_blocks for blockHash/blockTimestamp and a missing row would drop
// that block's logs from every response.
func (s *Store) WriteRange(ctx context.Context, from, to uint64, blocks []Block, logs []types.Log) error {
	if uint64(len(blocks)) != to-from+1 {
		return fmt.Errorf("write range %d-%d: got %d block rows, want %d", from, to, len(blocks), to-from+1)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

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
	if err := setCursor(ctx, tx, to); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Rewind deletes every block above `to` and moves the cursor to `to`, so the
// next start re-fetches from to+1. It is the operator response to a reorg
// deeper than the confirmation lag (docs/operations.md) and the way to
// re-ingest a range that is suspected stale. A cursor below `to` is left
// alone: rewinding cannot move forward.
func (s *Store) Rewind(ctx context.Context, to uint64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM eth_logs WHERE block_number > $1`, int64(to)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("delete logs above %d: %w", to, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM eth_blocks WHERE number > $1`, int64(to)); err != nil { //nolint:gosec // fits int64
		return fmt.Errorf("delete blocks above %d: %w", to, err)
	}
	tag, err := tx.Exec(ctx, `UPDATE ingest_cursor SET block_number = $1, updated_at = now() WHERE id = 1 AND block_number > $1`, int64(to)) //nolint:gosec // fits int64
	if err != nil {
		return fmt.Errorf("rewind cursor to %d: %w", to, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("rewind to %d: cursor is not above it (nothing to rewind)", to)
	}
	return tx.Commit(ctx)
}

// setCursor upserts the single cursor row inside tx.
func setCursor(ctx context.Context, tx pgx.Tx, to uint64) error {
	_, err := tx.Exec(ctx, `INSERT INTO ingest_cursor (id, block_number, updated_at) VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET block_number = EXCLUDED.block_number, updated_at = now()`, int64(to)) //nolint:gosec // fits int64
	if err != nil {
		return fmt.Errorf("set cursor to %d: %w", to, err)
	}
	return nil
}

// SetCursor upserts the cursor outside a write; the backfill uses it once
// the export is fully loaded.
func (s *Store) SetCursor(ctx context.Context, to uint64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setCursor(ctx, tx, to); err != nil {
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
