package logstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"

	"github.com/feral-file/ff-eth-logs/internal/eventset"
)

// Query is a resolved eth_getLogs filter: an inclusive block range plus
// go-ethereum's address/topic matching rules.
type Query struct {
	FromBlock uint64
	ToBlock   uint64
	// Addresses are OR'd; empty = any.
	Addresses []common.Address
	// Topics[i] is OR'd within the position and AND'd across positions; an
	// empty position is a wildcard that imposes nothing — not even that the
	// topic exists. That is what the vendor does (Infura, Geth v1.17.5,
	// verified 2026-08-29: [[Transfer]], [[Transfer],null,null,null] and
	// [[Transfer],[],[],[]] all return the same 3-topic ERC-20 logs, in range
	// and blockHash queries alike), and go-ethereum's older filterLogs rule
	// "N positions need ≥ N topics" would drop stored 1-topic MetadataUpdate
	// logs from a mixed [[Transfer, MetadataUpdate], null, null, null] query
	// that the vendor answers. A position with values requires the topic to
	// exist and match, which a NULL column never does.
	Topics [][]common.Hash
	// ERC1155ID restricts TransferSingle logs (topic0 = eventset.TransferSingle)
	// to the one whose data word 0 equals it; logs of every other signature in
	// the filter are untouched, so a mixed [[TransferSingle, URI]] query keeps
	// its URI logs. nil imposes nothing. See rpcapi.FilterCriteria.ERC1155ID.
	ERC1155ID *common.Hash
}

// ErrTooManyResults is returned when a query would exceed the caller's limit.
var ErrTooManyResults = errors.New("too many results")

// View is one consistent read of the warehouse: coverage, block lookup and
// log selection all see the same snapshot.
//
// Reason: a deep-reorg recovery rewinds the store while the API is serving.
// Reading the coverage from one connection and the logs from another lets a
// rewind commit in between, so a range the old coverage authorized could
// come back empty or partial from the new data — a silent omission, the one
// failure the API exists to prevent. Store.Read runs the whole request in a
// REPEATABLE READ transaction, so it either sees the pre-rewind data
// entirely (coherent, and superseded a moment later like any read) or the
// post-rewind coverage that refuses the range.
type View interface {
	Coverage(ctx context.Context) (Coverage, bool, error)
	FilterLogs(ctx context.Context, q Query, limit int) ([]types.Log, error)
	BlockByHash(ctx context.Context, hash common.Hash) (Block, bool, error)
}

// Read runs fn against a single read-only REPEATABLE READ snapshot.
func (s *Store) Read(ctx context.Context, fn func(View) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin read snapshot: %w", err)
	}
	defer rollback(tx)
	// Shared, transaction-scoped: released with the snapshot. A backfill
	// holding the exclusive maintenance lock makes this fail, so the read is
	// refused rather than served from partitions mid-reload.
	var admitted bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock_shared($1)`, maintenanceLockKey).Scan(&admitted); err != nil {
		return fmt.Errorf("maintenance check: %w", err)
	}
	if !admitted {
		return ErrMaintenance
	}
	// The durable flag outlives a backfill that died mid-reload.
	var flagged bool
	if err := tx.QueryRow(ctx, `SELECT maintenance FROM warehouse_state WHERE id = 1`).Scan(&flagged); err != nil {
		return fmt.Errorf("maintenance flag: %w", err)
	}
	if flagged {
		return ErrMaintenance
	}
	if err := fn(snapshot{q: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// rowQuerier is the subset of pgx.Tx / pgxpool.Pool the reads need.
type rowQuerier interface {
	querier
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// snapshot is a View bound to one transaction.
type snapshot struct{ q rowQuerier }

func (v snapshot) Coverage(ctx context.Context) (Coverage, bool, error) {
	return readCoverage(ctx, v.q, false)
}

func (v snapshot) FilterLogs(ctx context.Context, q Query, limit int) ([]types.Log, error) {
	return filterLogs(ctx, v.q, q, limit)
}

func (v snapshot) BlockByHash(ctx context.Context, hash common.Hash) (Block, bool, error) {
	return blockByHash(ctx, v.q, hash)
}

// FilterLogs is the pool-backed (non-snapshot) read; the API goes through
// Read instead so its coverage check and selection share one snapshot.
func (s *Store) FilterLogs(ctx context.Context, q Query, limit int) ([]types.Log, error) {
	return filterLogs(ctx, s.pool, q, limit)
}

// BlockByHash resolves a blockHash filter to its stored block.
func (s *Store) BlockByHash(ctx context.Context, hash common.Hash) (Block, bool, error) {
	return blockByHash(ctx, s.pool, hash)
}

// selectLogs is the column list every log read uses; scanLog must match it.
const selectLogs = `SELECT l.block_number, l.log_index, l.tx_index, l.tx_hash, l.address,
	l.topic0, l.topic1, l.topic2, l.topic3, l.data, b.hash, b.ts
	FROM eth_logs l JOIN eth_blocks b ON b.number = l.block_number`

// filterLogs returns the logs matching q in chain order (block, log index).
// limit > 0 caps the result: exceeding it returns ErrTooManyResults rather
// than a truncated slice, because a silently partial eth_getLogs answer is
// the one thing a client cannot detect. removed is always false: only
// confirmed blocks are stored.
func filterLogs(ctx context.Context, db rowQuerier, q Query, limit int) ([]types.Log, error) {
	sql, args := buildFilter(q, limit)
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	logs := []types.Log{}
	for rows.Next() {
		l, err := scanLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, l)
		if limit > 0 && len(logs) > limit {
			return nil, fmt.Errorf("%w: more than %d", ErrTooManyResults, limit)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read logs: %w", err)
	}
	return logs, nil
}

// buildFilter renders q as SQL with positional parameters. The WHERE clause
// is assembled from fixed fragments; user data only ever travels as
// parameters, never in the SQL text.
func buildFilter(q Query, limit int) (string, []any) {
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	where = append(where, fmt.Sprintf("l.block_number BETWEEN %s AND %s", arg(int64(q.FromBlock)), arg(int64(q.ToBlock)))) //nolint:gosec // fit int64
	if len(q.Addresses) > 0 {
		addrs := make([][]byte, len(q.Addresses))
		for i, a := range q.Addresses {
			addrs[i] = a.Bytes()
		}
		where = append(where, fmt.Sprintf("l.address = ANY(%s::bytea[])", arg(addrs)))
	}
	for i, sub := range q.Topics {
		col := fmt.Sprintf("l.topic%d", i)
		if len(sub) == 0 {
			continue // wildcard: no clause, the topic need not exist
		}
		vals := make([][]byte, len(sub))
		for j, h := range sub {
			vals[j] = h.Bytes()
		}
		where = append(where, fmt.Sprintf("%s = ANY(%s::bytea[])", col, arg(vals)))
	}
	if q.ERC1155ID != nil {
		// Restrict only TransferSingle rows to the id (data word 0); any other
		// signature in the filter is left in. The partial expression index
		// eth_logs_erc1155_id serves the TransferSingle branch as a point lookup.
		where = append(where, fmt.Sprintf("(l.topic0 <> %s OR substring(l.data from 1 for 32) = %s)",
			arg(eventset.TransferSingle.Bytes()), arg(q.ERC1155ID.Bytes())))
	}
	sql := selectLogs + " WHERE " + strings.Join(where, " AND ") + " ORDER BY l.block_number, l.log_index"
	if limit > 0 {
		// One past the limit so the caller can tell "exactly limit" from "more".
		sql += fmt.Sprintf(" LIMIT %d", limit+1)
	}
	return sql, args
}

// scanLog maps one selectLogs row to a types.Log.
func scanLog(rows pgx.Rows) (types.Log, error) {
	var (
		blockNumber, ts    int64
		logIndex, txIndex  int32
		txHash, addr, data []byte
		blockHash          []byte
		topics             [4][]byte
	)
	if err := rows.Scan(&blockNumber, &logIndex, &txIndex, &txHash, &addr,
		&topics[0], &topics[1], &topics[2], &topics[3], &data, &blockHash, &ts); err != nil {
		return types.Log{}, fmt.Errorf("scan log: %w", err)
	}
	l := types.Log{
		Address:        common.BytesToAddress(addr),
		Data:           data,
		BlockNumber:    uint64(blockNumber), //nolint:gosec // non-negative
		TxHash:         common.BytesToHash(txHash),
		TxIndex:        uint(txIndex), //nolint:gosec // non-negative
		BlockHash:      common.BytesToHash(blockHash),
		BlockTimestamp: uint64(ts),     //nolint:gosec // non-negative
		Index:          uint(logIndex), //nolint:gosec // non-negative
	}
	for _, t := range topics {
		if t == nil {
			break
		}
		l.Topics = append(l.Topics, common.BytesToHash(t))
	}
	if l.Data == nil {
		l.Data = []byte{}
	}
	return l, nil
}

// StoredBlockHash returns the hash the warehouse holds for block n, or
// ok=false when n is not stored. Ingestion compares it with the canonical
// header to locate the fork point of a deep reorg.
func (s *Store) StoredBlockHash(ctx context.Context, n uint64) (common.Hash, bool, error) {
	var h []byte
	err := s.pool.QueryRow(ctx, `SELECT hash FROM eth_blocks WHERE number = $1`, int64(n)).Scan(&h) //nolint:gosec // fits int64
	if errors.Is(err, pgx.ErrNoRows) {
		return common.Hash{}, false, nil
	}
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("stored block hash %d: %w", n, err)
	}
	return common.BytesToHash(h), true, nil
}

func blockByHash(ctx context.Context, db querier, hash common.Hash) (Block, bool, error) {
	var (
		n, ts int64
		h     []byte
	)
	err := db.QueryRow(ctx, `SELECT number, hash, ts FROM eth_blocks WHERE hash = $1`, hash.Bytes()).Scan(&n, &h, &ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Block{}, false, nil
	}
	if err != nil {
		return Block{}, false, fmt.Errorf("block by hash: %w", err)
	}
	return Block{Number: uint64(n), Hash: common.BytesToHash(h), Timestamp: uint64(ts)}, true, nil //nolint:gosec // non-negative
}
