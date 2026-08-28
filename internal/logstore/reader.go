package logstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
)

// Query is a resolved eth_getLogs filter: an inclusive block range plus
// go-ethereum's address/topic matching rules.
type Query struct {
	FromBlock uint64
	ToBlock   uint64
	// Addresses are OR'd; empty = any.
	Addresses []common.Address
	// Topics[i] is OR'd within the position and AND'd across positions; an
	// empty position is a wildcard. A query with N positions only matches
	// logs with at least N topics — the rule that makes [[Transfer],[],[],[]]
	// exclude ERC-20 on a real node, reproduced here as IS NOT NULL.
	Topics [][]common.Hash
}

// ErrTooManyResults is returned when a query would exceed the caller's limit.
var ErrTooManyResults = errors.New("too many results")

// selectLogs is the column list every log read uses; scanLog must match it.
const selectLogs = `SELECT l.block_number, l.log_index, l.tx_index, l.tx_hash, l.address,
	l.topic0, l.topic1, l.topic2, l.topic3, l.data, b.hash, b.ts
	FROM eth_logs l JOIN eth_blocks b ON b.number = l.block_number`

// FilterLogs returns the logs matching q in chain order (block, log index).
// limit > 0 caps the result: exceeding it returns ErrTooManyResults rather
// than a truncated slice, because a silently partial eth_getLogs answer is
// the one thing a client cannot detect. removed is always false: only
// confirmed blocks are stored.
func (s *Store) FilterLogs(ctx context.Context, q Query, limit int) ([]types.Log, error) {
	sql, args := buildFilter(q, limit)
	rows, err := s.pool.Query(ctx, sql, args...)
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
			where = append(where, col+" IS NOT NULL")
			continue
		}
		vals := make([][]byte, len(sub))
		for j, h := range sub {
			vals[j] = h.Bytes()
		}
		where = append(where, fmt.Sprintf("%s = ANY(%s::bytea[])", col, arg(vals)))
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

// BlockByHash resolves a blockHash filter to its stored block.
func (s *Store) BlockByHash(ctx context.Context, hash common.Hash) (Block, bool, error) {
	var (
		n, ts int64
		h     []byte
	)
	err := s.pool.QueryRow(ctx, `SELECT number, hash, ts FROM eth_blocks WHERE hash = $1`, hash.Bytes()).Scan(&n, &h, &ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Block{}, false, nil
	}
	if err != nil {
		return Block{}, false, fmt.Errorf("block by hash: %w", err)
	}
	return Block{Number: uint64(n), Hash: common.BytesToHash(h), Timestamp: uint64(ts)}, true, nil //nolint:gosec // non-negative
}
