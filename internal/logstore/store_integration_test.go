//go:build integration

package logstore

import (
	"context"
	"math/big"
	"math/rand"
	"slices"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-eth-logs/internal/testdb"
)

func hashOf(n uint64) common.Hash { return common.BigToHash(new(big.Int).SetUint64(n)) }

func blockAt(n uint64) Block {
	return Block{Number: n, Hash: hashOf(n), Timestamp: 1_700_000_000 + n*12}
}

func TestWriteRangeCursorAndRewind(t *testing.T) {
	ctx := context.Background()
	s := NewFromPool(testdb.Open(t))

	_, ok, err := s.Cursor(ctx)
	require.NoError(t, err)
	assert.False(t, ok)

	logs := []types.Log{
		{BlockNumber: 11, Index: 0, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0xa")}, Data: nil, TxHash: common.HexToHash("0xf1")},
		{BlockNumber: 12, Index: 3, Address: common.HexToAddress("0x2"), Topics: []common.Hash{common.HexToHash("0xa"), common.HexToHash("0xb")}, Data: []byte{1, 2}, TxHash: common.HexToHash("0xf2"), TxIndex: 7},
	}
	require.NoError(t, s.WriteRange(ctx, 10, 12, []Block{blockAt(10), blockAt(11), blockAt(12)}, logs))
	head, ok, err := s.Head(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(12), head)

	got, err := s.FilterLogs(ctx, Query{FromBlock: 0, ToBlock: 100}, 0)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []byte{}, got[0].Data, "nil data is stored and served as empty bytes")
	assert.Equal(t, blockAt(12).Hash, got[1].BlockHash)
	assert.Equal(t, blockAt(12).Timestamp, got[1].BlockTimestamp)
	assert.Equal(t, uint(7), got[1].TxIndex)
	assert.False(t, got[1].Removed)

	// Replaying a range overwrites instead of duplicating.
	require.NoError(t, s.WriteRange(ctx, 11, 12, []Block{blockAt(11), blockAt(12)}, logs[:1]))
	got, err = s.FilterLogs(ctx, Query{FromBlock: 0, ToBlock: 100}, 0)
	require.NoError(t, err)
	assert.Len(t, got, 1)

	// Block coverage is mandatory.
	assert.Error(t, s.WriteRange(ctx, 13, 14, []Block{blockAt(13)}, nil))

	// Rewind drops everything above and moves the cursor.
	require.NoError(t, s.Rewind(ctx, 10))
	head, _, err = s.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), head)
	got, err = s.FilterLogs(ctx, Query{FromBlock: 0, ToBlock: 100}, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
	_, found, err := s.BlockByHash(ctx, blockAt(11).Hash)
	require.NoError(t, err)
	assert.False(t, found)
	b, found, err := s.BlockByHash(ctx, blockAt(10).Hash)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, blockAt(10), b)
	assert.Error(t, s.Rewind(ctx, 10), "cannot rewind forward or to the cursor itself")

	// The limit is a hard error, never a truncated answer.
	require.NoError(t, s.WriteRange(ctx, 11, 12, []Block{blockAt(11), blockAt(12)}, logs))
	_, err = s.FilterLogs(ctx, Query{FromBlock: 0, ToBlock: 100}, 1)
	assert.ErrorIs(t, err, ErrTooManyResults)
	got, err = s.FilterLogs(ctx, Query{FromBlock: 0, ToBlock: 100}, 2)
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

// referenceFilter is go-ethereum's eth/filters filterLogs, copied so the SQL
// translation is checked against the semantics the indexer relies on rather
// than against my reading of them.
func referenceFilter(logs []types.Log, q Query) []types.Log {
	var out []types.Log
	for _, l := range logs {
		if l.BlockNumber < q.FromBlock || l.BlockNumber > q.ToBlock {
			continue
		}
		if len(q.Addresses) > 0 && !slices.Contains(q.Addresses, l.Address) {
			continue
		}
		if len(q.Topics) > len(l.Topics) {
			continue
		}
		match := true
		for i, sub := range q.Topics {
			if len(sub) == 0 {
				continue
			}
			if !slices.Contains(sub, l.Topics[i]) {
				match = false
				break
			}
		}
		if match {
			out = append(out, l)
		}
	}
	return out
}

// TestFilterLogsMatchesGethSemantics writes random logs and checks hundreds
// of random filters against the reference implementation, field by field.
func TestFilterLogsMatchesGethSemantics(t *testing.T) {
	ctx := context.Background()
	s := NewFromPool(testdb.Open(t))
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test data

	addrs := []common.Address{common.HexToAddress("0x1"), common.HexToAddress("0x2"), common.HexToAddress("0x3")}
	topicPool := []common.Hash{common.HexToHash("0xa"), common.HexToHash("0xb"), common.HexToHash("0xc"), common.HexToHash("0xd")}
	const blocks = 40
	var all []types.Log
	var meta []Block
	for n := uint64(1); n <= blocks; n++ {
		meta = append(meta, blockAt(n))
		for i := 0; i < rng.Intn(6); i++ {
			l := types.Log{BlockNumber: n, Index: uint(i), Address: addrs[rng.Intn(len(addrs))], TxHash: hashOf(n*100 + uint64(i)), TxIndex: uint(i / 2)}
			for k := 0; k < 1+rng.Intn(4); k++ { // anonymous logs are never stored (topic0 NOT NULL)
				l.Topics = append(l.Topics, topicPool[rng.Intn(len(topicPool))])
			}
			if rng.Intn(2) == 0 {
				l.Data = []byte{byte(n), byte(i)}
			} else {
				l.Data = []byte{}
			}
			l.BlockHash, l.BlockTimestamp = blockAt(n).Hash, blockAt(n).Timestamp
			all = append(all, l)
		}
	}
	require.NoError(t, s.WriteRange(ctx, 1, blocks, meta, all))

	pick := func(pool []common.Hash) []common.Hash {
		switch rng.Intn(3) {
		case 0:
			return nil
		case 1:
			return []common.Hash{pool[rng.Intn(len(pool))]}
		default:
			return []common.Hash{pool[rng.Intn(len(pool))], pool[rng.Intn(len(pool))]}
		}
	}
	for i := 0; i < 400; i++ {
		from := uint64(rng.Intn(blocks + 2))
		q := Query{FromBlock: from, ToBlock: from + uint64(rng.Intn(blocks))}
		for j := 0; j < rng.Intn(3); j++ {
			q.Addresses = append(q.Addresses, addrs[rng.Intn(len(addrs))])
		}
		for j := 0; j < rng.Intn(5); j++ {
			q.Topics = append(q.Topics, pick(topicPool))
		}
		got, err := s.FilterLogs(ctx, q, 0)
		require.NoError(t, err)
		want := referenceFilter(all, q)
		if want == nil {
			want = []types.Log{}
		}
		require.Equal(t, want, got, "query %+v", q)
	}
}
