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
	head, ok, err := s.Cursor(ctx)
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
	assert.ErrorIs(t, s.Rewind(ctx, 9), ErrCoverageGap, "rewinding below the coverage start would empty the warehouse")

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

// TestCoverageStaysContiguous pins the rule that makes the coverage interval
// trustworthy: a write must touch the existing interval, and the interval
// grows to include it — so a start_block that jumps ahead cannot leave a hole
// the API would then serve as "no logs".
func TestCoverageStaysContiguous(t *testing.T) {
	ctx := context.Background()
	s := NewFromPool(testdb.Open(t))

	_, ok, err := s.Coverage(ctx)
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, s.WriteRange(ctx, 100, 102, []Block{blockAt(100), blockAt(101), blockAt(102)}, nil))
	cov, ok, err := s.Coverage(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Coverage{Start: 100, Head: 102}, cov)

	// Gap above (104 > head+1) and gap below (to+1 < start) are refused.
	assert.ErrorIs(t, s.WriteRange(ctx, 104, 104, []Block{blockAt(104)}, nil), ErrCoverageGap)
	assert.ErrorIs(t, s.WriteRange(ctx, 90, 98, blocksFor(90, 98), nil), ErrCoverageGap)

	// Extending on either side and replaying inside all work.
	require.NoError(t, s.WriteRange(ctx, 103, 103, []Block{blockAt(103)}, nil))
	require.NoError(t, s.WriteRange(ctx, 97, 99, blocksFor(97, 99), nil))
	require.NoError(t, s.WriteRange(ctx, 100, 101, blocksFor(100, 101), nil))
	cov, _, err = s.Coverage(ctx)
	require.NoError(t, err)
	assert.Equal(t, Coverage{Start: 97, Head: 103}, cov)

	// A rewind keeps the start.
	require.NoError(t, s.Rewind(ctx, 100))
	cov, _, err = s.Coverage(ctx)
	require.NoError(t, err)
	assert.Equal(t, Coverage{Start: 97, Head: 100}, cov)
}

// TestWriteRangeCreatesPartitionOnRollover pins that a batch crossing the
// last pre-created partition (p039 ends at 40,000,000) creates the next one
// instead of failing the COPY.
func TestWriteRangeCreatesPartitionOnRollover(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Open(t)
	s := NewFromPool(pool)
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS eth_logs_p040")

	from, to := uint64(39_999_999), uint64(40_000_001)
	logs := []types.Log{
		{BlockNumber: 39_999_999, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0xa")}, Data: []byte{}},
		{BlockNumber: 40_000_000, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0xa")}, Data: []byte{}},
	}
	require.NoError(t, s.WriteRange(ctx, from, to, blocksFor(from, to), logs))

	var partitionRows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM eth_logs_p040`).Scan(&partitionRows))
	assert.Equal(t, 1, partitionRows)
	got, err := s.FilterLogs(ctx, Query{FromBlock: from, ToBlock: to}, 0)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, "eth_logs_p040", PartitionName(40_000_000))
}

func blocksFor(from, to uint64) []Block {
	out := make([]Block, 0, to-from+1)
	for n := from; n <= to; n++ {
		out = append(out, blockAt(n))
	}
	return out
}

// TestReadSnapshotSurvivesConcurrentRewind pins the API's atomicity
// guarantee: a Read that has already checked coverage keeps seeing the same
// logs even when a rewind commits from another connection mid-request, so a
// request never turns into a silently partial answer; the next Read sees the
// rewound coverage and refuses the range.
func TestReadSnapshotSurvivesConcurrentRewind(t *testing.T) {
	ctx := context.Background()
	s := NewFromPool(testdb.Open(t))
	logs := []types.Log{
		{BlockNumber: 10, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0xa")}, Data: []byte{}},
		{BlockNumber: 12, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0xa")}, Data: []byte{}},
	}
	require.NoError(t, s.WriteRange(ctx, 10, 12, blocksFor(10, 12), logs))

	err := s.Read(ctx, func(v View) error {
		cov, ok, err := v.Coverage(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, Coverage{Start: 10, Head: 12}, cov)

		// A deep-reorg recovery rewinds between the coverage check and the read.
		require.NoError(t, s.Rewind(ctx, 10))

		got, err := v.FilterLogs(ctx, Query{FromBlock: 10, ToBlock: 12}, 0)
		require.NoError(t, err)
		assert.Len(t, got, 2, "the snapshot still holds both logs the coverage authorized")
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, s.Read(ctx, func(v View) error {
		cov, _, err := v.Coverage(ctx)
		require.NoError(t, err)
		assert.Equal(t, Coverage{Start: 10, Head: 10}, cov, "a later read sees the rewound coverage")
		return nil
	}))
}

// TestWriteRangeRefusesLogsFromAnotherBlock pins the store-level guard: a
// log whose blockHash is not the block row it would be stored under is
// refused, so a reorg between two ingestion fetches can never land here.
func TestWriteRangeRefusesLogsFromAnotherBlock(t *testing.T) {
	ctx := context.Background()
	s := NewFromPool(testdb.Open(t))
	l := types.Log{BlockNumber: 10, Address: common.HexToAddress("0x1"), Topics: []common.Hash{common.HexToHash("0xa")}, Data: []byte{}, BlockHash: common.HexToHash("0xbad")}
	err := s.WriteRange(ctx, 10, 10, []Block{blockAt(10)}, []types.Log{l})
	require.ErrorContains(t, err, "log at block 10 carries block hash")
	_, ok, err := s.Coverage(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "nothing was written")

	l.BlockHash = blockAt(10).Hash
	require.NoError(t, s.WriteRange(ctx, 10, 10, []Block{blockAt(10)}, []types.Log{l}))
}

// TestRewindToGenesisBoundary pins that block 0 is a valid rewind target on
// a full-history warehouse: genesis stays, everything above is dropped, and
// the next start resumes at block 1.
func TestRewindToGenesisBoundary(t *testing.T) {
	ctx := context.Background()
	s := NewFromPool(testdb.Open(t))
	require.NoError(t, s.WriteRange(ctx, 0, 2, blocksFor(0, 2), nil))
	require.NoError(t, s.Rewind(ctx, 0))
	cov, ok, err := s.Coverage(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Coverage{Start: 0, Head: 0}, cov)
	_, found, err := s.BlockByHash(ctx, blockAt(0).Hash)
	require.NoError(t, err)
	assert.True(t, found, "genesis is kept")
	_, found, err = s.BlockByHash(ctx, blockAt(1).Hash)
	require.NoError(t, err)
	assert.False(t, found)
}
