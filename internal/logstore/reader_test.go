package logstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-eth-logs/internal/eventset"
)

func TestBuildFilter(t *testing.T) {
	a := common.HexToAddress("0x1")
	t0 := common.HexToHash("0xa0")
	t2 := common.HexToHash("0xa2")
	sql, args := buildFilter(Query{
		FromBlock: 5, ToBlock: 9,
		Addresses: []common.Address{a},
		Topics:    [][]common.Hash{{t0}, nil, {t2}, {}},
	}, 100)
	assert.Contains(t, sql, "l.block_number BETWEEN $1 AND $2")
	assert.Contains(t, sql, "l.address = ANY($3::bytea[])")
	assert.Contains(t, sql, "l.topic0 = ANY($4::bytea[])")
	assert.NotContains(t, sql, "l.topic1 ", "a wildcard position adds no clause")
	assert.Contains(t, sql, "l.topic2 = ANY($5::bytea[])")
	assert.NotContains(t, sql, "l.topic3 ")
	assert.True(t, strings.HasSuffix(sql, "ORDER BY l.block_number, l.log_index LIMIT 101"), sql)
	require.Len(t, args, 5)
	assert.Equal(t, int64(5), args[0])
	assert.Equal(t, [][]byte{a.Bytes()}, args[2])

	sql, args = buildFilter(Query{FromBlock: 1, ToBlock: 1}, 0)
	assert.NotContains(t, sql, "LIMIT")
	assert.NotContains(t, sql, "ANY(")
	assert.Len(t, args, 2)

	// ERC1155ID: the TransferSingle arm inlines topic0 as a fixed bytea literal
	// (so the partial index stays eligible under a generic prepared plan) and
	// keeps the id parameterized. TransferSingle-only collapses to that one arm.
	id := common.HexToHash("0x2a")
	ts := eventset.TransferSingle
	lit := transferSingleTopic0SQL
	sql, args = buildFilter(Query{FromBlock: 1, ToBlock: 2, Topics: [][]common.Hash{{ts}}, ERC1155ID: &id}, 0)
	assert.Contains(t, sql, "((l.topic0 = "+lit+" AND substring(l.data from 1 for 32) = $3))")
	assert.NotContains(t, sql, "l.topic0 = $", "TransferSingle topic0 must be a literal, not a parameter")
	require.Len(t, args, 3)
	assert.Equal(t, id.Bytes(), args[2])

	// A mixed [[TransferSingle, URI]] topics[0] becomes a two-arm OR: TS by the
	// literal + data word 0, URI by topic1 (served by eth_logs_t1, param is fine).
	uri := eventset.URI
	sql, args = buildFilter(Query{FromBlock: 1, ToBlock: 2, Topics: [][]common.Hash{{ts, uri}}, ERC1155ID: &id}, 0)
	assert.Contains(t, sql, "((l.topic0 = "+lit+" AND substring(l.data from 1 for 32) = $3) OR (l.topic0 = $4 AND l.topic1 = $5))")
	require.Len(t, args, 5)
	assert.Equal(t, id.Bytes(), args[2])
	assert.Equal(t, uri.Bytes(), args[3])
	assert.Equal(t, id.Bytes(), args[4])

	// URI alone is still id-filtered, by topic1.
	sql, _ = buildFilter(Query{FromBlock: 1, ToBlock: 2, Topics: [][]common.Hash{{uri}}, ERC1155ID: &id}, 0)
	assert.Contains(t, sql, "((l.topic0 = $3 AND l.topic1 = $4))")
	assert.NotContains(t, sql, "substring(l.data")

	// A signature without an id column (TransferBatch) passes through unfiltered.
	batch := eventset.TransferBatch
	sql, _ = buildFilter(Query{FromBlock: 1, ToBlock: 2, Topics: [][]common.Hash{{batch}}, ERC1155ID: &id}, 0)
	assert.NotContains(t, sql, "substring(l.data")
	assert.NotContains(t, sql, "l.topic1 =")
	assert.Contains(t, sql, "(l.topic0 = ANY($3::bytea[]))")

	// nil ERC1155ID never emits the data predicate.
	assert.NotContains(t, buildFilterSQL(Query{FromBlock: 1, ToBlock: 2, Topics: [][]common.Hash{{ts}}}), "substring(l.data")
}

// TestBuildFilterTransferSingleLiteral pins the inlined topic0 literal to
// eventset.TransferSingle so it cannot drift from the index predicate.
func TestBuildFilterTransferSingleLiteral(t *testing.T) {
	want := "'\\x" + strings.TrimPrefix(eventset.TransferSingle.Hex(), "0x") + "'::bytea"
	assert.Equal(t, want, transferSingleTopic0SQL)
}

// buildFilterSQL is a tiny helper returning only the SQL, for negative asserts.
func buildFilterSQL(q Query) string {
	sql, _ := buildFilter(q, 0)
	return sql
}

// TestSchemaERC1155IndexMatchesSignature guards that the TransferSingle hash
// hard-coded in the eth_logs_erc1155_id partial-index predicate stays equal to
// eventset.TransferSingle; a drift would build an index no query can use.
func TestSchemaERC1155IndexMatchesSignature(t *testing.T) {
	var ddl string
	for _, idx := range SecondaryIndexes {
		if idx.Name == "eth_logs_erc1155_id" {
			ddl = idx.DDL
		}
	}
	require.NotEmpty(t, ddl, "eth_logs_erc1155_id must be in SecondaryIndexes")
	want := strings.TrimPrefix(eventset.TransferSingle.Hex(), "0x")
	assert.Contains(t, ddl, want, "index predicate topic0 must equal eventset.TransferSingle")
}

func TestPartitionNames(t *testing.T) {
	assert.Equal(t, "eth_logs_p000", PartitionName(999_999))
	assert.Equal(t, "eth_logs_p016", PartitionName(16_194_300))
	assert.Equal(t, "CREATE TABLE IF NOT EXISTS eth_logs_p016 PARTITION OF eth_logs FOR VALUES FROM (16000000) TO (17000000)", PartitionDDL(16_194_300))
}

// TestSchemaMatchesInit pins that db/init_pg_db.sql carries every secondary
// index the backfill recreates from SecondaryIndexes, statement for statement.
func TestSchemaMatchesInit(t *testing.T) {
	sql, err := os.ReadFile(filepath.Join("..", "..", "db", "init_pg_db.sql"))
	require.NoError(t, err)
	for _, idx := range SecondaryIndexes {
		want := strings.Replace(idx.DDL, "CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1) + ";"
		assert.Contains(t, string(sql), want, idx.Name)
	}
}
