// Package logstore is the Postgres warehouse: the schema, the atomic writer
// tail ingestion uses, the reader the JSON-RPC facade serves from, and the
// cursor that ties them together (warehouse head == last written block).
package logstore

import "fmt"

// PartitionBlocks is the width of one eth_logs range partition. It matches
// the directory layout of the BigQuery export (part=NNN = 1,000,000 blocks),
// so the backfill loads one directory into one partition.
const PartitionBlocks = uint64(1_000_000)

// IndexDef is one secondary index on eth_logs. The backfill drops them before
// the bulk load (random-order btree inserts over 400 M rows would take days)
// and recreates them afterwards, so the DDL lives here as the single source
// the loader executes. db/init_pg_db.sql must carry the same statements —
// TestSchemaMatchesInit pins that.
type IndexDef struct {
	Name string
	DDL  string
}

// SecondaryIndexes are the eth_logs indexes beyond the primary key, one per
// query shape and nothing speculative:
//   - owner scans (owner in topic 1 / 2 / 3), block range in the key so a
//     bounded scan never touches the heap for out-of-range rows;
//   - per-contract provenance (address), for walks with no token topic
//     (ERC-1155, whose token id lives in data);
//   - ERC-721 per-token provenance (address, topic3): measured in prod
//     (2026-08-31), neither single-column index serves it — a small token id
//     matches every "token N" log of every contract via eth_logs_t3, and a
//     large collection matches its whole history via eth_logs_addr, so the
//     planner filters megarows either way (20-60 s per genesis-to-head walk,
//     timing out the indexer's provenance backfill into vendor fall-through).
//
// No index on tx_hash: the only tx lookup in the indexer is bounded by
// address + block. No index on topic0 alone: every served query carries a
// block range or a more selective column.
var SecondaryIndexes = []IndexDef{
	{Name: "eth_logs_t1", DDL: "CREATE INDEX eth_logs_t1 ON eth_logs (topic1, block_number) WHERE topic1 IS NOT NULL"},
	{Name: "eth_logs_t2", DDL: "CREATE INDEX eth_logs_t2 ON eth_logs (topic2, block_number) WHERE topic2 IS NOT NULL"},
	{Name: "eth_logs_t3", DDL: "CREATE INDEX eth_logs_t3 ON eth_logs (topic3, block_number) WHERE topic3 IS NOT NULL"},
	{Name: "eth_logs_addr", DDL: "CREATE INDEX eth_logs_addr ON eth_logs (address, block_number)"},
	{Name: "eth_logs_addr_t3", DDL: "CREATE INDEX eth_logs_addr_t3 ON eth_logs (address, topic3, block_number) WHERE topic3 IS NOT NULL"},
}

// PartitionName is the eth_logs partition holding block n.
func PartitionName(n uint64) string {
	return fmt.Sprintf("eth_logs_p%03d", n/PartitionBlocks)
}

// PartitionDDL creates the partition for the 1M-block bucket containing n.
func PartitionDDL(n uint64) string {
	lo := n / PartitionBlocks * PartitionBlocks
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF eth_logs FOR VALUES FROM (%d) TO (%d)",
		PartitionName(n), lo, lo+PartitionBlocks)
}
