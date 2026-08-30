-- FF Eth Logs warehouse schema.
-- Source of truth for a fresh database and for integration tests. Every
-- migration under db/migrations/ must be mirrored here.
--
-- Design (docs/schema.md): bytea everywhere (hex text would double every
-- column), block hash and timestamp stored once per block in eth_blocks and
-- joined on read, eth_logs range-partitioned per 1,000,000 blocks — the same
-- boundaries as the BigQuery export directories the backfill loads.

-- Confirmed blocks only (>= ethereum.confirmation_blocks behind the tip).
CREATE TABLE IF NOT EXISTS eth_blocks (
    number bigint PRIMARY KEY,
    hash   bytea  NOT NULL,   -- 32 bytes
    ts     bigint NOT NULL    -- unix seconds
);
CREATE INDEX IF NOT EXISTS eth_blocks_hash ON eth_blocks (hash);  -- eth_getLogs {blockHash}

CREATE TABLE IF NOT EXISTS eth_logs (
    block_number bigint  NOT NULL,
    log_index    integer NOT NULL,
    tx_index     integer NOT NULL,
    tx_hash      bytea   NOT NULL,   -- 32
    address      bytea   NOT NULL,   -- 20
    topic0       bytea   NOT NULL,   -- 32
    topic1       bytea,              -- 32 or NULL when the log has fewer topics
    topic2       bytea,
    topic3       bytea,
    data         bytea   NOT NULL,   -- '' for ERC-721 Transfer
    PRIMARY KEY (block_number, log_index)
) PARTITION BY RANGE (block_number);

-- One partition per 1,000,000 blocks. 40 covers mainnet to block 40M
-- (~2031 at 12 s blocks); tail ingestion also creates a partition on demand
-- (internal/logstore PartitionDDL), so running out is not a failure mode.
DO $$
BEGIN
    FOR p IN 0..39 LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS eth_logs_p%s PARTITION OF eth_logs FOR VALUES FROM (%s) TO (%s)',
            lpad(p::text, 3, '0'), p * 1000000, (p + 1) * 1000000);
    END LOOP;
END $$;

-- Secondary indexes. Keep the four statements byte-identical to
-- internal/logstore/schema.go (SecondaryIndexes): the backfill drops and
-- recreates them from that list, and a test compares the two.
CREATE INDEX IF NOT EXISTS eth_logs_t1 ON eth_logs (topic1, block_number) WHERE topic1 IS NOT NULL;
CREATE INDEX IF NOT EXISTS eth_logs_t2 ON eth_logs (topic2, block_number) WHERE topic2 IS NOT NULL;
CREATE INDEX IF NOT EXISTS eth_logs_t3 ON eth_logs (topic3, block_number) WHERE topic3 IS NOT NULL;
CREATE INDEX IF NOT EXISTS eth_logs_addr ON eth_logs (address, block_number);

-- What the backfill loaded, per unit ("logs/part=NNN" or "blocks"): the
-- fingerprint of that unit's manifest entries (file sizes + MD5s). A stage
-- skips a unit only when the database holds the manifest's row count AND
-- the recorded fingerprint equals the current manifest's; a corrected export
-- with the same row count but different content therefore reloads, and
-- finish refuses to publish while any unit was loaded from another export.
CREATE TABLE IF NOT EXISTS backfill_units (
    unit        text PRIMARY KEY,
    fingerprint text NOT NULL,
    rows_loaded bigint NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now()
);

-- Durable maintenance flag. A backfill sets it before the first mutation of
-- a unit inside the published coverage and finish clears it after the
-- replacement is verified; every read snapshot refuses while it is set, so
-- a reload that dies between partitions cannot serve a hybrid history once
-- its session lock is gone.
CREATE TABLE IF NOT EXISTS warehouse_state (
    id          smallint PRIMARY KEY CHECK (id = 1),
    maintenance boolean NOT NULL DEFAULT false,
    reason      text NOT NULL DEFAULT '',
    updated_at  timestamptz NOT NULL DEFAULT now()
);
INSERT INTO warehouse_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- The covered interval [coverage_start, block_number]: every block in it has
-- its eth_blocks row and every warehouse log. Exactly one row; written in the
-- same transaction as the blocks and logs it accounts for, so it is never
-- ahead of the data. The API refuses ranges outside it (a fresh database
-- that starts at the chain tip must not answer genesis..head with []).
CREATE TABLE IF NOT EXISTS ingest_cursor (
    id             smallint PRIMARY KEY CHECK (id = 1),
    coverage_start bigint NOT NULL,
    block_number   bigint NOT NULL,   -- the head
    updated_at     timestamptz NOT NULL DEFAULT now(),
    CHECK (coverage_start <= block_number)
);
