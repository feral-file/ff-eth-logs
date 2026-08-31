-- Migration 002: eth_logs_erc1155_id partial expression index.
--
-- The end state (the index) is already in db/init_pg_db.sql, which defines it
-- for a FRESH database. This migration is the production-safe path for a
-- POPULATED warehouse: it builds the index CONCURRENTLY per partition and
-- attaches it, so tail ingestion is never blocked. Run it (psql -f) BEFORE
-- deploying the image whose init_pg_db.sql carries the same statement; the init
-- CREATE INDEX IF NOT EXISTS then no-ops. See docs/operations.md 7.1.
--
-- CREATE INDEX CONCURRENTLY cannot run inside a transaction block, so apply
-- this file in autocommit (plain psql -f, no surrounding BEGIN). It is
-- idempotent and order-independent: on a fresh database where init already
-- created the parent index with its partition indexes attached, every step
-- below no-ops (nothing is built or attached, the validity gate passes).

\set ON_ERROR_STOP on
SET statement_timeout = 0;   -- a dense partition takes minutes

-- 1. Invalid parent shell (ON ONLY: holds no data, instant, no lasting lock).
CREATE INDEX IF NOT EXISTS eth_logs_erc1155_id
  ON ONLY eth_logs (address, (substring(data from 1 for 32)), block_number)
  WHERE topic0 = '\xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'::bytea;

-- 2. Build each leaf CONCURRENTLY (autocommit: each runs in its own
--    transaction). Skip partitions that already have an index attached to the
--    parent -- the fresh-database case, where init built them.
SELECT format(
  'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I (address, (substring(data from 1 for 32)), block_number) WHERE topic0 = ''\xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62''::bytea',
  part || '_erc1155_id', part)
FROM (SELECT 'eth_logs_p' || lpad(g::text, 3, '0') AS part FROM generate_series(0, 39) g) s
WHERE NOT EXISTS (
  SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
  WHERE i.inhparent = 'eth_logs_erc1155_id'::regclass AND c.relname LIKE s.part || '\_%')
\gexec

-- 3. Attach every freshly built leaf not yet attached to the parent.
SELECT format('ALTER INDEX eth_logs_erc1155_id ATTACH PARTITION %I', c.relname)
FROM pg_class c
WHERE c.relname LIKE 'eth\_logs\_p%\_erc1155\_id'
  AND NOT EXISTS (
    SELECT 1 FROM pg_inherits i
    WHERE i.inhparent = 'eth_logs_erc1155_id'::regclass AND i.inhrelid = c.oid)
\gexec

-- 4. Refuse to finish unless the parent is valid (every leaf attached). A
--    non-concurrent deploy build must never be reached because init no-ops once
--    this ran; if the parent is invalid, a leaf failed -- do not deploy.
DO $$
BEGIN
  IF NOT (SELECT indisvalid FROM pg_index WHERE indexrelid = 'eth_logs_erc1155_id'::regclass) THEN
    RAISE EXCEPTION 'eth_logs_erc1155_id is INVALID: a leaf did not attach; do not deploy';
  END IF;
END $$;
