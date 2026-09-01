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
-- this file in autocommit (plain psql -f, no surrounding BEGIN). It enumerates
-- the live partitions from the catalog (WriteRange creates partitions past the
-- initially provisioned p000..p039 on demand) and is idempotent, order- and
-- failure-independent: it drops any leaf a previous concurrent build left
-- INVALID before rebuilding, skips partitions already covered by an attached
-- leaf, and on a fresh database (init already built and attached every leaf)
-- does nothing.

\set ON_ERROR_STOP on
SET statement_timeout = 0;   -- a dense partition takes minutes

-- 1. Invalid parent shell (ON ONLY: holds no data, instant, no lasting lock).
CREATE INDEX IF NOT EXISTS eth_logs_erc1155_id
  ON ONLY eth_logs (address, (substring(data from 1 for 32)), block_number)
  WHERE topic0 = '\xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'::bytea;

-- 2. Recovery: drop any of our leaf indexes a prior run left INVALID and
--    unattached (a failed CONCURRENTLY build), so step 3 rebuilds it cleanly
--    instead of IF NOT EXISTS skipping the broken one. CONCURRENTLY so the
--    cleanup keeps the no-interruption promise (autocommit \gexec permits it).
SELECT format('DROP INDEX CONCURRENTLY IF EXISTS %I', ci.relname)
FROM pg_class ci
JOIN pg_index x ON x.indexrelid = ci.oid
WHERE ci.relname LIKE 'eth\_logs\_p%\_erc1155\_id'
  AND NOT x.indisvalid
  AND NOT EXISTS (SELECT 1 FROM pg_inherits pi
                  WHERE pi.inhrelid = ci.oid AND pi.inhparent = 'eth_logs_erc1155_id'::regclass)
\gexec

-- 3. Build a leaf CONCURRENTLY for every live partition that the parent does
--    not already cover with an attached leaf (regardless of that leaf's name,
--    so a fresh database's init-built leaves are left alone). Partitions come
--    from the catalog, not a fixed range, so p040+ are included.
SELECT format(
  'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I (address, (substring(data from 1 for 32)), block_number) WHERE topic0 = ''\xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62''::bytea',
  part.relname || '_erc1155_id', part.relname)
FROM pg_inherits pt
JOIN pg_class part ON part.oid = pt.inhrelid
WHERE pt.inhparent = 'eth_logs'::regclass
  AND NOT EXISTS (
    SELECT 1 FROM pg_inherits pi
    JOIN pg_index cx ON cx.indexrelid = pi.inhrelid
    WHERE pi.inhparent = 'eth_logs_erc1155_id'::regclass AND cx.indrelid = part.oid)
\gexec

-- 4. Attach every valid, not-yet-attached leaf this migration built.
SELECT format('ALTER INDEX eth_logs_erc1155_id ATTACH PARTITION %I', ci.relname)
FROM pg_class ci
JOIN pg_index x ON x.indexrelid = ci.oid
WHERE ci.relname LIKE 'eth\_logs\_p%\_erc1155\_id'
  AND x.indisvalid
  AND NOT EXISTS (SELECT 1 FROM pg_inherits pi
                  WHERE pi.inhrelid = ci.oid AND pi.inhparent = 'eth_logs_erc1155_id'::regclass)
\gexec

-- 5. Refuse to finish unless the parent is valid (every live partition covered
--    by an attached leaf). Otherwise a leaf failed -- do not deploy.
DO $$
BEGIN
  IF NOT (SELECT indisvalid FROM pg_index WHERE indexrelid = 'eth_logs_erc1155_id'::regclass) THEN
    RAISE EXCEPTION 'eth_logs_erc1155_id is INVALID: a leaf did not build/attach; do not deploy';
  END IF;
END $$;
