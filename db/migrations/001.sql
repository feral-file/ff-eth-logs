-- Migration 001: initial warehouse schema.
-- Identical to db/init_pg_db.sql at this version; apply one or the other on a
-- fresh database (psql -f db/migrations/001.sql from any directory: \ir resolves
-- relative to this file). Later migrations are numbered NNN.sql and are
-- mirrored into init_pg_db.sql.
\ir ../init_pg_db.sql
