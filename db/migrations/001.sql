-- Migration 001: initial warehouse schema.
-- Identical to db/init_pg_db.sql at this version; apply one or the other on a
-- fresh database (psql -f). Later migrations are numbered NNN.sql and are
-- mirrored into init_pg_db.sql.
\i init_pg_db.sql
