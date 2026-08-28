//go:build integration

// Package testdb provides the PostgreSQL harness for integration tests: an
// external database when TEST_DB_HOST is set (CI), otherwise a testcontainers
// postgres:18-alpine, with db/init_pg_db.sql applied once per process.
package testdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	once sync.Once
	dsn  string
	err  error
)

// Open returns a pool over a database with the warehouse schema applied and
// every table empty. Tests share the database, so Open truncates before
// returning; do not run such tests in parallel — and with TEST_DB_* (one
// external database for every package) run `go test -p 1`, as the Makefile
// and CI do, so packages do not truncate each other mid-test.
func Open(t *testing.T) *pgxpool.Pool {
	t.Helper()
	once.Do(func() { dsn, err = start() })
	if err != nil {
		t.Fatalf("test database: %v", err)
	}
	pool, perr := pgxpool.New(context.Background(), dsn)
	if perr != nil {
		t.Fatalf("connect test database: %v", perr)
	}
	t.Cleanup(pool.Close)
	if _, terr := pool.Exec(context.Background(), `TRUNCATE eth_logs, eth_blocks, ingest_cursor`); terr != nil {
		t.Fatalf("truncate: %v", terr)
	}
	return pool
}

func start() (string, error) {
	ctx := context.Background()
	d, err := connectionString(ctx)
	if err != nil {
		return "", err
	}
	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	_, file, _, _ := runtime.Caller(0)
	schema, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "db", "init_pg_db.sql")) //nolint:gosec // repo path
	if err != nil {
		return "", err
	}
	if _, err := pool.Exec(ctx, string(schema)); err != nil {
		return "", fmt.Errorf("apply schema: %w", err)
	}
	return d, nil
}

func connectionString(ctx context.Context) (string, error) {
	if host := os.Getenv("TEST_DB_HOST"); host != "" {
		get := func(k, def string) string {
			if v := os.Getenv(k); v != "" {
				return v
			}
			return def
		}
		return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host, get("TEST_DB_PORT", "5432"), get("TEST_DB_USER", "postgres"),
			get("TEST_DB_PASSWORD", "postgres"), get("TEST_DB_NAME", "test_db")), nil
	}
	container, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("test_db"), postgres.WithUsername("postgres"), postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		return "", fmt.Errorf("start postgres container: %w", err)
	}
	return container.ConnectionString(ctx, "sslmode=disable")
}
