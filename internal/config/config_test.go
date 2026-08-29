package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaultsAndEnvOverride(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte("database:\n  host: dbhost\nethereum:\n  websocket_url: wss://x\n"), 0o600))
	t.Setenv("FF_ETH_LOGS_SERVER_PORT", "9999")
	t.Setenv("FF_ETH_LOGS_ETHEREUM_MAX_CATCHUP_BLOCKS", "0")

	cfg, err := Load(cfgPath, dir)
	require.NoError(t, err)
	assert.Equal(t, "dbhost", cfg.Database.Host)
	assert.Equal(t, 9999, cfg.Server.Port)
	assert.Equal(t, uint64(0), cfg.Ethereum.MaxCatchupBlocks)
	assert.Equal(t, uint64(2), cfg.Ethereum.ConfirmationBlocks)
	assert.Equal(t, uint64(10_000), cfg.Ethereum.GetLogsSpanCap)
	assert.Equal(t, 5*time.Minute, cfg.Ethereum.HeadTimeout)
	assert.Equal(t, 60*time.Second, cfg.Ethereum.RPCTimeout)
	assert.Equal(t, 100_000, cfg.RPC.MaxResults)
	assert.Equal(t, 60*time.Second, cfg.RPC.QueryTimeout)
	assert.True(t, cfg.Ethereum.IngestionEnabled)
	assert.Equal(t, "postgres://postgres:@dbhost:5432/ff_eth_logs?pool_max_conns=16&sslmode=disable", cfg.Database.DSN())
}

// TestDSNEscapesCredentials pins that generated passwords with characters
// that are special in URLs or keyword/value syntax survive the round trip.
func TestDSNEscapesCredentials(t *testing.T) {
	d := DatabaseConfig{Host: "db.internal", Port: 5433, User: "ff user", Password: `p@ss w/rd'"\#?&`, DBName: "ff_eth_logs", SSLMode: "require", MaxConns: 8} //nolint:gosec // deliberately awkward test value, not a credential
	cfg, err := pgxpool.ParseConfig(d.DSN())
	require.NoError(t, err)
	assert.Equal(t, "ff user", cfg.ConnConfig.User)
	assert.Equal(t, `p@ss w/rd'"\#?&`, cfg.ConnConfig.Password)
	assert.Equal(t, "db.internal", cfg.ConnConfig.Host)
	assert.Equal(t, uint16(5433), cfg.ConnConfig.Port)
	assert.Equal(t, "ff_eth_logs", cfg.ConnConfig.Database)
	assert.Equal(t, int32(8), cfg.MaxConns)
}

func TestValidate(t *testing.T) {
	base := func() *Config {
		return &Config{Database: DatabaseConfig{Host: "h"}, Ethereum: EthereumConfig{WebSocketURL: "wss://x", ChainID: 1, IngestionEnabled: true, ConfirmationBlocks: 2, MaxCatchupBlocks: 100}, RPC: RPCConfig{QueryTimeout: time.Second}}
	}
	require.NoError(t, Validate(base()))

	c := base()
	c.Database.Host = ""
	c.Ethereum.WebSocketURL = ""
	assert.EqualError(t, Validate(c), "missing required config: database.host, ethereum.websocket_url")

	c = base()
	c.Ethereum.WebSocketURL = ""
	c.Ethereum.IngestionEnabled = false
	require.NoError(t, Validate(c), "API-only replicas need no RPC endpoint")

	c = base()
	c.Ethereum.ConfirmationBlocks = 100
	assert.ErrorContains(t, Validate(c), "must be below")

	for _, id := range []uint64{0, 11155111, 137} {
		c = base()
		c.Ethereum.ChainID = id
		assert.ErrorContains(t, Validate(c), "ethereum.chain_id must be 1", "chain %d", id)
	}

	c = base()
	c.RPC.MaxResults = -1
	assert.ErrorContains(t, Validate(c), "max_results")

	for _, d := range []time.Duration{0, -time.Second} {
		c = base()
		c.RPC.QueryTimeout = d
		assert.ErrorContains(t, Validate(c), "rpc.query_timeout must be > 0", d)
	}
}

func TestLoadMissingFileUsesEnv(t *testing.T) {
	t.Setenv("FF_ETH_LOGS_DATABASE_HOST", "envhost")
	t.Setenv("FF_ETH_LOGS_ETHEREUM_INGESTION_ENABLED", "false")
	cfg, err := Load("", t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, "envhost", cfg.Database.Host)
	assert.False(t, cfg.Ethereum.IngestionEnabled)
}
