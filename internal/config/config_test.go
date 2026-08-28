package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
	assert.Equal(t, 100_000, cfg.RPC.MaxResults)
	assert.Equal(t, 60*time.Second, cfg.RPC.QueryTimeout)
	assert.True(t, cfg.Ethereum.IngestionEnabled)
	assert.Contains(t, cfg.Database.DSN(), "host=dbhost port=5432 user=postgres")
}

func TestValidate(t *testing.T) {
	base := func() *Config {
		return &Config{Database: DatabaseConfig{Host: "h"}, Ethereum: EthereumConfig{WebSocketURL: "wss://x", IngestionEnabled: true, ConfirmationBlocks: 2, MaxCatchupBlocks: 100}}
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

	c = base()
	c.RPC.MaxResults = -1
	assert.ErrorContains(t, Validate(c), "max_results")
}

func TestLoadMissingFileUsesEnv(t *testing.T) {
	t.Setenv("FF_ETH_LOGS_DATABASE_HOST", "envhost")
	t.Setenv("FF_ETH_LOGS_ETHEREUM_INGESTION_ENABLED", "false")
	cfg, err := Load("", t.TempDir())
	require.NoError(t, err)
	assert.Equal(t, "envhost", cfg.Database.Host)
	assert.False(t, cfg.Ethereum.IngestionEnabled)
}
