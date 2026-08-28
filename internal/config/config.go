// Package config loads the service configuration from a YAML file layered
// with environment variables (prefix FF_ETH_LOGS_, dots → underscores), the
// same dual scheme as ff-indexer-v2.
package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// EnvPrefix is the environment variable prefix for every key.
const EnvPrefix = "FF_ETH_LOGS"

// Config is the whole service configuration.
type Config struct {
	Debug     bool   `mapstructure:"debug"`
	SentryDSN string `mapstructure:"sentry_dsn"`

	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Ethereum EthereumConfig `mapstructure:"ethereum"`
	RPC      RPCConfig      `mapstructure:"rpc"`
}

// ServerConfig is the HTTP listener.
type ServerConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

// DatabaseConfig is the warehouse Postgres.
type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
	SSLMode  string `mapstructure:"sslmode"`
	MaxConns int    `mapstructure:"max_conns"`
}

// DSN renders the pgx connection string.
func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s pool_max_conns=%d",
		d.Host, d.Port, d.User, d.Password, d.DBName, d.SSLMode, d.MaxConns)
}

// EthereumConfig drives tail ingestion. Keys and defaults mirror the
// indexer's ethereum.* block so operators can reason about both the same way.
type EthereumConfig struct {
	// WebSocketURL is the newHeads + eth_getLogs endpoint (one connection).
	WebSocketURL string `mapstructure:"websocket_url"`
	ChainID      uint64 `mapstructure:"chain_id"`
	// IngestionEnabled lets an API-only replica run without following the chain.
	IngestionEnabled bool `mapstructure:"ingestion_enabled"`
	// StartBlock, when non-zero, overrides the cursor. See ingestion.RunConfig.
	StartBlock uint64 `mapstructure:"start_block"`
	// ConfirmationBlocks is the reorg strategy: blocks are written this many
	// blocks behind the tip. Post-merge mainnet reorgs are almost always one
	// block deep; two blocks (~24 s) absorbs them.
	ConfirmationBlocks uint64 `mapstructure:"confirmation_blocks"`
	// MaxCatchupBlocks bounds the gap walked on start. 0 = unbounded.
	MaxCatchupBlocks uint64 `mapstructure:"max_catchup_blocks"`
	// GetLogsSpanCap is the provider's eth_getLogs block-range cap
	// (toBlock-fromBlock). Chainstack: 10100. 0 = discover by rejection.
	GetLogsSpanCap uint64 `mapstructure:"getlogs_span_cap"`
}

// RPCConfig tunes the served API.
type RPCConfig struct {
	MaxResults   int           `mapstructure:"max_results"`
	QueryTimeout time.Duration `mapstructure:"query_timeout"`
}

// Load reads configFile (or config.yaml from ., cmd/ff-eth-logs/, config/)
// after loading envPath/.env and .env.local, then overlays FF_ETH_LOGS_*
// variables and validates.
func Load(configFile, envPath string) (*Config, error) {
	v := viper.New()
	loadEnv(envPath)
	if configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("cmd/ff-eth-logs/")
		v.AddConfigPath("config/")
	}
	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	applyDefaults(v)
	for _, key := range keys {
		_ = v.BindEnv(key)
	}
	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := Validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// loadEnv layers .env then .env.local (later overrides earlier).
func loadEnv(envPath string) {
	if envPath == "" {
		envPath = "config/"
	}
	for _, name := range []string{".env", ".env.local"} {
		_ = godotenv.Overload(filepath.Join(envPath, name))
	}
}

// keys lists every configuration key so environment variables bind even
// when no config file exists.
var keys = []string{
	"debug", "sentry_dsn",
	"server.host", "server.port", "server.read_timeout", "server.write_timeout", "server.idle_timeout",
	"database.host", "database.port", "database.user", "database.password", "database.dbname", "database.sslmode", "database.max_conns",
	"ethereum.websocket_url", "ethereum.chain_id", "ethereum.ingestion_enabled", "ethereum.start_block",
	"ethereum.confirmation_blocks", "ethereum.max_catchup_blocks", "ethereum.getlogs_span_cap",
	"rpc.max_results", "rpc.query_timeout",
}

func applyDefaults(v *viper.Viper) {
	v.SetDefault("debug", false)
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8545)
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "120s")
	v.SetDefault("server.idle_timeout", "120s")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.dbname", "ff_eth_logs")
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("database.max_conns", 16)
	v.SetDefault("ethereum.chain_id", 1)
	v.SetDefault("ethereum.ingestion_enabled", true)
	v.SetDefault("ethereum.confirmation_blocks", 2)
	v.SetDefault("ethereum.max_catchup_blocks", 50_000)
	v.SetDefault("ethereum.getlogs_span_cap", 10_000)
	v.SetDefault("rpc.max_results", 100_000)
	v.SetDefault("rpc.query_timeout", "60s")
}

// Validate rejects configurations the binary would misbehave on. Required
// values are reported together so one restart fixes them all.
func Validate(cfg *Config) error {
	var missing []string
	if cfg.Database.Host == "" {
		missing = append(missing, "database.host")
	}
	if cfg.Ethereum.IngestionEnabled && cfg.Ethereum.WebSocketURL == "" {
		missing = append(missing, "ethereum.websocket_url")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}
	if cfg.Ethereum.MaxCatchupBlocks > 0 && cfg.Ethereum.ConfirmationBlocks >= cfg.Ethereum.MaxCatchupBlocks {
		return fmt.Errorf("ethereum.confirmation_blocks (%d) must be below ethereum.max_catchup_blocks (%d)",
			cfg.Ethereum.ConfirmationBlocks, cfg.Ethereum.MaxCatchupBlocks)
	}
	if cfg.Ethereum.ChainID == 0 {
		return errors.New("ethereum.chain_id must be > 0")
	}
	if cfg.RPC.MaxResults < 0 {
		return errors.New("rpc.max_results must be >= 0")
	}
	return nil
}
