// Command ff-eth-logs is the Ethereum NFT log warehouse: a JSON-RPC server
// that answers eth_getLogs from Postgres, a tail ingestion job that keeps it
// current, and the one-off backfill/rewind operations.
//
// Usage:
//
//	ff-eth-logs [serve] [-config path] [-env dir]        API + tail ingestion (default)
//	ff-eth-logs backfill -dir <export> [-stage all|prepare|logs|blocks|finish]
//	ff-eth-logs rewind -to <block>                        drop blocks above <block>, move the cursor
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/config"
	"github.com/feral-file/ff-eth-logs/internal/ingestion"
	"github.com/feral-file/ff-eth-logs/internal/logger"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
	"github.com/feral-file/ff-eth-logs/internal/rpcapi"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run dispatches on the subcommand and returns the exit code.
func run(args []string) int {
	cmd := "serve"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd, args = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		return exit(runServe(args))
	case "backfill":
		return exit(runBackfill(args))
	case "rewind":
		return exit(runRewind(args))
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (serve|backfill|rewind)\n", cmd)
		return 2
	}
}

// flushTimeout bounds the Sentry flush that follows the terminal log line.
const flushTimeout = 2 * time.Second

// stderr is where a fatal error goes when no logger exists yet (tests swap it).
var stderr io.Writer = os.Stderr

// exit turns the subcommand's result into an exit code. It is the one
// terminal path: the fatal line is written first, then Sentry is flushed, so
// the event that describes the exit is delivered before os.Exit. Failures
// that happen before the logger exists (config, logger setup) go to stderr
// instead of the no-op fallback logger, so an operator still sees why the
// process refused to start.
func exit(err error) int {
	defer logger.Flush(flushTimeout)
	if err != nil && !errors.Is(err, context.Canceled) {
		if !logger.Initialized() {
			_, _ = fmt.Fprintf(stderr, "ff-eth-logs: %v\n", err)
			return 1
		}
		logger.ErrorCtx(context.Background(), errors.New("ff-eth-logs stopped with error"), zap.Error(err))
		return 1
	}
	if logger.Initialized() {
		logger.Info("ff-eth-logs stopped")
	}
	return 0
}

// commonFlags parses -config/-env, loads the configuration, initializes the
// logger and returns a signal-aware root context.
func commonFlags(name string, args []string, extra func(*flag.FlagSet)) (*config.Config, context.Context, context.CancelFunc, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	configFile := fs.String("config", "", "Path to configuration file")
	envPath := fs.String("env", "config/", "Path to environment files")
	if extra != nil {
		extra(fs)
	}
	if err := fs.Parse(args); err != nil {
		return nil, nil, nil, err
	}
	cfg, err := config.Load(*configFile, *envPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load config: %w", err)
	}
	if err := logger.Initialize(logger.Config{Debug: cfg.Debug, SentryDSN: cfg.SentryDSN, Tags: map[string]string{"service": "ff-eth-logs"}}); err != nil {
		return nil, nil, nil, fmt.Errorf("initialize logger: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return cfg, ctx, stop, nil
}

// runServe runs the API and (unless disabled) tail ingestion as one process.
// Either subsystem failing stops the other: ingestion errors are fatal by
// design (restart from the cursor), and the API alone would serve a head
// that silently stops moving.
func runServe(args []string) error {
	cfg, ctx, stop, err := commonFlags("serve", args, nil)
	if err != nil {
		return err
	}
	defer stop()

	store, err := logstore.Open(ctx, cfg.Database.DSN())
	if err != nil {
		return err
	}
	defer store.Close()

	api := rpcapi.NewAPI(store, rpcapi.Config{ChainID: cfg.Ethereum.ChainID, MaxResults: cfg.RPC.MaxResults, QueryTimeout: cfg.RPC.QueryTimeout})
	srv, err := rpcapi.NewServer(rpcapi.ServerConfig{
		Host: cfg.Server.Host, Port: cfg.Server.Port,
		ReadTimeout: cfg.Server.ReadTimeout, WriteTimeout: cfg.Server.WriteTimeout, IdleTimeout: cfg.Server.IdleTimeout,
	}, api)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Run(logger.WithComponent(gctx, logger.ComponentHTTPServer)) })
	if cfg.Ethereum.IngestionEnabled {
		g.Go(func() error { return runIngestion(logger.WithComponent(gctx, logger.ComponentIngestion), cfg, store) })
	} else {
		logger.InfoCtx(ctx, "Tail ingestion disabled; serving the stored head only")
	}
	return g.Wait()
}

func runIngestion(ctx context.Context, cfg *config.Config, store *logstore.Store) error {
	client, err := chain.Dial(ctx, cfg.Ethereum.WebSocketURL, cfg.Ethereum.RPCTimeout)
	if err != nil {
		return fmt.Errorf("dial ethereum websocket: %w", err)
	}
	defer client.Close()
	return ingestion.Run(ctx, ingestion.RunConfig{
		Config:     ingestion.Config{MaxCatchupBlocks: cfg.Ethereum.MaxCatchupBlocks, ConfirmationBlocks: cfg.Ethereum.ConfirmationBlocks, HeadTimeout: cfg.Ethereum.HeadTimeout},
		ChainID:    cfg.Ethereum.ChainID,
		StartBlock: cfg.Ethereum.StartBlock,
		SpanCap:    cfg.Ethereum.GetLogsSpanCap,
	}, client, store)
}

// runRewind is the operator response to a reorg deeper than the confirmation
// lag: drop everything above -to and let the next start re-fetch it.
func runRewind(args []string) error {
	var to uint64
	toSet := false
	cfg, ctx, stop, err := commonFlags("rewind", args, func(fs *flag.FlagSet) {
		// Presence is tracked separately from the value: block 0 is a valid
		// target on a full-history warehouse (keep genesis, re-ingest from 1).
		fs.Func("to", "Last block to keep; everything above is deleted and re-ingested on the next start", func(v string) error {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fmt.Errorf("-to must be a block number: %w", err)
			}
			to, toSet = n, true
			return nil
		})
	})
	if err != nil {
		return err
	}
	defer stop()
	if !toSet {
		return errors.New("rewind: -to is required")
	}
	store, err := logstore.Open(ctx, cfg.Database.DSN())
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Rewind(ctx, to); err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Rewound warehouse", zap.Uint64("to", to))
	return nil
}
