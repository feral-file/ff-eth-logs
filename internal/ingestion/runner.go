package ingestion

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/logger"
)

// CursorStore is the durable position ingestion resumes from.
type CursorStore interface {
	// Cursor returns the last written block and whether one exists.
	Cursor(ctx context.Context) (uint64, bool, error)
}

// Store is what the runner needs from the warehouse: the cursor and the sink.
type Store interface {
	CursorStore
	Sink
}

// RunConfig is the runner's configuration.
type RunConfig struct {
	Config
	// ChainID is the chain the warehouse stores; the provider must report it
	// (eth_chainId) before a single block is written.
	ChainID uint64
	// StartBlock, when non-zero, overrides the cursor unconditionally — a
	// deliberate rewind or a first run without a backfill. A start block
	// further behind the head than MaxCatchupBlocks is a startup failure.
	StartBlock uint64
	// SpanCap is the provider's eth_getLogs block-range cap (0 = unknown).
	SpanCap uint64
}

// Run resolves the start block and follows the chain until ctx ends or an
// error occurs. Errors are fatal by design: the process exits and the
// supervisor restarts it from the cursor (see Subscriber.Run).
func Run(ctx context.Context, cfg RunConfig, client chain.EthClient, store Store) error {
	if err := verifyChain(ctx, cfg.ChainID, client); err != nil {
		return err
	}
	from, err := resolveStartBlock(ctx, cfg, client, store)
	if err != nil {
		return err
	}
	logger.InfoCtx(ctx, "Starting ethereum ingestion", zap.Uint64("fromBlock", from),
		zap.Uint64("confirmationBlocks", cfg.ConfirmationBlocks), zap.Uint64("maxCatchupBlocks", cfg.MaxCatchupBlocks))
	fetcher := NewFetcher(client, chain.RealClock{}, cfg.SpanCap)
	return NewSubscriber(cfg.Config, client, fetcher, store).Run(ctx, from)
}

// verifyChain refuses to ingest from a provider on the wrong chain. Reason:
// the endpoint is configuration, and a testnet URL would otherwise fill the
// warehouse with blocks the API then serves as mainnet (eth_chainId reports
// the configured id, not the provider's).
func verifyChain(ctx context.Context, want uint64, client chain.EthClient) error {
	got, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("read provider chain id: %w", err)
	}
	if got != want {
		return fmt.Errorf("provider chain id %d does not match configured ethereum.chain_id %d", got, want)
	}
	return nil
}

// resolveStartBlock mirrors the indexer: an explicit start_block wins; else
// the cursor + 1 (the cursor is the last written block); else the current
// head, which leaves history to the backfill.
func resolveStartBlock(ctx context.Context, cfg RunConfig, client chain.EthClient, store CursorStore) (uint64, error) {
	if cfg.StartBlock != 0 {
		return cfg.StartBlock, nil
	}
	cursor, ok, err := store.Cursor(ctx)
	if err != nil {
		return 0, fmt.Errorf("read ingestion cursor: %w", err)
	}
	if ok {
		return cursor + 1, nil
	}
	head, err := client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("read chain head for first start: %w", err)
	}
	logger.WarnCtx(ctx, "No cursor and no start_block: starting at the chain head; history below it needs a backfill",
		zap.Uint64("head", head))
	return head, nil
}
