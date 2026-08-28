package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/feral-file/ff-eth-logs/internal/backfill"
	"github.com/feral-file/ff-eth-logs/internal/logger"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

// runBackfill loads the Parquet export. Run it with the service stopped (or
// with ethereum.ingestion_enabled=false) on an empty warehouse; each stage is
// idempotent, so re-running after a failure resumes where it stopped.
func runBackfill(args []string) error {
	var dir, stage string
	cfg, ctx, stop, err := commonFlags("backfill", args, func(fs *flag.FlagSet) {
		fs.StringVar(&dir, "dir", "", "Export root containing logs/part=NNN/ and blocks/")
		fs.StringVar(&stage, "stage", "all", "all | prepare | logs | blocks | finish")
	})
	if err != nil {
		return err
	}
	defer stop()
	defer logger.Flush(2 * time.Second)
	if dir == "" {
		return errors.New("backfill: -dir is required")
	}
	store, err := logstore.Open(ctx, cfg.Database.DSN())
	if err != nil {
		return err
	}
	defer store.Close()
	ctx = logger.WithComponent(ctx, logger.ComponentBackfill)
	loader := backfill.New(store.Pool(), dir)
	stages := map[string]func(context.Context) error{
		"prepare": loader.Prepare, "logs": loader.Logs, "blocks": loader.Blocks, "finish": loader.Finish,
	}
	if stage == "all" {
		for _, name := range []string{"prepare", "logs", "blocks", "finish"} {
			if err := stages[name](ctx); err != nil {
				return fmt.Errorf("backfill %s: %w", name, err)
			}
		}
		return nil
	}
	fn, ok := stages[stage]
	if !ok {
		return fmt.Errorf("backfill: unknown stage %q", stage)
	}
	if err := fn(ctx); err != nil {
		return fmt.Errorf("backfill %s: %w", stage, err)
	}
	return nil
}
