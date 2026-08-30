package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunDispatch(t *testing.T) {
	assert.Equal(t, 2, run([]string{"bogus"}), "unknown subcommand is a usage error")
	// A subcommand with a bad flag fails before touching config or the database.
	assert.Equal(t, 1, run([]string{"rewind", "-nope"}))
	assert.Equal(t, 1, run([]string{"backfill", "-nope"}))
	assert.Equal(t, 1, run([]string{"-nope"}), "flags without a subcommand mean serve")
}

func TestExit(t *testing.T) {
	var out bytes.Buffer
	stderr = &out
	t.Cleanup(func() { stderr = os.Stderr })

	assert.Equal(t, 0, exit(nil))
	assert.Equal(t, 0, exit(context.Canceled), "shutdown by signal is a clean exit")
	// Before the logger exists (bad config, logger setup) the error must still
	// reach the operator: it goes to stderr instead of the no-op logger.
	assert.Equal(t, 1, exit(errors.New("load config: missing required config: database.host")))
	assert.Equal(t, "ff-eth-logs: load config: missing required config: database.host\n", out.String())
}

// TestStartupFailureIsVisible pins the end-to-end path: an invalid
// configuration makes the subcommand exit 1 with the reason on stderr.
func TestStartupFailureIsVisible(t *testing.T) {
	var out bytes.Buffer
	stderr = &out
	t.Cleanup(func() { stderr = os.Stderr })
	t.Setenv("FF_ETH_LOGS_DATABASE_HOST", "")
	t.Setenv("FF_ETH_LOGS_ETHEREUM_INGESTION_ENABLED", "false")

	assert.Equal(t, 1, run([]string{"serve", "-env", t.TempDir()}))
	assert.Contains(t, out.String(), "missing required config: database.host")
}

func TestRewindRequiresTarget(t *testing.T) {
	t.Setenv("FF_ETH_LOGS_DATABASE_HOST", "localhost")
	t.Setenv("FF_ETH_LOGS_ETHEREUM_INGESTION_ENABLED", "false")
	err := runRewind([]string{"-env", t.TempDir()})
	assert.EqualError(t, err, "rewind: -to is required")
	// An explicit -to 0 is a target (keep genesis), so it gets past the flag
	// check and fails only on the unreachable database.
	t.Setenv("FF_ETH_LOGS_DATABASE_PORT", "1")
	err = runRewind([]string{"-env", t.TempDir(), "-to", "0"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "-to is required")
	assert.Contains(t, err.Error(), "warehouse database")
	err = runRewind([]string{"-env", t.TempDir(), "-to", "x"})
	assert.ErrorContains(t, err, "-to must be a block number")
	err = runBackfill([]string{"-env", t.TempDir()})
	assert.EqualError(t, err, "backfill: -dir is required")
}
