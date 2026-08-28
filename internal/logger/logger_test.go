package logger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestFromContextAddsComponent(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	log = zap.New(core)
	t.Cleanup(func() { log = nil })

	ctx := WithComponent(context.Background(), ComponentIngestion)
	InfoCtx(ctx, "hello", zap.Int("n", 1))
	WarnCtx(ctx, "careful")
	DebugCtx(ctx, "detail")
	ErrorCtx(ctx, nil)
	ErrorCtx(context.Background(), assert.AnError)
	Info("plain")

	entries := logs.All()
	require.Len(t, entries, 6)
	assert.Equal(t, "hello", entries[0].Message)
	assert.Equal(t, ComponentIngestion, entries[0].ContextMap()["component"])
	assert.Equal(t, "error occurred", entries[3].Message)
	assert.Equal(t, assert.AnError.Error(), entries[4].Message)
	_, hasComponent := entries[5].ContextMap()["component"]
	assert.False(t, hasComponent)
}

func TestInitializeWithoutSentry(t *testing.T) {
	require.NoError(t, Initialize(Config{Debug: true}))
	assert.NotNil(t, FromContext(context.Background()))
	Flush(0)
	log = nil
}

func TestFromContextBeforeInitialize(t *testing.T) {
	log = nil
	assert.NotPanics(t, func() { InfoCtx(context.Background(), "no logger yet") })
}
