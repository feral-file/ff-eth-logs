// Package logger is the process-wide structured logger: zap, with errors
// forwarded to Sentry when a DSN is configured. It is a trimmed port of
// ff-indexer-v2/internal/logger so log shapes (JSON fields, the "component"
// tag) match what operators already filter on.
package logger

import (
	"context"
	"time"

	"github.com/TheZeroSlave/zapsentry"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	log          *zap.Logger
	sentryClient *sentry.Client
)

type componentKey struct{}

// Component tags identify the subsystem in structured logs (field "component").
// Keep these string values stable: operators and log filters depend on them.
const (
	ComponentHTTPServer = "http-server"
	ComponentIngestion  = "ingestion"
	ComponentBackfill   = "backfill"
)

// WithComponent returns a new context with the component name attached.
func WithComponent(ctx context.Context, component string) context.Context {
	return context.WithValue(ctx, componentKey{}, component)
}

// Config holds logger configuration.
type Config struct {
	Debug     bool
	SentryDSN string
	Tags      map[string]string
}

// Initialize builds the global logger. Errors (and above) are sent to Sentry
// when a DSN is configured; Info-level lines become breadcrumbs.
func Initialize(cfg Config) error {
	zapConfig := zap.NewProductionConfig()
	if cfg.Debug {
		zapConfig = zap.NewDevelopmentConfig()
	}
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	if cfg.Debug {
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}
	zapConfig.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder

	baseLogger, err := zapConfig.Build()
	if err != nil {
		return err
	}
	if cfg.SentryDSN == "" {
		log = baseLogger
		return nil
	}

	sentryClient, err = sentry.NewClient(sentry.ClientOptions{Dsn: cfg.SentryDSN, Debug: cfg.Debug})
	if err != nil {
		return err
	}
	core, err := zapsentry.NewCore(zapsentry.Configuration{
		Level:             zapcore.ErrorLevel,
		EnableBreadcrumbs: true,
		BreadcrumbLevel:   zapcore.InfoLevel,
		Tags:              cfg.Tags,
	}, zapsentry.NewSentryClientFromClient(sentryClient))
	if err != nil {
		return err
	}
	log = zapsentry.AttachCoreToLogger(core, baseLogger)
	return nil
}

// Flush flushes buffered Sentry events; call it after the last error line,
// before the process exits.
func Flush(timeout time.Duration) {
	if sentryClient != nil {
		sentryClient.Flush(timeout)
	}
}

// Initialized reports whether Initialize has built the global logger. Before
// that, FromContext returns a no-op logger, so a caller that must not lose a
// message (a fatal startup error) writes it elsewhere.
func Initialized() bool { return log != nil }

// FromContext returns the logger with the context's component field attached.
// Before Initialize (tests) it returns a no-op logger rather than nil.
func FromContext(ctx context.Context) *zap.Logger {
	base := log
	if base == nil {
		base = zap.NewNop()
	}
	if ctx == nil {
		return base
	}
	l := base.With(zapsentry.Context(ctx))
	if component, _ := ctx.Value(componentKey{}).(string); component != "" {
		l = l.With(zap.String("component", component))
	}
	return l
}

// Info logs at info level without context.
func Info(msg string, fields ...zap.Field) { FromContext(context.Background()).Info(msg, fields...) }

// InfoCtx logs at info level with the context's component.
func InfoCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Info(msg, fields...)
}

// WarnCtx logs at warn level with the context's component.
func WarnCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Warn(msg, fields...)
}

// DebugCtx logs at debug level with the context's component.
func DebugCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Debug(msg, fields...)
}

// ErrorCtx logs an error (its message becomes the log line) with the
// context's component; this is the level that reaches Sentry.
func ErrorCtx(ctx context.Context, err error, fields ...zap.Field) {
	msg := "error occurred"
	if err != nil {
		msg = err.Error()
	}
	FromContext(ctx).Error(msg, fields...)
}
