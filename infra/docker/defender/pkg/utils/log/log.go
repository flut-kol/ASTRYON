package log

import (
	"context"
	"go.uber.org/zap"
)

var L = newLoggerEntry(true)

type logKey struct{}

func InitLog(local bool) {
	L = newLoggerEntry(local)
}

func FromContext(ctx context.Context) *zap.SugaredLogger {
	logger := ctx.Value(logKey{})
	if logger == nil {
		return L
	}

	return logger.(*zap.SugaredLogger)
}

func WithLogger(ctx context.Context, logger *zap.SugaredLogger) context.Context {
	return context.WithValue(ctx, logKey{}, logger)
}

func WithError(logger *zap.SugaredLogger, err error) *zap.SugaredLogger {
	return logger.With("error", err)
}

func UpdateContext(ctx context.Context, fields ...interface{}) (context.Context, *zap.SugaredLogger) {
	logger := FromContext(ctx).With(fields...)
	ctx = WithLogger(ctx, logger)
	return ctx, logger
}

func newLoggerEntry(local bool) *zap.SugaredLogger {
	var logger *zap.Logger
	if local {
		logger, _ = zap.NewDevelopment()
	} else {
		logger, _ = zap.NewProduction()
	}

	return logger.Sugar()
}
