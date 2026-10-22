package middleware

import (
	"context"
	"fmt"
	"github.com/labstack/echo/v4"
	"go-defender/pkg/utils/log"
	"net/http"
	"runtime/debug"
	"time"
)

func Measure(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		req := c.Request()
		if req.RequestURI == "/ping" {
			return next(c)
		}

		ctx := newContext(c, req)
		req = req.WithContext(ctx)
		c.SetRequest(req)

		start := time.Now()
		logger := log.FromContext(ctx)
		logger.Infof("request received")

		err := next(c)
		latency := time.Since(start)
		status := c.Response().Status

		entry := logger.With(
			"status", status,
			"latency", latency.Milliseconds(),
			"method", req.Method,
			"uri", req.RequestURI,
			"path", c.Path(),
		)

		if err != nil {
			c.Error(err)
		}

		// Success
		if status >= 200 && status < 300 {
			entry.Info("request completed successfully")
			return nil
		}

		// Error
		if err == nil {
			err = fmt.Errorf("http error %d", status)
		}

		// Якщо це помилка виконання команди
		if ee, ok := err.(*echo.HTTPError); ok {
			entry = entry.With("echo_error", ee.Message)
		} else {
			entry = entry.With("error_type", fmt.Sprintf("%T", err))
		}

		// Додаємо stacktrace для 5xx
		if status >= 500 {
			entry = entry.With("stacktrace", string(debug.Stack()))
		}

		log.WithError(entry, err).Error("request completed with error")
		return nil
	}
}

func newContext(c echo.Context, req *http.Request) context.Context {
	ctx := req.Context()
	ctx, _ = log.UpdateContext(ctx,
		"method", req.Method,
		"uri", req.RequestURI,
		"path", c.Path(),
	)
	return ctx
}
