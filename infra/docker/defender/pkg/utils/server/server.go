package server

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-defender/pkg/utils/log"
)

func RunHTTP(ctx context.Context, s *http.Server, quit chan<- struct{}) *http.Server {
	go func() {
		err := s.ListenAndServe()

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger := log.WithError(log.FromContext(ctx), err)
			logger.Error("http server start error")
			quit <- struct{}{}
		}
	}()

	return s
}

func ShutdownHTTP(ctx context.Context, srv *http.Server) {
	err := srv.Shutdown(ctx)

	if err != nil {
		log.WithError(log.FromContext(ctx), err).Error("unable to shutdown the http server")
	} else {
		log.FromContext(ctx).Info("http server shutdown success")
	}
}

func GracefulShutdown(ctx context.Context, quit <-chan struct{}, cleanup func(ctx context.Context)) {
	const shutdownWaitingTime = 10 * time.Second

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)

	select {
	case <-term:
		log.FromContext(ctx).Info("received shutdown signal")
	case <-quit:
		log.FromContext(ctx).Info("received quit notification")
	}

	ctx, cancel := context.WithTimeout(context.Background(), shutdownWaitingTime)
	defer cancel()

	cleanup(ctx)

	log.FromContext(ctx).Info("server shutdown success")
}
