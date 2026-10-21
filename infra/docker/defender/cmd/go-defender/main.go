package main

import (
	"context"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go-defender/pkg/api"
	mw "go-defender/pkg/api/middleware"
	"go-defender/pkg/domain/defender"
	"go-defender/pkg/utils/log"
	"go-defender/pkg/utils/server"
	"net/http"
)

func main() {
	cfg, err := ReadConfig()
	if err != nil {
		log.L.Fatalf("failed to read config, %v", err)
	}

	ctx := context.Background()
	log.InitLog(cfg.Debug)
	logger := log.FromContext(ctx)

	logger.Infof("starting the app with following config: %v", cfg)

	apiConfig := api.Config{
		Defender: defender.MakeDefender(defender.Config{
			Timeout:      cfg.Timeout,
			SharedFolder: cfg.SharedFolder,
		}),
	}
	defenderService := api.MakeService(apiConfig)

	logger.Info("Services are created")
	logger.Infof("Shared folder to receive files to scan: [%s]", cfg.SharedFolder)

	e := echo.New()
	e.Use(middleware.Recover())
	root := e.Group("/", mw.Measure)
	api.Assemble(root, defenderService)

	s := &http.Server{
		Addr:    cfg.Port,
		Handler: e,
	}

	logger.Infof("go-defender api is starting on port %s", cfg.Port)

	quitChan := make(chan struct{}, 1)
	s = server.RunHTTP(ctx, s, quitChan)

	server.GracefulShutdown(ctx, quitChan, func(ctx context.Context) {
		server.ShutdownHTTP(ctx, s)
		logger.Info("graceful shutdown performed successfully")
	})
}
