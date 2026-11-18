package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/boscostix/JobFlow/internal/api"
	"github.com/boscostix/JobFlow/internal/config"
	"github.com/boscostix/JobFlow/internal/metrics"
	"github.com/boscostix/JobFlow/internal/store"
	"github.com/boscostix/JobFlow/internal/tasks"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("API stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "api")
	url, err := config.DatabaseURL()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Scrapes use a separate, single-connection pool so they cannot occupy
	// connections reserved for API operations.
	scrapeConfig := pool.Config()
	scrapeConfig.MaxConns = 1
	scrapeConfig.MinConns = 0
	scrapeConfig.MinIdleConns = 0
	scrapePool, err := pgxpool.NewWithConfig(context.Background(), scrapeConfig)
	if err != nil {
		return err
	}
	defer scrapePool.Close()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.QueueHandler(store.New(scrapePool).QueueDepth, logger))
	mux.Handle("/", api.New(store.New(pool).WithObservability(logger, nil), tasks.DefaultRegistry(), logger))
	srv := &http.Server{Addr: config.Env("API_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { logger.Info("API listening", "address", srv.Addr); done <- srv.ListenAndServe() }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
