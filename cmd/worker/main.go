package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/boscostix/JobFlow/internal/config"
	"github.com/boscostix/JobFlow/internal/store"
	"github.com/boscostix/JobFlow/internal/tasks"
	"github.com/boscostix/JobFlow/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	url, err := config.DatabaseURL()
	if err != nil {
		return err
	}
	poll, err := config.Duration("WORKER_POLL_INTERVAL", 250*time.Millisecond)
	if err != nil {
		return err
	}
	grace, err := config.Duration("WORKER_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	lease, err := config.Duration("WORKER_LEASE_DURATION", 10*time.Second)
	if err != nil {
		return err
	}
	if lease < 30*time.Millisecond {
		return fmt.Errorf("WORKER_LEASE_DURATION must be at least 30ms")
	}
	concurrency, err := config.Concurrency()
	if err != nil {
		return err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	workerID := "worker_" + hex.EncodeToString(token[:])
	logger.Info("worker starting", "worker_id", workerID, "concurrency", concurrency)
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	w := worker.Worker{ID: workerID, Concurrency: concurrency, LeaseDuration: lease, Store: store.NewWithLease(pool, lease), Registry: tasks.DefaultRegistry(), Log: logger, PollInterval: poll}
	return runWorker(ctx, grace, w.Run, pool.Close)
}

// Only close the pool after the worker has returned its connections. On timeout,
// main exits the process; pool.Close would wait for the very work we timed out.
func runWorker(ctx context.Context, grace time.Duration, run func(context.Context) error, closePool func()) error {
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	select {
	case err := <-done:
		closePool()
		return err
	case <-ctx.Done():
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case err := <-done:
			closePool()
			return err
		case <-timer.C:
			return fmt.Errorf("shutdown exceeded %s; in-flight jobs will recover after lease expiry", grace)
		}
	}
}
