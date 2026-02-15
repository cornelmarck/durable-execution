package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cornelmarck/durable-execution/api"
	"github.com/cornelmarck/durable-execution/internal/db"
	"github.com/cornelmarck/durable-execution/internal/server"
	"github.com/cornelmarck/durable-execution/internal/service"
)

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL environment variable is required")
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	store := db.NewStore(pool)
	svc := service.New(store)
	srv := server.NewServer(svc)

	mux := http.NewServeMux()
	api.RegisterDocsRoutes(mux)
	mux.Handle("/", srv)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		httpServer.Shutdown(context.Background())
	}()

	slog.Info("server started", "addr", httpServer.Addr)
	slog.Info("swagger ui", "url", "http://localhost"+httpServer.Addr+"/docs")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	wg.Wait()
	slog.Info("server stopped")
	return nil
}
