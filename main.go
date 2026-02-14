package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/cornelmarck/durable-execution/api"
	"github.com/cornelmarck/durable-execution/internal/server"
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

	svc := server.NewServer(nil)

	mux := http.NewServeMux()
	api.RegisterDocsRoutes(mux)
	mux.Handle("/", svc)

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
