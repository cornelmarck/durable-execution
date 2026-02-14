package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/cornelmarck/durable-execution/api"
	"github.com/cornelmarck/durable-execution/internal/server"
)

func main() {
	ctx := context.Background()
	if err := run(ctx, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, w io.Writer) error {
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

	fmt.Fprintf(w, "Listening on %s\n", httpServer.Addr)
	fmt.Fprintf(w, "Swagger UI: http://localhost%s/docs\n", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	wg.Wait()
	return nil
}
