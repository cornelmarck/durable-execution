package db

import (
	"context"
	"fmt"

	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the sqlc-generated Queries with a connection pool and
// transaction support. All query methods are available directly via embedding.
type Store struct {
	pool *pgxpool.Pool
	*dbgen.Queries
}

// NewStore creates a Store backed by the given connection pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:    pool,
		Queries: dbgen.New(pool),
	}
}

// ExecTx runs fn within a database transaction. The *dbgen.Queries passed to
// fn is bound to the transaction. If fn returns an error the transaction is
// rolled back; otherwise it is committed.
func (s *Store) ExecTx(ctx context.Context, fn func(*dbgen.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := fn(s.Queries.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
