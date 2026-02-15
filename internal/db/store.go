package db

import (
	"context"
	"errors"
	"fmt"

	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrNotFound is returned by Store methods when a queried row does not exist.
	ErrNotFound = errors.New("not found")

	// ErrConflict is returned when a write violates a unique constraint.
	ErrConflict = errors.New("conflict")

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

// ExecTx runs fn within a database transaction. The Querier passed to fn is
// bound to the transaction. If fn returns an error the transaction is rolled
// back; otherwise it is committed.
func (s *Store) ExecTx(ctx context.Context, fn func(dbgen.Querier) error) error {
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

func (s *Store) GetRun(ctx context.Context, id pgtype.UUID) (dbgen.Run, error) {
	r, err := s.Queries.GetRun(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) GetTask(ctx context.Context, id pgtype.UUID) (dbgen.Task, error) {
	r, err := s.Queries.GetTask(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) GetQueueByName(ctx context.Context, name string) (dbgen.Queue, error) {
	r, err := s.Queries.GetQueueByName(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) GetEventByName(ctx context.Context, eventName string) (dbgen.Event, error) {
	r, err := s.Queries.GetEventByName(ctx, eventName)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) GetCheckpoint(ctx context.Context, arg dbgen.GetCheckpointParams) (dbgen.Checkpoint, error) {
	r, err := s.Queries.GetCheckpoint(ctx, arg)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) GetWorkflowRun(ctx context.Context, id pgtype.UUID) (dbgen.WorkflowRun, error) {
	r, err := s.Queries.GetWorkflowRun(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) UpdateWorkflowRun(ctx context.Context, arg dbgen.UpdateWorkflowRunParams) (dbgen.WorkflowRun, error) {
	r, err := s.Queries.UpdateWorkflowRun(ctx, arg)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) CreateQueue(ctx context.Context, arg dbgen.CreateQueueParams) (dbgen.Queue, error) {
	r, err := s.Queries.CreateQueue(ctx, arg)
	if isUniqueViolation(err) {
		return dbgen.Queue{}, ErrConflict
	}
	return r, err
}

func (s *Store) DeleteQueue(ctx context.Context, id pgtype.UUID) error {
	return s.Queries.DeleteQueue(ctx, id)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

