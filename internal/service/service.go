package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

// ErrBadRequest is returned for invalid input that originates in the service layer.
var ErrBadRequest = errors.New("bad request")

// Store abstracts database access for the service layer.
//
//go:generate go tool moq -out store_mock_test.go . Store
type Store interface {
	dbgen.Querier
	ExecTx(ctx context.Context, fn func(dbgen.Querier) error) error
}

// RunNotifier signals when new runs may be available for claiming.
type RunNotifier interface {
	// Signal returns a channel that is closed when a new run is created in
	// the given queue. Obtain the channel before attempting a claim to avoid
	// missing notifications that arrive between the claim and the wait.
	Signal(queueName string) <-chan struct{}
}

// Service implements server.Service by bridging API types to the DB layer.
type Service struct {
	store    Store
	notifier RunNotifier
}

// Option configures a Service.
type Option func(*Service)

// WithNotifier sets a RunNotifier for long-poll support in ClaimTasks.
func WithNotifier(n RunNotifier) Option {
	return func(s *Service) { s.notifier = n }
}

// New creates a Service backed by the given store.
func New(store Store, opts ...Option) *Service {
	s := &Service{store: store}
	for _, o := range opts {
		o(s)
	}
	return s
}

func parseUUID(s string) (pgtype.UUID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{}, err
	}
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

func uuidString(u pgtype.UUID) string {
	return uuid.UUID(u.Bytes).String()
}

func toText(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func toTimestamptz(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

