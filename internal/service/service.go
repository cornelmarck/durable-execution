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

// Service implements server.Service by bridging API types to the DB layer.
type Service struct {
	store Store
}

// New creates a Service backed by the given store.
func New(store Store) *Service {
	return &Service{store: store}
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

