package service

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

const defaultTTLSeconds = int32(3600)

func (s *Service) CreateQueue(ctx context.Context, req apiv1.CreateQueueRequest) (*apiv1.CreateQueueResponse, error) {
	taskTTL := defaultTTLSeconds
	eventTTL := defaultTTLSeconds
	if req.Cleanup != nil {
		if req.Cleanup.TaskTTLSeconds != nil {
			taskTTL = *req.Cleanup.TaskTTLSeconds
		}
		if req.Cleanup.EventTTLSeconds != nil {
			eventTTL = *req.Cleanup.EventTTLSeconds
		}
	}

	q, err := s.store.CreateQueue(ctx, dbgen.CreateQueueParams{
		Name:            req.Name,
		TaskTtlSeconds:  taskTTL,
		EventTtlSeconds: eventTTL,
	})
	if err != nil {
		if errors.Is(err, db.ErrConflict) {
			return nil, fmt.Errorf("queue %q: %w", req.Name, db.ErrConflict)
		}
		return nil, err
	}

	return &apiv1.CreateQueueResponse{
		Name:      q.Name,
		CreatedAt: q.CreatedAt.Time,
		Cleanup: &apiv1.CleanupPolicy{
			TaskTTLSeconds:  &q.TaskTtlSeconds,
			EventTTLSeconds: &q.EventTtlSeconds,
		},
	}, nil
}
