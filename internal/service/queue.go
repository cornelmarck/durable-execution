package service

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

const (
	defaultTTLSeconds = int32(3600)
	maxQueues         = 128
)

func (s *Service) CreateQueue(ctx context.Context, req apiv1.CreateQueueRequest) (*apiv1.CreateQueueResponse, error) {
	count, err := s.store.CountQueues(ctx)
	if err != nil {
		return nil, err
	}
	if count >= maxQueues {
		return nil, fmt.Errorf("maximum number of queues (%d) reached: %w", maxQueues, ErrBadRequest)
	}

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

func (s *Service) ListQueues(ctx context.Context) (*apiv1.ListQueuesResponse, error) {
	rows, err := s.store.ListQueues(ctx)
	if err != nil {
		return nil, err
	}

	queues := make([]apiv1.CreateQueueResponse, 0, len(rows))
	for _, q := range rows {
		queues = append(queues, apiv1.CreateQueueResponse{
			Name:      q.Name,
			CreatedAt: q.CreatedAt.Time,
			Cleanup: &apiv1.CleanupPolicy{
				TaskTTLSeconds:  &q.TaskTtlSeconds,
				EventTTLSeconds: &q.EventTtlSeconds,
			},
		})
	}

	return &apiv1.ListQueuesResponse{Queues: queues}, nil
}

func (s *Service) DeleteQueue(ctx context.Context, queueName string) error {
	q, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return err
	}

	return s.store.DeleteQueue(ctx, q.ID)
}

func (s *Service) GetQueueStats(ctx context.Context, queueName string) (*apiv1.QueueStatsResponse, error) {
	q, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		return nil, err
	}

	stats, err := s.store.GetQueueStats(ctx, q.ID)
	if err != nil {
		return nil, err
	}

	resp := &apiv1.QueueStatsResponse{
		QueueName:     queueName,
		PendingRuns:   stats.PendingRuns,
		ClaimedRuns:   stats.ClaimedRuns,
		CompletedRuns: stats.CompletedRuns,
	}

	if stats.OldestPendingAgeSeconds.Valid {
		f, _ := stats.OldestPendingAgeSeconds.Float64Value()
		if f.Valid {
			resp.OldestPendingAgeSeconds = &f.Float64
		}
	}

	return resp, nil
}
