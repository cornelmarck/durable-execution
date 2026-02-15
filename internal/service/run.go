package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

const (
	defaultRetryDelay        = time.Second
	defaultExponentialFactor = 2.0
)

func (s *Service) CompleteRun(ctx context.Context, runID string, req apiv1.CompleteRunRequest) (*apiv1.CompleteRunResponse, error) {
	id, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("invalid run_id: %w", ErrBadRequest)
	}

	if err := s.store.CompleteRun(ctx, dbgen.CompleteRunParams{
		ID:     id,
		Result: req.Result,
	}); err != nil {
		return nil, err
	}

	return &apiv1.CompleteRunResponse{
		RunID:  runID,
		Status: apiv1.RunStatusCompleted,
	}, nil
}

func (s *Service) FailRun(ctx context.Context, runID string, req apiv1.FailRunRequest) (*apiv1.FailRunResponse, error) {
	id, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("invalid run_id: %w", ErrBadRequest)
	}

	run, err := s.store.GetRun(ctx, id)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("run %q: %w", runID, db.ErrNotFound)
		}
		return nil, err
	}

	task, err := s.store.GetTask(ctx, run.TaskID)
	if err != nil {
		return nil, err
	}

	resp := &apiv1.FailRunResponse{
		RunID:   runID,
		Status:  apiv1.RunStatusFailed,
		Attempt: run.Attempt,
	}

	err = s.store.ExecTx(ctx, func(q dbgen.Querier) error {
		if txErr := q.FailRun(ctx, dbgen.FailRunParams{
			ID:    id,
			Error: pgtype.Text{String: req.Error, Valid: true},
		}); txErr != nil {
			return txErr
		}

		if run.Attempt < task.MaxAttempts {
			delay := retryDelay(task.RetryStrategy, run.Attempt)
			scheduledAt := time.Now().Add(delay)

			nextRun, txErr := q.CreateRun(ctx, dbgen.CreateRunParams{
				TaskID:      run.TaskID,
				Attempt:     run.Attempt + 1,
				ScheduledAt: pgtype.Timestamptz{Time: scheduledAt, Valid: true},
			})
			if txErr != nil {
				return txErr
			}

			nextID := uuidString(nextRun.ID)
			resp.NextRunID = &nextID
			resp.NextAttemptAt = &scheduledAt
		} else {
			now := time.Now()
			if txErr := q.UpdateTaskStatus(ctx, dbgen.UpdateTaskStatusParams{
				ID:          task.ID,
				Status:      dbgen.TaskStatusFailed,
				CompletedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}); txErr != nil {
				return txErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Service) ScheduleRun(ctx context.Context, runID string, req apiv1.ScheduleRunRequest) (*apiv1.ScheduleRunResponse, error) {
	id, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("invalid run_id: %w", ErrBadRequest)
	}

	if err := s.store.ScheduleRun(ctx, dbgen.ScheduleRunParams{
		ID:          id,
		ScheduledAt: pgtype.Timestamptz{Time: req.RunAt, Valid: true},
	}); err != nil {
		return nil, err
	}

	return &apiv1.ScheduleRunResponse{
		RunID:       runID,
		ScheduledAt: req.RunAt,
	}, nil
}

func retryDelay(retryStrategyJSON []byte, attempt int32) time.Duration {
	if len(retryStrategyJSON) == 0 {
		return defaultRetryDelay
	}

	var rs apiv1.RetryStrategy
	if err := json.Unmarshal(retryStrategyJSON, &rs); err != nil {
		return defaultRetryDelay
	}

	switch rs.Kind {
	case apiv1.RetryFixed:
		return time.Duration(rs.BaseSeconds * float64(time.Second))
	case apiv1.RetryExponential:
		factor := defaultExponentialFactor
		if rs.Factor != nil {
			factor = *rs.Factor
		}
		delay := rs.BaseSeconds * math.Pow(factor, float64(attempt-1))
		if rs.MaxSeconds != nil && delay > *rs.MaxSeconds {
			delay = *rs.MaxSeconds
		}
		return time.Duration(delay * float64(time.Second))
	default:
		return time.Duration(rs.BaseSeconds * float64(time.Second))
	}
}
