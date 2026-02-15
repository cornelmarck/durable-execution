package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

func (s *Service) WaitForEvent(ctx context.Context, runID string, req apiv1.WaitForEventRequest) (*apiv1.WaitForEventResponse, error) {
	id, err := parseUUID(runID)
	if err != nil {
		return nil, fmt.Errorf("invalid run_id: %w", ErrBadRequest)
	}

	// Check if event already exists.
	event, err := s.store.GetEventByName(ctx, req.EventName)
	if err == nil {
		return &apiv1.WaitForEventResponse{
			Status:  apiv1.WaitResultResolved,
			Payload: event.Payload,
		}, nil
	}
	if !errors.Is(err, db.ErrNotFound) {
		return nil, err
	}

	// Event not found — put run to sleep.
	timeoutAt := time.Now().Add(time.Duration(req.TimeoutSeconds) * time.Second)
	if err := s.store.SetRunSleeping(ctx, dbgen.SetRunSleepingParams{
		ID:               id,
		WaitingEventName: pgtype.Text{String: req.EventName, Valid: true},
		WaitingStepName:  pgtype.Text{String: req.StepName, Valid: true},
		WaitingTimeoutAt: pgtype.Timestamptz{Time: timeoutAt, Valid: true},
	}); err != nil {
		return nil, err
	}

	return &apiv1.WaitForEventResponse{
		Status:  apiv1.WaitResultSleeping,
		Message: "waiting for event: " + req.EventName,
	}, nil
}

// EmitEvent creates an event and wakes any runs waiting for it.
func (s *Service) EmitEvent(ctx context.Context, req apiv1.EmitEventRequest) (*apiv1.EmitEventResponse, error) {
	event, err := s.store.CreateEvent(ctx, dbgen.CreateEventParams{
		EventName: req.EventName,
		Payload:   req.Payload,
	})
	if err != nil {
		return nil, err
	}

	if _, err := s.store.WakeRunsByEvent(ctx, pgtype.Text{String: req.EventName, Valid: true}); err != nil {
		return nil, err
	}

	return &apiv1.EmitEventResponse{
		EventID:   uuidString(event.ID),
		EventName: event.EventName,
		CreatedAt: event.CreatedAt.Time,
	}, nil
}
