package service

import (
	"context"
	"errors"
	"fmt"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

// SetCheckpoint upserts a checkpoint and optionally extends the run claim.
func (s *Service) SetCheckpoint(ctx context.Context, taskID, stepName string, req apiv1.SetCheckpointRequest) (*apiv1.SetCheckpointResponse, error) {
	tid, err := parseUUID(taskID)
	if err != nil {
		return nil, fmt.Errorf("invalid task_id: %w", ErrBadRequest)
	}

	ownerRunID, err := parseUUID(req.OwnerRun)
	if err != nil {
		return nil, fmt.Errorf("invalid owner_run: %w", ErrBadRequest)
	}

	cp, err := s.store.UpsertCheckpoint(ctx, dbgen.UpsertCheckpointParams{
		TaskID:     tid,
		StepName:   stepName,
		State:      req.State,
		OwnerRunID: ownerRunID,
	})
	if err != nil {
		return nil, err
	}

	if req.ExtendClaimBy != nil {
		if err := s.store.ExtendRunClaim(ctx, dbgen.ExtendRunClaimParams{
			ID:              ownerRunID,
			ExtendBySeconds: *req.ExtendClaimBy,
		}); err != nil {
			return nil, err
		}
	}

	return &apiv1.SetCheckpointResponse{
		TaskID:    taskID,
		StepName:  stepName,
		UpdatedAt: cp.UpdatedAt.Time,
	}, nil
}

// GetCheckpoint retrieves a checkpoint by task ID and step name.
func (s *Service) GetCheckpoint(ctx context.Context, taskID, stepName string) (*apiv1.GetCheckpointResponse, error) {
	tid, err := parseUUID(taskID)
	if err != nil {
		return nil, fmt.Errorf("invalid task_id: %w", ErrBadRequest)
	}

	cp, err := s.store.GetCheckpoint(ctx, dbgen.GetCheckpointParams{
		TaskID:   tid,
		StepName: stepName,
	})
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("checkpoint %q/%q: %w", taskID, stepName, db.ErrNotFound)
		}
		return nil, err
	}

	return &apiv1.GetCheckpointResponse{
		TaskID:    taskID,
		StepName:  stepName,
		State:     cp.State,
		UpdatedAt: cp.UpdatedAt.Time,
	}, nil
}
