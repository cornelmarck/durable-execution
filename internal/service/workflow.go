package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

func (s *Service) CreateWorkflowRun(ctx context.Context, req apiv1.CreateWorkflowRunRequest) (*apiv1.CreateWorkflowRunResponse, error) {
	inputs, _ := json.Marshal(req.Inputs)
	tags, _ := json.Marshal(req.Tags)

	wfr, err := s.store.CreateWorkflowRun(ctx, dbgen.CreateWorkflowRunParams{
		WorkflowName:    req.WorkflowName,
		WorkflowVersion: toText(req.WorkflowVersion),
		Inputs:          inputs,
		CreatedBy:       toText(req.CreatedBy),
		Tags:            tags,
	})
	if err != nil {
		return nil, err
	}

	return &apiv1.CreateWorkflowRunResponse{
		WorkflowRunID: uuidString(wfr.ID),
		WorkflowName:  wfr.WorkflowName,
		Status:        apiv1.WorkflowRunStatus(wfr.Status),
		CreatedAt:     wfr.CreatedAt.Time,
	}, nil
}

func (s *Service) UpdateWorkflowRun(ctx context.Context, workflowRunID string, req apiv1.UpdateWorkflowRunRequest) (*apiv1.UpdateWorkflowRunResponse, error) {
	id, err := parseUUID(workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("invalid workflow_run_id: %w", ErrBadRequest)
	}

	var status dbgen.NullWorkflowRunStatus
	if req.Status != nil {
		status = dbgen.NullWorkflowRunStatus{
			WorkflowRunStatus: dbgen.WorkflowRunStatus(*req.Status),
			Valid:             true,
		}
	}

	wfr, err := s.store.UpdateWorkflowRun(ctx, dbgen.UpdateWorkflowRunParams{
		ID:          id,
		Status:      status,
		Result:      req.Result,
		StartedAt:   toTimestamptz(req.StartedAt),
		CompletedAt: toTimestamptz(req.CompletedAt),
	})
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("workflow run %q: %w", workflowRunID, db.ErrNotFound)
		}
		return nil, err
	}

	return &apiv1.UpdateWorkflowRunResponse{
		WorkflowRunID: workflowRunID,
		Status:        apiv1.WorkflowRunStatus(wfr.Status),
		UpdatedAt:     wfr.UpdatedAt.Time,
	}, nil
}
