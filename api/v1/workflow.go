package apiv1

import (
	"encoding/json"
	"time"
)

// WorkflowRunStatus represents the status of a workflow run.
type WorkflowRunStatus string

const (
	WorkflowRunStatusPending   WorkflowRunStatus = "pending"
	WorkflowRunStatusRunning   WorkflowRunStatus = "running"
	WorkflowRunStatusCompleted WorkflowRunStatus = "completed"
	WorkflowRunStatusFailed    WorkflowRunStatus = "failed"
	WorkflowRunStatusCancelled WorkflowRunStatus = "cancelled"
)

// CreateWorkflowRunRequest is the body for POST /workflow-runs.
type CreateWorkflowRunRequest struct {
	WorkflowName    string            `json:"workflow_name"`
	WorkflowVersion *string           `json:"workflow_version,omitempty"`
	Inputs          json.RawMessage   `json:"inputs,omitempty"`
	CreatedBy       *string           `json:"created_by,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
}

// CreateWorkflowRunResponse is the response for POST /workflow-runs.
type CreateWorkflowRunResponse struct {
	WorkflowRunID string            `json:"workflow_run_id"`
	WorkflowName  string            `json:"workflow_name"`
	Status        WorkflowRunStatus `json:"status"`
	CreatedAt     time.Time         `json:"created_at"`
}

// UpdateWorkflowRunRequest is the body for PATCH /workflow-runs/{workflow_run_id}.
type UpdateWorkflowRunRequest struct {
	Status      *WorkflowRunStatus `json:"status,omitempty"`
	Result      json.RawMessage    `json:"result,omitempty"`
	StartedAt   *time.Time         `json:"started_at,omitempty"`
	CompletedAt *time.Time         `json:"completed_at,omitempty"`
}

// UpdateWorkflowRunResponse is the response for PATCH /workflow-runs/{workflow_run_id}.
type UpdateWorkflowRunResponse struct {
	WorkflowRunID string            `json:"workflow_run_id"`
	Status        WorkflowRunStatus `json:"status"`
	UpdatedAt     time.Time         `json:"updated_at"`
}
