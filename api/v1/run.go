package apiv1

import (
	"encoding/json"
	"time"
)

// RunStatus represents the status of a run.
type RunStatus string

const (
	RunStatusPending   RunStatus = "pending"
	RunStatusClaimed   RunStatus = "claimed"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusSleeping  RunStatus = "sleeping"
)

// WaitResult represents the outcome of a wait-for-event operation.
type WaitResult string

const (
	WaitResultResolved WaitResult = "resolved"
	WaitResultSleeping WaitResult = "sleeping"
	WaitResultTimeout  WaitResult = "timeout"
)

// RunSummary is a single run in a list response.
type RunSummary struct {
	ID          string    `json:"id"`
	TaskID      string    `json:"task_id"`
	Attempt     int32     `json:"attempt"`
	Status      RunStatus `json:"status"`
	Error       *string   `json:"error,omitempty"`
	CreatedAt   string    `json:"created_at"`
	CompletedAt *string   `json:"completed_at,omitempty"`
}

// ListRunsResponse is the response for GET /runs.
type ListRunsResponse struct {
	Runs       []RunSummary `json:"runs"`
	NextCursor *string      `json:"next_cursor,omitempty"`
}

// CompleteRunRequest is the body for POST /runs/{run_id}/complete.
type CompleteRunRequest struct {
	Result json.RawMessage `json:"result,omitempty"`
}

// CompleteRunResponse is the response for POST /runs/{run_id}/complete.
type CompleteRunResponse struct {
	RunID  string    `json:"run_id"`
	Status RunStatus `json:"status"`
}

// FailRunRequest is the body for POST /runs/{run_id}/fail.
type FailRunRequest struct {
	Error string `json:"error" validate:"required"`
}

// FailRunResponse is the response for POST /runs/{run_id}/fail.
type FailRunResponse struct {
	RunID         string     `json:"run_id"`
	Status        RunStatus  `json:"status"`
	Attempt       int32      `json:"attempt"`
	NextRunID     *string    `json:"next_run_id,omitempty"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
}

// ScheduleRunRequest is the body for POST /runs/{run_id}/schedule.
type ScheduleRunRequest struct {
	RunAt time.Time `json:"run_at" validate:"required"`
}

// ScheduleRunResponse is the response for POST /runs/{run_id}/schedule.
type ScheduleRunResponse struct {
	RunID       string    `json:"run_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// WaitForEventRequest is the body for POST /runs/{run_id}/wait-for-event.
type WaitForEventRequest struct {
	EventName      string `json:"event_name" validate:"required"`
	TimeoutSeconds int32  `json:"timeout_seconds" validate:"min=1"`
	TaskID         string `json:"task_id" validate:"required,uuid"`
	StepName       string `json:"step_name" validate:"required"`
}

// WaitForEventResponse covers all three outcomes (200 resolved, 202 sleeping, 408 timeout).
// The handler uses Status to determine the HTTP status code.
type WaitForEventResponse struct {
	Status  WaitResult      `json:"status"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Message string          `json:"message,omitempty"`
}
