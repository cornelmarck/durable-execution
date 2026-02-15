package apiv1

import "encoding/json"

// TaskStatus represents the status of a task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCanceled TaskStatus = "canceled"
)

// RetryStrategyKind identifies the retry back-off algorithm.
type RetryStrategyKind string

const (
	RetryFixed       RetryStrategyKind = "fixed"
	RetryExponential RetryStrategyKind = "exponential"
)

// RetryStrategy configures the delay between retries.
type RetryStrategy struct {
	Kind        RetryStrategyKind `json:"kind"`
	BaseSeconds float64           `json:"base_seconds"`
	Factor      *float64          `json:"factor,omitempty"`
	MaxSeconds  *float64          `json:"max_seconds,omitempty"`
}

// ClaimedTask is a single task returned from a claim operation.
type ClaimedTask struct {
	RunID         string            `json:"run_id"`
	TaskID        string            `json:"task_id"`
	Attempt       int32             `json:"attempt"`
	TaskName      string            `json:"task_name"`
	Params        json.RawMessage   `json:"params,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	RetryStrategy *RetryStrategy    `json:"retry_strategy,omitempty"`
	MaxAttempts   int32             `json:"max_attempts"`
}

// CreateTaskRequest is the body for POST /queues/{queue_name}/tasks.
type CreateTaskRequest struct {
	TaskName         string            `json:"task_name" validate:"required"`
	Params           json.RawMessage   `json:"params,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	RetryStrategy    *RetryStrategy    `json:"retry_strategy,omitempty"`
	MaxAttempts      *int32            `json:"max_attempts,omitempty"`
	StartTimeout     *int32            `json:"start_timeout,omitempty"`
	ExecutionTimeout *int32            `json:"execution_timeout,omitempty"`
	WorkflowRunID    *string           `json:"workflow_run_id,omitempty" validate:"omitempty,uuid"`
}

// CreateTaskResponse is the response for POST /queues/{queue_name}/tasks.
type CreateTaskResponse struct {
	TaskID        string  `json:"task_id"`
	RunID         string  `json:"run_id"`
	WorkflowRunID *string `json:"workflow_run_id,omitempty"`
}

// ClaimTasksRequest is the body for POST /queues/{queue_name}/tasks/claim.
type ClaimTasksRequest struct {
	Limit              int32  `json:"limit" validate:"min=1"`
	ClaimTimeout       int32  `json:"claim_timeout" validate:"min=1"`
	LongPollSeconds *int32 `json:"long_poll_seconds,omitempty" validate:"omitempty,min=5,max=30"`
}

// ClaimTasksResponse is the response for POST /queues/{queue_name}/tasks/claim.
type ClaimTasksResponse struct {
	Tasks []ClaimedTask `json:"tasks"`
}

// TaskSummary is a task returned from the list endpoint.
type TaskSummary struct {
	ID          string     `json:"id"`
	TaskName    string     `json:"task_name"`
	Status      TaskStatus `json:"status"`
	QueueName   string     `json:"queue_name"`
	MaxAttempts int32      `json:"max_attempts"`
	CreatedAt   string     `json:"created_at"`
	CompletedAt *string    `json:"completed_at,omitempty"`
}

// ListTasksResponse is the response for GET /tasks.
type ListTasksResponse struct {
	Tasks      []TaskSummary `json:"tasks"`
	NextCursor *string       `json:"next_cursor,omitempty"`
}
