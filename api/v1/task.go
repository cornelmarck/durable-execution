package apiv1

import "encoding/json"

// RetryStrategy configures the delay between retries.
type RetryStrategy struct {
	Kind        string   `json:"kind"`
	BaseSeconds float64  `json:"base_seconds"`
	Factor      *float64 `json:"factor,omitempty"`
	MaxSeconds  *float64 `json:"max_seconds,omitempty"`
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

// SpawnTaskRequest is the body for POST /queues/{queue_name}/tasks.
type SpawnTaskRequest struct {
	TaskName         string            `json:"task_name"`
	Params           json.RawMessage   `json:"params,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	RetryStrategy    *RetryStrategy    `json:"retry_strategy,omitempty"`
	MaxAttempts      *int32            `json:"max_attempts,omitempty"`
	StartTimeout     *int32            `json:"start_timeout,omitempty"`
	ExecutionTimeout *int32            `json:"execution_timeout,omitempty"`
	WorkflowRunID    *string           `json:"workflow_run_id,omitempty"`
}

// SpawnTaskResponse is the response for POST /queues/{queue_name}/tasks.
type SpawnTaskResponse struct {
	TaskID        string  `json:"task_id"`
	RunID         string  `json:"run_id"`
	WorkflowRunID *string `json:"workflow_run_id,omitempty"`
}

// ClaimTasksRequest is the body for POST /queues/{queue_name}/tasks/claim.
type ClaimTasksRequest struct {
	Qty          int32 `json:"qty"`
	ClaimTimeout int32 `json:"claim_timeout"`
}

// ClaimTasksResponse is the response for POST /queues/{queue_name}/tasks/claim.
type ClaimTasksResponse struct {
	Tasks []ClaimedTask `json:"tasks"`
}
