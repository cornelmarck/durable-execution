package apiv1

import (
	"encoding/json"
	"time"
)

// SetCheckpointRequest is the body for PUT /tasks/{task_id}/checkpoints/{step_name}.
type SetCheckpointRequest struct {
	State         json.RawMessage `json:"state"`
	OwnerRun      string          `json:"owner_run"`
	ExtendClaimBy *int32          `json:"extend_claim_by,omitempty"`
}

// SetCheckpointResponse is the response for PUT /tasks/{task_id}/checkpoints/{step_name}.
type SetCheckpointResponse struct {
	TaskID    string    `json:"task_id"`
	StepName  string    `json:"step_name"`
	UpdatedAt time.Time `json:"updated_at"`
}

// GetCheckpointResponse is the response for GET /tasks/{task_id}/checkpoints/{step_name}.
type GetCheckpointResponse struct {
	TaskID    string          `json:"task_id"`
	StepName  string          `json:"step_name"`
	State     json.RawMessage `json:"state"`
	UpdatedAt time.Time       `json:"updated_at"`
}
