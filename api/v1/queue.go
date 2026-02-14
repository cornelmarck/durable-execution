package apiv1

import "time"

// CleanupPolicy configures automatic background cleanup for completed tasks and events.
type CleanupPolicy struct {
	TaskTTLSeconds  *int32 `json:"task_ttl_seconds,omitempty"`
	EventTTLSeconds *int32 `json:"event_ttl_seconds,omitempty"`
}

// CreateQueueRequest is the body for POST /queues.
type CreateQueueRequest struct {
	Name    string         `json:"name" validate:"required"`
	Cleanup *CleanupPolicy `json:"cleanup,omitempty"`
}

// CreateQueueResponse is the response for POST /queues.
type CreateQueueResponse struct {
	Name      string         `json:"name"`
	CreatedAt time.Time      `json:"created_at"`
	Cleanup   *CleanupPolicy `json:"cleanup,omitempty"`
}
