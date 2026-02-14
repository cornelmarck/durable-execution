package apiv1

import (
	"encoding/json"
	"time"
)

// EmitEventRequest is the body for POST /events.
type EmitEventRequest struct {
	EventName string          `json:"event_name"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// EmitEventResponse is the response for POST /events.
type EmitEventResponse struct {
	EventID   string    `json:"event_id"`
	EventName string    `json:"event_name"`
	CreatedAt time.Time `json:"created_at"`
}
