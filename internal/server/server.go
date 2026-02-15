package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	"github.com/cornelmarck/durable-execution/internal/service"
	"github.com/go-playground/validator/v10"
)

// Service defines the domain operations that API handlers delegate to.
// Implementations contain the business logic and interact with the store.
type Service interface {
	// Queues
	CreateQueue(ctx context.Context, req apiv1.CreateQueueRequest) (*apiv1.CreateQueueResponse, error)
	ListQueues(ctx context.Context) (*apiv1.ListQueuesResponse, error)
	DeleteQueue(ctx context.Context, queueName string) error
	GetQueueStats(ctx context.Context, queueName string) (*apiv1.QueueStatsResponse, error)

	// Tasks
	SpawnTask(ctx context.Context, queueName string, req apiv1.SpawnTaskRequest) (*apiv1.SpawnTaskResponse, error)
	ClaimTasks(ctx context.Context, queueName string, req apiv1.ClaimTasksRequest) (*apiv1.ClaimTasksResponse, error)
	ListTasks(ctx context.Context, queueName, status, taskName, cursor *string, limit int32) (*apiv1.ListTasksResponse, error)

	// Runs
	CompleteRun(ctx context.Context, runID string, req apiv1.CompleteRunRequest) (*apiv1.CompleteRunResponse, error)
	FailRun(ctx context.Context, runID string, req apiv1.FailRunRequest) (*apiv1.FailRunResponse, error)
	ScheduleRun(ctx context.Context, runID string, req apiv1.ScheduleRunRequest) (*apiv1.ScheduleRunResponse, error)
	WaitForEvent(ctx context.Context, runID string, req apiv1.WaitForEventRequest) (*apiv1.WaitForEventResponse, error)

	// Checkpoints
	SetCheckpoint(ctx context.Context, taskID, stepName string, req apiv1.SetCheckpointRequest) (*apiv1.SetCheckpointResponse, error)
	GetCheckpoint(ctx context.Context, taskID, stepName string) (*apiv1.GetCheckpointResponse, error)

	// Events
	EmitEvent(ctx context.Context, req apiv1.EmitEventRequest) (*apiv1.EmitEventResponse, error)

	// Workflow Runs
	CreateWorkflowRun(ctx context.Context, req apiv1.CreateWorkflowRunRequest) (*apiv1.CreateWorkflowRunResponse, error)
	UpdateWorkflowRun(ctx context.Context, workflowRunID string, req apiv1.UpdateWorkflowRunRequest) (*apiv1.UpdateWorkflowRunResponse, error)
}

// NewServer creates an http.Handler with all API v1 routes registered.
func NewServer(svc Service) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, svc)
	return logMiddleware(mux)
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError maps a service error to an HTTP response. Domain errors are mapped
// to appropriate status codes; everything else is logged and returned as 500.
func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrBadRequest):
		writeJSON(w, http.StatusBadRequest, apiv1.ErrorResponse{Error: err.Error(), Code: "VALIDATION_ERROR"})
	case errors.Is(err, db.ErrNotFound):
		writeJSON(w, http.StatusNotFound, apiv1.ErrorResponse{Error: err.Error(), Code: "NOT_FOUND"})
	case errors.Is(err, db.ErrConflict):
		writeJSON(w, http.StatusConflict, apiv1.ErrorResponse{Error: err.Error(), Code: "CONFLICT"})
	default:
		slog.Error("internal error", "error", err)
		writeJSON(w, http.StatusInternalServerError, apiv1.ErrorResponse{Error: "internal error", Code: "INTERNAL_ERROR"})
	}
}

// writeBadRequest writes a 400 response for request-level validation errors
// (malformed JSON, missing fields) that originate in the handler, not the service.
func writeBadRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, apiv1.ErrorResponse{Error: msg, Code: "VALIDATION_ERROR"})
}

var validate = validator.New()

// decodeAndValidate reads the request body as JSON into a new T, then validates
// struct tags. Returns the zero value and false (writing a 400) on failure.
func decodeAndValidate[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeBadRequest(w, "invalid JSON: "+err.Error())
		return v, false
	}
	if err := validate.Struct(v); err != nil {
		writeBadRequest(w, err.Error())
		return v, false
	}
	return v, true
}
