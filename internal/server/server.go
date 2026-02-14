package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	"github.com/go-playground/validator/v10"
)

// Service defines the domain operations that API handlers delegate to.
// Implementations contain the business logic and interact with the store.
type Service interface {
	// Queues
	CreateQueue(ctx context.Context, req apiv1.CreateQueueRequest) (*apiv1.CreateQueueResponse, error)

	// Tasks
	SpawnTask(ctx context.Context, queueName string, req apiv1.SpawnTaskRequest) (*apiv1.SpawnTaskResponse, error)
	ClaimTasks(ctx context.Context, queueName string, req apiv1.ClaimTasksRequest) (*apiv1.ClaimTasksResponse, error)

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

// writeError maps an error to an HTTP response. If the error is an *apiv1.Error,
// its status and code are used; otherwise a 500 is returned.
func writeError(w http.ResponseWriter, err error) {
	var apiErr *apiv1.Error
	if errors.As(err, &apiErr) {
		writeJSON(w, apiErr.Status, apiv1.ErrorResponse{
			Error: apiErr.Message,
			Code:  apiErr.Code,
		})
		return
	}
	slog.Error("internal error", "error", err)
	writeJSON(w, http.StatusInternalServerError, apiv1.ErrorResponse{
		Error: "internal error",
		Code:  "INTERNAL_ERROR",
	})
}

var validate = validator.New()

// decodeAndValidate reads the request body as JSON into a new T, then validates
// struct tags. Returns the zero value and false (writing a 400) on failure.
func decodeAndValidate[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		writeError(w, apiv1.ErrBadRequest("invalid JSON: "+err.Error()))
		return v, false
	}
	if err := validate.Struct(v); err != nil {
		writeError(w, apiv1.ErrBadRequest(err.Error()))
		return v, false
	}
	return v, true
}
