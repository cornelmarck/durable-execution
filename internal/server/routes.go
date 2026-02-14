package server

import (
	"net/http"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
)

func addRoutes(mux *http.ServeMux, svc Service) {
	// Queues
	mux.Handle("POST /api/v1/queues", handleCreateQueue(svc))

	// Tasks
	mux.Handle("POST /api/v1/queues/{queue_name}/tasks", handleSpawnTask(svc))
	mux.Handle("POST /api/v1/queues/{queue_name}/tasks/claim", handleClaimTasks(svc))

	// Runs
	mux.Handle("POST /api/v1/runs/{run_id}/complete", handleCompleteRun(svc))
	mux.Handle("POST /api/v1/runs/{run_id}/fail", handleFailRun(svc))
	mux.Handle("POST /api/v1/runs/{run_id}/schedule", handleScheduleRun(svc))
	mux.Handle("POST /api/v1/runs/{run_id}/wait-for-event", handleWaitForEvent(svc))

	// Checkpoints
	mux.Handle("PUT /api/v1/tasks/{task_id}/checkpoints/{step_name}", handleSetCheckpoint(svc))
	mux.Handle("GET /api/v1/tasks/{task_id}/checkpoints/{step_name}", handleGetCheckpoint(svc))

	// Events
	mux.Handle("POST /api/v1/events", handleEmitEvent(svc))

	// Workflow Runs
	mux.Handle("POST /api/v1/workflow-runs", handleCreateWorkflowRun(svc))
	mux.Handle("PATCH /api/v1/workflow-runs/{workflow_run_id}", handleUpdateWorkflowRun(svc))
}

func handleCreateQueue(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req apiv1.CreateQueueRequest
		if !decode(w, r, &req) {
			return
		}
		if req.Name == "" {
			writeError(w, apiv1.ErrBadRequest("name is required"))
			return
		}

		resp, err := svc.CreateQueue(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})
}

func handleSpawnTask(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue_name")

		var req apiv1.SpawnTaskRequest
		if !decode(w, r, &req) {
			return
		}
		if req.TaskName == "" {
			writeError(w, apiv1.ErrBadRequest("task_name is required"))
			return
		}

		resp, err := svc.SpawnTask(r.Context(), queueName, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})
}

func handleClaimTasks(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue_name")

		var req apiv1.ClaimTasksRequest
		if !decode(w, r, &req) {
			return
		}
		if req.Qty < 1 {
			writeError(w, apiv1.ErrBadRequest("qty must be at least 1"))
			return
		}
		if req.ClaimTimeout < 1 {
			writeError(w, apiv1.ErrBadRequest("claim_timeout must be at least 1"))
			return
		}

		resp, err := svc.ClaimTasks(r.Context(), queueName, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleCompleteRun(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")

		var req apiv1.CompleteRunRequest
		if r.ContentLength > 0 {
			if !decode(w, r, &req) {
				return
			}
		}

		resp, err := svc.CompleteRun(r.Context(), runID, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleFailRun(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")

		var req apiv1.FailRunRequest
		if !decode(w, r, &req) {
			return
		}
		if req.Error == "" {
			writeError(w, apiv1.ErrBadRequest("error is required"))
			return
		}

		resp, err := svc.FailRun(r.Context(), runID, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleScheduleRun(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")

		var req apiv1.ScheduleRunRequest
		if !decode(w, r, &req) {
			return
		}
		if req.RunAt.IsZero() {
			writeError(w, apiv1.ErrBadRequest("run_at is required"))
			return
		}

		resp, err := svc.ScheduleRun(r.Context(), runID, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleWaitForEvent(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runID := r.PathValue("run_id")

		var req apiv1.WaitForEventRequest
		if !decode(w, r, &req) {
			return
		}
		if req.EventName == "" {
			writeError(w, apiv1.ErrBadRequest("event_name is required"))
			return
		}
		if req.TimeoutSeconds < 1 {
			writeError(w, apiv1.ErrBadRequest("timeout_seconds must be at least 1"))
			return
		}
		if req.TaskID == "" {
			writeError(w, apiv1.ErrBadRequest("task_id is required"))
			return
		}
		if req.StepName == "" {
			writeError(w, apiv1.ErrBadRequest("step_name is required"))
			return
		}

		resp, err := svc.WaitForEvent(r.Context(), runID, req)
		if err != nil {
			writeError(w, err)
			return
		}

		status := http.StatusOK
		switch resp.Status {
		case apiv1.WaitResultSleeping:
			status = http.StatusAccepted
		case apiv1.WaitResultTimeout:
			status = http.StatusRequestTimeout
		}
		writeJSON(w, status, resp)
	})
}

func handleSetCheckpoint(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		taskID := r.PathValue("task_id")
		stepName := r.PathValue("step_name")

		var req apiv1.SetCheckpointRequest
		if !decode(w, r, &req) {
			return
		}
		if req.State == nil {
			writeError(w, apiv1.ErrBadRequest("state is required"))
			return
		}
		if req.OwnerRun == "" {
			writeError(w, apiv1.ErrBadRequest("owner_run is required"))
			return
		}

		resp, err := svc.SetCheckpoint(r.Context(), taskID, stepName, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleGetCheckpoint(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		taskID := r.PathValue("task_id")
		stepName := r.PathValue("step_name")

		resp, err := svc.GetCheckpoint(r.Context(), taskID, stepName)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleEmitEvent(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req apiv1.EmitEventRequest
		if !decode(w, r, &req) {
			return
		}
		if req.EventName == "" {
			writeError(w, apiv1.ErrBadRequest("event_name is required"))
			return
		}

		resp, err := svc.EmitEvent(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})
}

func handleCreateWorkflowRun(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req apiv1.CreateWorkflowRunRequest
		if !decode(w, r, &req) {
			return
		}
		if req.WorkflowName == "" {
			writeError(w, apiv1.ErrBadRequest("workflow_name is required"))
			return
		}

		resp, err := svc.CreateWorkflowRun(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})
}

func handleUpdateWorkflowRun(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workflowRunID := r.PathValue("workflow_run_id")

		var req apiv1.UpdateWorkflowRunRequest
		if !decode(w, r, &req) {
			return
		}

		resp, err := svc.UpdateWorkflowRun(r.Context(), workflowRunID, req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}
