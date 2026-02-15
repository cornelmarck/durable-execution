package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
)

func addRoutes(mux *http.ServeMux, svc Service) {
	// Queues
	mux.Handle("GET /api/v1/queues", handleListQueues(svc))
	mux.Handle("POST /api/v1/queues", handleCreateQueue(svc))
	mux.Handle("DELETE /api/v1/queues/{queue_name}", handleDeleteQueue(svc))
	mux.Handle("GET /api/v1/queues/{queue_name}/stats", handleGetQueueStats(svc))

	// Tasks
	mux.Handle("GET /api/v1/tasks", handleListTasks(svc))
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
		req, ok := decodeAndValidate[apiv1.CreateQueueRequest](w, r)
		if !ok {
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

func handleListTasks(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		var queueName, status, taskName, cursor *string
		if v := q.Get("queue_name"); v != "" {
			queueName = &v
		}
		if v := q.Get("status"); v != "" {
			status = &v
		}
		if v := q.Get("task_name"); v != "" {
			taskName = &v
		}
		if v := q.Get("cursor"); v != "" {
			cursor = &v
		}

		var limit int32
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				writeBadRequest(w, "limit must be a positive integer")
				return
			}
			limit = int32(n)
		}

		resp, err := svc.ListTasks(r.Context(), queueName, status, taskName, cursor, limit)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleSpawnTask(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue_name")

		req, ok := decodeAndValidate[apiv1.SpawnTaskRequest](w, r)
		if !ok {
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

		req, ok := decodeAndValidate[apiv1.ClaimTasksRequest](w, r)
		if !ok {
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
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeBadRequest(w, "invalid JSON: "+err.Error())
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

		req, ok := decodeAndValidate[apiv1.FailRunRequest](w, r)
		if !ok {
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

		req, ok := decodeAndValidate[apiv1.ScheduleRunRequest](w, r)
		if !ok {
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

		req, ok := decodeAndValidate[apiv1.WaitForEventRequest](w, r)
		if !ok {
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

		req, ok := decodeAndValidate[apiv1.SetCheckpointRequest](w, r)
		if !ok {
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
		req, ok := decodeAndValidate[apiv1.EmitEventRequest](w, r)
		if !ok {
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
		req, ok := decodeAndValidate[apiv1.CreateWorkflowRunRequest](w, r)
		if !ok {
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

		req, ok := decodeAndValidate[apiv1.UpdateWorkflowRunRequest](w, r)
		if !ok {
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

func handleListQueues(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := svc.ListQueues(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func handleDeleteQueue(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue_name")

		if err := svc.DeleteQueue(r.Context(), queueName); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func handleGetQueueStats(svc Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queueName := r.PathValue("queue_name")

		resp, err := svc.GetQueueStats(r.Context(), queueName)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
}
