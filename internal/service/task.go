package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

const (
	defaultMaxAttempts = int32(1)
	defaultListLimit   = int32(50)
)

func (s *Service) CreateTask(ctx context.Context, queueName string, req apiv1.CreateTaskRequest) (*apiv1.CreateTaskResponse, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("queue %q: %w", queueName, db.ErrNotFound)
		}
		return nil, err
	}

	var params, headers, retryStrategy []byte
	if req.Params != nil {
		params, _ = json.Marshal(req.Params)
	}
	if req.Headers != nil {
		headers, _ = json.Marshal(req.Headers)
	}
	if req.RetryStrategy != nil {
		retryStrategy, _ = json.Marshal(req.RetryStrategy)
	}

	maxAttempts := defaultMaxAttempts
	if req.MaxAttempts != nil {
		maxAttempts = *req.MaxAttempts
	}
	startTimeout := int32(0)
	if req.StartTimeout != nil {
		startTimeout = *req.StartTimeout
	}
	executionTimeout := int32(0)
	if req.ExecutionTimeout != nil {
		executionTimeout = *req.ExecutionTimeout
	}

	var workflowRunID pgtype.UUID
	if req.WorkflowRunID != nil {
		wid, err := parseUUID(*req.WorkflowRunID)
		if err != nil {
			return nil, fmt.Errorf("invalid workflow_run_id: %w", ErrBadRequest)
		}
		workflowRunID = wid
	}

	var taskID pgtype.UUID
	var runID pgtype.UUID

	err = s.store.ExecTx(ctx, func(q dbgen.Querier) error {
		var txErr error
		taskID, txErr = q.CreateTask(ctx, dbgen.CreateTaskParams{
			QueueID:          queue.ID,
			TaskName:         req.TaskName,
			Params:           params,
			Headers:          headers,
			RetryStrategy:    retryStrategy,
			MaxAttempts:      maxAttempts,
			StartTimeout:     startTimeout,
			ExecutionTimeout: executionTimeout,
			WorkflowRunID:    workflowRunID,
		})
		if txErr != nil {
			return txErr
		}

		run, txErr := q.CreateRun(ctx, dbgen.CreateRunParams{
			TaskID:  taskID,
			Attempt: 1,
		})
		if txErr != nil {
			return txErr
		}
		runID = run.ID
		return nil
	})
	if err != nil {
		return nil, err
	}

	resp := &apiv1.CreateTaskResponse{
		TaskID: uuidString(taskID),
		RunID:  uuidString(runID),
	}
	if req.WorkflowRunID != nil {
		resp.WorkflowRunID = req.WorkflowRunID
	}
	return resp, nil
}

func (s *Service) ClaimTasks(ctx context.Context, queueName string, req apiv1.ClaimTasksRequest) (*apiv1.ClaimTasksResponse, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("queue %q: %w", queueName, db.ErrNotFound)
		}
		return nil, err
	}

	claimParams := dbgen.ClaimRunsParams{
		QueueID:             queue.ID,
		Limit:               req.Limit,
		ClaimTimeoutSeconds: req.ClaimTimeout,
	}

	rows, err := s.store.ClaimRuns(ctx, claimParams)
	if err != nil {
		return nil, err
	}

	// Long-poll: wait for runs if none are immediately available.
	if len(rows) == 0 && s.notifier != nil && req.LongPollSeconds != nil && *req.LongPollSeconds > 0 {
		deadline := time.After(time.Duration(*req.LongPollSeconds) * time.Second)
		for len(rows) == 0 {
			// Obtain the signal channel before claiming so we don't miss
			// notifications that arrive between the claim and the wait.
			sig := s.notifier.Signal(queueName)

			rows, err = s.store.ClaimRuns(ctx, claimParams)
			if err != nil {
				return nil, err
			}
			if len(rows) > 0 {
				break
			}

			select {
			case <-sig:
				continue
			case <-deadline:
				// Timeout elapsed; return empty response.
				rows = nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			if rows == nil {
				break
			}
		}
	}

	tasks := make([]apiv1.ClaimedTask, 0, len(rows))
	for _, r := range rows {
		ct := apiv1.ClaimedTask{
			RunID:       uuidString(r.ID),
			TaskID:      uuidString(r.TaskID),
			Attempt:     r.Attempt,
			TaskName:    r.TaskName,
			Params:      r.TaskParams,
			MaxAttempts: r.TaskMaxAttempts,
		}

		if len(r.TaskHeaders) > 0 {
			var hdrs map[string]string
			if err := json.Unmarshal(r.TaskHeaders, &hdrs); err == nil {
				ct.Headers = hdrs
			}
		}

		if len(r.TaskRetryStrategy) > 0 {
			var rs apiv1.RetryStrategy
			if err := json.Unmarshal(r.TaskRetryStrategy, &rs); err == nil {
				ct.RetryStrategy = &rs
			}
		}

		tasks = append(tasks, ct)
	}

	return &apiv1.ClaimTasksResponse{Tasks: tasks}, nil
}

func (s *Service) ListTasks(ctx context.Context, queueName, status, taskName, cursor *string, limit int32) (*apiv1.ListTasksResponse, error) {
	if limit < 0 {
		return nil, fmt.Errorf("limit must be non-negative: %w", ErrBadRequest)
	}
	if limit == 0 {
		return &apiv1.ListTasksResponse{Tasks: []apiv1.TaskSummary{}}, nil
	}

	var statusFilter dbgen.NullTaskStatus
	if status != nil {
		statusFilter = dbgen.NullTaskStatus{TaskStatus: dbgen.TaskStatus(*status), Valid: true}
	}

	params := dbgen.ListTasksParams{
		QueueName: toText(queueName),
		Status:    statusFilter,
		TaskName:  toText(taskName),
		Lim:       limit + 1, // fetch one extra to detect next page
	}

	if cursor != nil {
		cursorID, err := parseUUID(*cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", ErrBadRequest)
		}
		params.CursorID = cursorID
	}

	rows, err := s.store.ListTasks(ctx, params)
	if err != nil {
		return nil, err
	}

	var nextCursor *string
	if int32(len(rows)) > limit {
		rows = rows[:limit]
		last := rows[limit-1]
		c := uuidString(last.ID)
		nextCursor = &c
	}

	tasks := make([]apiv1.TaskSummary, 0, len(rows))
	for _, r := range rows {
		t := apiv1.TaskSummary{
			ID:          uuidString(r.ID),
			TaskName:    r.TaskName,
			Status:      apiv1.TaskStatus(r.Status),
			QueueName:   r.QueueName,
			MaxAttempts: r.MaxAttempts,
			CreatedAt:   r.CreatedAt.Time.Format(time.RFC3339),
		}
		if r.CompletedAt.Valid {
			s := r.CompletedAt.Time.Format(time.RFC3339)
			t.CompletedAt = &s
		}
		tasks = append(tasks, t)
	}

	return &apiv1.ListTasksResponse{Tasks: tasks, NextCursor: nextCursor}, nil
}
