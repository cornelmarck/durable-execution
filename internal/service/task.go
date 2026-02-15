package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

func (s *Service) SpawnTask(ctx context.Context, queueName string, req apiv1.SpawnTaskRequest) (*apiv1.SpawnTaskResponse, error) {
	queue, err := s.store.GetQueueByName(ctx, queueName)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, fmt.Errorf("queue %q: %w", queueName, db.ErrNotFound)
		}
		return nil, err
	}

	params, _ := json.Marshal(req.Params)
	headers, _ := json.Marshal(req.Headers)
	retryStrategy, _ := json.Marshal(req.RetryStrategy)

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

	resp := &apiv1.SpawnTaskResponse{
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

	rows, err := s.store.ClaimRuns(ctx, dbgen.ClaimRunsParams{
		QueueID:             queue.ID,
		Limit:               req.Limit,
		ClaimTimeoutSeconds: req.ClaimTimeout,
	})
	if err != nil {
		return nil, err
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
	if limit <= 0 {
		limit = defaultListLimit
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
		cursorCreatedAt, cursorID, err := decodeCursor(*cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", ErrBadRequest)
		}
		params.CursorCreatedAt = cursorCreatedAt
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
		c := encodeCursor(last.CreatedAt, last.ID)
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

func encodeCursor(createdAt pgtype.Timestamptz, id pgtype.UUID) string {
	return base64.StdEncoding.EncodeToString(
		[]byte(createdAt.Time.Format(time.RFC3339Nano) + "|" + uuidString(id)),
	)
}

func decodeCursor(cursor string) (pgtype.Timestamptz, pgtype.UUID, error) {
	b, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return pgtype.Timestamptz{}, pgtype.UUID{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return pgtype.Timestamptz{}, pgtype.UUID{}, fmt.Errorf("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return pgtype.Timestamptz{}, pgtype.UUID{}, err
	}
	id, err := parseUUID(parts[1])
	if err != nil {
		return pgtype.Timestamptz{}, pgtype.UUID{}, err
	}
	return pgtype.Timestamptz{Time: t, Valid: true}, id, nil
}
