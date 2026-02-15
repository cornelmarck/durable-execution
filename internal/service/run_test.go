package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

func TestRetryDelay(t *testing.T) {
	float64Ptr := func(f float64) *float64 { return &f }

	tests := []struct {
		name    string
		json    []byte
		attempt int32
		want    time.Duration
	}{
		{
			name:    "empty JSON returns default",
			json:    nil,
			attempt: 1,
			want:    defaultRetryDelay,
		},
		{
			name:    "invalid JSON returns default",
			json:    []byte(`{invalid`),
			attempt: 1,
			want:    defaultRetryDelay,
		},
		{
			name:    "fixed strategy",
			json:    mustJSON(apiv1.RetryStrategy{Kind: apiv1.RetryFixed, BaseSeconds: 5}),
			attempt: 3,
			want:    5 * time.Second,
		},
		{
			name:    "exponential default factor",
			json:    mustJSON(apiv1.RetryStrategy{Kind: apiv1.RetryExponential, BaseSeconds: 1}),
			attempt: 3,
			want:    4 * time.Second, // 1 * 2^(3-1) = 4
		},
		{
			name:    "exponential custom factor",
			json:    mustJSON(apiv1.RetryStrategy{Kind: apiv1.RetryExponential, BaseSeconds: 1, Factor: float64Ptr(3)}),
			attempt: 3,
			want:    9 * time.Second, // 1 * 3^(3-1) = 9
		},
		{
			name:    "exponential max cap",
			json:    mustJSON(apiv1.RetryStrategy{Kind: apiv1.RetryExponential, BaseSeconds: 1, MaxSeconds: float64Ptr(5)}),
			attempt: 10,
			want:    5 * time.Second,
		},
		{
			name:    "unknown kind uses base_seconds",
			json:    mustJSON(apiv1.RetryStrategy{Kind: "unknown", BaseSeconds: 7}),
			attempt: 1,
			want:    7 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryDelay(tt.json, tt.attempt)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFailRun_Retry(t *testing.T) {
	taskID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	runID := pgtype.UUID{Bytes: [16]byte{2}, Valid: true}
	nextRunID := pgtype.UUID{Bytes: [16]byte{3}, Valid: true}

	mock := &StoreMock{
		GetRunFunc: func(ctx context.Context, id pgtype.UUID) (dbgen.Run, error) {
			return dbgen.Run{
				ID:      runID,
				TaskID:  taskID,
				Attempt: 1,
			}, nil
		},
		GetTaskFunc: func(ctx context.Context, id pgtype.UUID) (dbgen.Task, error) {
			return dbgen.Task{
				ID:          taskID,
				MaxAttempts: 3,
			}, nil
		},
		FailRunFunc: func(ctx context.Context, arg dbgen.FailRunParams) error {
			return nil
		},
		CreateRunFunc: func(ctx context.Context, arg dbgen.CreateRunParams) (dbgen.Run, error) {
			assert.Equal(t, int32(2), arg.Attempt)
			assert.Equal(t, taskID, arg.TaskID)
			assert.True(t, arg.ScheduledAt.Valid)
			return dbgen.Run{ID: nextRunID}, nil
		},
	}
	mock.ExecTxFunc = func(ctx context.Context, fn func(dbgen.Querier) error) error {
		return fn(mock)
	}

	svc := New(mock)
	resp, err := svc.FailRun(context.Background(), "00000001-0000-0000-0000-000000000000", apiv1.FailRunRequest{Error: "boom"})
	require.NoError(t, err)
	assert.Equal(t, apiv1.RunStatusFailed, resp.Status)
	assert.NotNil(t, resp.NextRunID)
	assert.NotNil(t, resp.NextAttemptAt)
}

func TestFailRun_NoRetry(t *testing.T) {
	taskID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	runID := pgtype.UUID{Bytes: [16]byte{2}, Valid: true}

	mock := &StoreMock{
		GetRunFunc: func(ctx context.Context, id pgtype.UUID) (dbgen.Run, error) {
			return dbgen.Run{
				ID:      runID,
				TaskID:  taskID,
				Attempt: 3,
			}, nil
		},
		GetTaskFunc: func(ctx context.Context, id pgtype.UUID) (dbgen.Task, error) {
			return dbgen.Task{
				ID:          taskID,
				MaxAttempts: 3,
			}, nil
		},
		FailRunFunc: func(ctx context.Context, arg dbgen.FailRunParams) error {
			return nil
		},
		UpdateTaskStatusFunc: func(ctx context.Context, arg dbgen.UpdateTaskStatusParams) error {
			assert.Equal(t, dbgen.TaskStatusFailed, arg.Status)
			return nil
		},
	}
	mock.ExecTxFunc = func(ctx context.Context, fn func(dbgen.Querier) error) error {
		return fn(mock)
	}

	svc := New(mock)
	resp, err := svc.FailRun(context.Background(), "00000001-0000-0000-0000-000000000000", apiv1.FailRunRequest{Error: "boom"})
	require.NoError(t, err)
	assert.Equal(t, apiv1.RunStatusFailed, resp.Status)
	assert.Nil(t, resp.NextRunID)
	assert.Equal(t, 1, len(mock.UpdateTaskStatusCalls()))
}

func TestFailRun_NotFound(t *testing.T) {
	mock := &StoreMock{
		GetRunFunc: func(ctx context.Context, id pgtype.UUID) (dbgen.Run, error) {
			return dbgen.Run{}, db.ErrNotFound
		},
	}

	svc := New(mock)
	_, err := svc.FailRun(context.Background(), "00000001-0000-0000-0000-000000000000", apiv1.FailRunRequest{Error: "boom"})
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrNotFound)
}

func TestWaitForEvent_Resolved(t *testing.T) {
	payload := json.RawMessage(`{"key":"value"}`)

	mock := &StoreMock{
		GetEventByNameFunc: func(ctx context.Context, eventName string) (dbgen.Event, error) {
			return dbgen.Event{Payload: payload}, nil
		},
	}

	svc := New(mock)
	resp, err := svc.WaitForEvent(context.Background(), "00000001-0000-0000-0000-000000000000", apiv1.WaitForEventRequest{
		EventName:      "test-event",
		TimeoutSeconds: 30,
		TaskID:         "00000002-0000-0000-0000-000000000000",
		StepName:       "step1",
	})
	require.NoError(t, err)
	assert.Equal(t, apiv1.WaitResultResolved, resp.Status)
	assert.Equal(t, payload, resp.Payload)
}

func TestWaitForEvent_Sleeping(t *testing.T) {
	mock := &StoreMock{
		GetEventByNameFunc: func(ctx context.Context, eventName string) (dbgen.Event, error) {
			return dbgen.Event{}, db.ErrNotFound
		},
		SetRunSleepingFunc: func(ctx context.Context, arg dbgen.SetRunSleepingParams) error {
			assert.Equal(t, "test-event", arg.WaitingEventName.String)
			assert.Equal(t, "step1", arg.WaitingStepName.String)
			return nil
		},
	}

	svc := New(mock)
	resp, err := svc.WaitForEvent(context.Background(), "00000001-0000-0000-0000-000000000000", apiv1.WaitForEventRequest{
		EventName:      "test-event",
		TimeoutSeconds: 30,
		TaskID:         "00000002-0000-0000-0000-000000000000",
		StepName:       "step1",
	})
	require.NoError(t, err)
	assert.Equal(t, apiv1.WaitResultSleeping, resp.Status)
	assert.Equal(t, 1, len(mock.SetRunSleepingCalls()))
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
