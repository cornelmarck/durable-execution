package service

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	db "github.com/cornelmarck/durable-execution/internal/db"
	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
)

// fakeNotifier implements RunNotifier for tests using the broadcast-via-close pattern.
type fakeNotifier struct {
	mu     sync.Mutex
	queues map[string]chan struct{}
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{queues: make(map[string]chan struct{})}
}

func (n *fakeNotifier) Signal(queueName string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch, ok := n.queues[queueName]
	if !ok {
		ch = make(chan struct{})
		n.queues[queueName] = ch
	}
	return ch
}

func (n *fakeNotifier) Notify(queueName string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.queues[queueName]; ok {
		close(ch)
		n.queues[queueName] = make(chan struct{})
	}
}

func TestCreateQueue_Conflict(t *testing.T) {
	mock := &StoreMock{
		CreateQueueFunc: func(ctx context.Context, arg dbgen.CreateQueueParams) (dbgen.Queue, error) {
			return dbgen.Queue{}, db.ErrConflict
		},
	}

	svc := New(mock)
	_, err := svc.CreateQueue(context.Background(), apiv1.CreateQueueRequest{Name: "test-queue"})
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrConflict)
}

func TestSpawnTask_QueueNotFound(t *testing.T) {
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{}, db.ErrNotFound
		},
	}

	svc := New(mock)
	_, err := svc.SpawnTask(context.Background(), "missing-queue", apiv1.SpawnTaskRequest{TaskName: "test"})
	require.Error(t, err)
	assert.ErrorIs(t, err, db.ErrNotFound)
}

func TestClaimTasks_UnmarshalHeaders(t *testing.T) {
	headers := map[string]string{"X-Custom": "value"}
	headersJSON, _ := json.Marshal(headers)

	retryStrategy := apiv1.RetryStrategy{Kind: apiv1.RetryFixed, BaseSeconds: 5}
	retryJSON, _ := json.Marshal(retryStrategy)

	queueID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{ID: queueID}, nil
		},
		ClaimRunsFunc: func(ctx context.Context, arg dbgen.ClaimRunsParams) ([]dbgen.ClaimRunsRow, error) {
			return []dbgen.ClaimRunsRow{
				{
					ID:                pgtype.UUID{Bytes: [16]byte{2}, Valid: true},
					TaskID:            pgtype.UUID{Bytes: [16]byte{3}, Valid: true},
					Attempt:           1,
					TaskName:          "my-task",
					TaskParams:        []byte(`{"key":"val"}`),
					TaskHeaders:       headersJSON,
					TaskRetryStrategy: retryJSON,
					TaskMaxAttempts:   3,
				},
			}, nil
		},
	}

	svc := New(mock)
	resp, err := svc.ClaimTasks(context.Background(), "test-queue", apiv1.ClaimTasksRequest{Limit: 10, ClaimTimeout: 30})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)

	task := resp.Tasks[0]
	assert.Equal(t, "my-task", task.TaskName)
	assert.Equal(t, map[string]string{"X-Custom": "value"}, task.Headers)
	require.NotNil(t, task.RetryStrategy)
	assert.Equal(t, apiv1.RetryFixed, task.RetryStrategy.Kind)
	assert.Equal(t, float64(5), task.RetryStrategy.BaseSeconds)
}

func TestClaimTasks_NilRetryStrategy(t *testing.T) {
	queueID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{ID: queueID}, nil
		},
		ClaimRunsFunc: func(ctx context.Context, arg dbgen.ClaimRunsParams) ([]dbgen.ClaimRunsRow, error) {
			return []dbgen.ClaimRunsRow{
				{
					ID:                pgtype.UUID{Bytes: [16]byte{2}, Valid: true},
					TaskID:            pgtype.UUID{Bytes: [16]byte{3}, Valid: true},
					Attempt:           1,
					TaskName:          "my-task",
					TaskRetryStrategy: nil,
					TaskMaxAttempts:   1,
				},
			}, nil
		},
	}

	svc := New(mock)
	resp, err := svc.ClaimTasks(context.Background(), "test-queue", apiv1.ClaimTasksRequest{Limit: 1, ClaimTimeout: 30})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	assert.Nil(t, resp.Tasks[0].RetryStrategy)
}

func TestClaimTasks_LongPoll_NotifyWakes(t *testing.T) {
	queueID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	row := dbgen.ClaimRunsRow{
		ID:              pgtype.UUID{Bytes: [16]byte{2}, Valid: true},
		TaskID:          pgtype.UUID{Bytes: [16]byte{3}, Valid: true},
		Attempt:         1,
		TaskName:        "my-task",
		TaskMaxAttempts: 1,
	}

	var calls atomic.Int32
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{ID: queueID}, nil
		},
		ClaimRunsFunc: func(ctx context.Context, arg dbgen.ClaimRunsParams) ([]dbgen.ClaimRunsRow, error) {
			// Return empty on first two calls, then return a task.
			if calls.Add(1) <= 2 {
				return nil, nil
			}
			return []dbgen.ClaimRunsRow{row}, nil
		},
	}

	notifier := newFakeNotifier()
	svc := New(mock, WithNotifier(notifier))

	timeout := int32(5)
	done := make(chan struct{})
	var resp *apiv1.ClaimTasksResponse
	var err error

	go func() {
		resp, err = svc.ClaimTasks(context.Background(), "q", apiv1.ClaimTasksRequest{
			Limit: 1, ClaimTimeout: 30, LongPollSeconds: &timeout,
		})
		close(done)
	}()

	// Let the goroutine start waiting.
	time.Sleep(50 * time.Millisecond)
	notifier.Notify("q") // first notify — claim still returns empty
	time.Sleep(50 * time.Millisecond)
	notifier.Notify("q") // second notify — claim returns a task

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ClaimTasks did not return in time")
	}

	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	assert.Equal(t, "my-task", resp.Tasks[0].TaskName)
}

func TestClaimTasks_LongPoll_Timeout(t *testing.T) {
	queueID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{ID: queueID}, nil
		},
		ClaimRunsFunc: func(ctx context.Context, arg dbgen.ClaimRunsParams) ([]dbgen.ClaimRunsRow, error) {
			return nil, nil
		},
	}

	notifier := newFakeNotifier()
	svc := New(mock, WithNotifier(notifier))

	timeout := int32(1) // 1 second timeout
	start := time.Now()
	resp, err := svc.ClaimTasks(context.Background(), "q", apiv1.ClaimTasksRequest{
		Limit: 1, ClaimTimeout: 30, LongPollSeconds: &timeout,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, resp.Tasks)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should wait ~1s")
}

func TestClaimTasks_LongPoll_ContextCancel(t *testing.T) {
	queueID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{ID: queueID}, nil
		},
		ClaimRunsFunc: func(ctx context.Context, arg dbgen.ClaimRunsParams) ([]dbgen.ClaimRunsRow, error) {
			return nil, nil
		},
	}

	notifier := newFakeNotifier()
	svc := New(mock, WithNotifier(notifier))

	ctx, cancel := context.WithCancel(context.Background())
	timeout := int32(30)

	done := make(chan struct{})
	var err error
	go func() {
		_, err = svc.ClaimTasks(ctx, "q", apiv1.ClaimTasksRequest{
			Limit: 1, ClaimTimeout: 30, LongPollSeconds: &timeout,
		})
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ClaimTasks did not return after context cancel")
	}

	assert.ErrorIs(t, err, context.Canceled)
}

func TestClaimTasks_LongPoll_NoNotifier(t *testing.T) {
	// Without a notifier, wait_timeout_seconds is ignored.
	queueID := pgtype.UUID{Bytes: [16]byte{1}, Valid: true}
	mock := &StoreMock{
		GetQueueByNameFunc: func(ctx context.Context, name string) (dbgen.Queue, error) {
			return dbgen.Queue{ID: queueID}, nil
		},
		ClaimRunsFunc: func(ctx context.Context, arg dbgen.ClaimRunsParams) ([]dbgen.ClaimRunsRow, error) {
			return nil, nil
		},
	}

	svc := New(mock) // no notifier
	timeout := int32(5)
	start := time.Now()
	resp, err := svc.ClaimTasks(context.Background(), "q", apiv1.ClaimTasksRequest{
		Limit: 1, ClaimTimeout: 30, LongPollSeconds: &timeout,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, resp.Tasks)
	assert.Less(t, elapsed, 100*time.Millisecond, "should return immediately without notifier")
}
