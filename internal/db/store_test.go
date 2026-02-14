package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func init() {
	sql.Register("postgres", stdlib.GetDefaultDriver())
}

var (
	testPool      *pgxpool.Pool
	testContainer *postgres.PostgresContainer
)

func migrationScripts() []string {
	files, err := filepath.Glob("migrations/*.up.sql")
	if err != nil {
		log.Fatalf("glob migrations: %v", err)
	}
	sort.Strings(files)
	return files
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithInitScripts(migrationScripts()...),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("start postgres container: %v", err)
	}
	testContainer = container

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	testPool = pool

	if err := container.Snapshot(ctx); err != nil {
		log.Fatalf("snapshot: %v", err)
	}

	code := m.Run()

	pool.Close()
	if err := container.Terminate(ctx); err != nil {
		log.Fatalf("terminate container: %v", err)
	}
	os.Exit(code)
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	err := testContainer.Restore(ctx)
	require.NoError(t, err)
	testPool.Reset()
	return NewStore(testPool)
}

// helpers

func validText(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: true}
}

func validTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func createTestQueue(t *testing.T, s *Store, name string) dbgen.Queue {
	t.Helper()
	q, err := s.CreateQueue(context.Background(), dbgen.CreateQueueParams{
		Name:            name,
		TaskTtlSeconds:  3600,
		EventTtlSeconds: 3600,
	})
	require.NoError(t, err)
	return q
}

func createTestTask(t *testing.T, s *Store, queueID pgtype.UUID, name string) pgtype.UUID {
	t.Helper()
	id, err := s.CreateTask(context.Background(), dbgen.CreateTaskParams{
		QueueID:      queueID,
		TaskName:     name,
		Params:       []byte(`{"key":"value"}`),
		MaxAttempts:  3,
		StartTimeout: 30,
	})
	require.NoError(t, err)
	return id
}

func createTestRun(t *testing.T, s *Store, taskID pgtype.UUID) dbgen.Run {
	t.Helper()
	run, err := s.CreateRun(context.Background(), dbgen.CreateRunParams{
		TaskID:  taskID,
		Attempt: 1,
	})
	require.NoError(t, err)
	return run
}

// --- Tests ---

func TestQueues(t *testing.T) {
	t.Run("create and get by name", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		q, err := s.CreateQueue(ctx, dbgen.CreateQueueParams{
			Name:            "test-queue",
			TaskTtlSeconds:  7200,
			EventTtlSeconds: 3600,
		})
		require.NoError(t, err)
		assert.True(t, q.ID.Valid)
		assert.Equal(t, "test-queue", q.Name)
		assert.Equal(t, int32(7200), q.TaskTtlSeconds)
		assert.Equal(t, int32(3600), q.EventTtlSeconds)
		assert.True(t, q.CreatedAt.Valid)

		got, err := s.GetQueueByName(ctx, "test-queue")
		require.NoError(t, err)
		assert.Equal(t, q.ID, got.ID)
		assert.Equal(t, q.Name, got.Name)
		assert.Equal(t, q.TaskTtlSeconds, got.TaskTtlSeconds)
		assert.Equal(t, q.EventTtlSeconds, got.EventTtlSeconds)
	})

	t.Run("duplicate name returns error", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		_, err := s.CreateQueue(ctx, dbgen.CreateQueueParams{
			Name:            "dup-queue",
			TaskTtlSeconds:  3600,
			EventTtlSeconds: 3600,
		})
		require.NoError(t, err)

		_, err = s.CreateQueue(ctx, dbgen.CreateQueueParams{
			Name:            "dup-queue",
			TaskTtlSeconds:  3600,
			EventTtlSeconds: 3600,
		})
		require.Error(t, err)
	})
}

func TestTasks(t *testing.T) {
	t.Run("create and get by ID", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "task-queue")

		params := []byte(`{"input":"hello"}`)
		headers := []byte(`{"x-trace":"abc"}`)

		taskID, err := s.CreateTask(ctx, dbgen.CreateTaskParams{
			QueueID:          q.ID,
			TaskName:         "my-task",
			Params:           params,
			Headers:          headers,
			MaxAttempts:      5,
			StartTimeout:     60,
			ExecutionTimeout: 300,
		})
		require.NoError(t, err)
		assert.True(t, taskID.Valid)

		task, err := s.GetTask(ctx, taskID)
		require.NoError(t, err)
		assert.Equal(t, taskID, task.ID)
		assert.Equal(t, q.ID, task.QueueID)
		assert.Equal(t, "my-task", task.TaskName)
		assert.JSONEq(t, `{"input":"hello"}`, string(task.Params))
		assert.JSONEq(t, `{"x-trace":"abc"}`, string(task.Headers))
		assert.Equal(t, int32(5), task.MaxAttempts)
		assert.Equal(t, dbgen.TaskStatusPending, task.Status)
	})

	t.Run("update task status", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "task-queue")
		taskID := createTestTask(t, s, q.ID, "status-task")

		now := time.Now().UTC().Truncate(time.Microsecond)
		err := s.UpdateTaskStatus(ctx, dbgen.UpdateTaskStatusParams{
			ID:          taskID,
			Status:      dbgen.TaskStatusCompleted,
			CompletedAt: validTimestamptz(now),
		})
		require.NoError(t, err)

		task, err := s.GetTask(ctx, taskID)
		require.NoError(t, err)
		assert.Equal(t, dbgen.TaskStatusCompleted, task.Status)
		assert.True(t, task.CompletedAt.Valid)
	})
}

func TestRuns(t *testing.T) {
	t.Run("create get and get by task", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "run-queue")
		taskID := createTestTask(t, s, q.ID, "run-task")

		run, err := s.CreateRun(ctx, dbgen.CreateRunParams{
			TaskID:  taskID,
			Attempt: 1,
		})
		require.NoError(t, err)
		assert.True(t, run.ID.Valid)
		assert.Equal(t, taskID, run.TaskID)
		assert.Equal(t, int32(1), run.Attempt)
		assert.Equal(t, dbgen.RunStatusPending, run.Status)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, run.ID, got.ID)

		runs, err := s.GetRunsByTask(ctx, taskID)
		require.NoError(t, err)
		require.Len(t, runs, 1)
		assert.Equal(t, run.ID, runs[0].ID)
	})

	t.Run("claim runs", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "claim-queue")
		taskID := createTestTask(t, s, q.ID, "claim-task")
		_ = createTestRun(t, s, taskID)

		claimed, err := s.ClaimRuns(ctx, dbgen.ClaimRunsParams{
			ClaimTimeoutSeconds: 60,
			QueueID:             q.ID,
			Limit:                 10,
		})
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		assert.Equal(t, dbgen.RunStatusClaimed, claimed[0].Status)
		assert.Equal(t, "claim-task", claimed[0].TaskName)
		assert.JSONEq(t, `{"key":"value"}`, string(claimed[0].TaskParams))
	})

	t.Run("claim skips future scheduled", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "sched-queue")
		taskID := createTestTask(t, s, q.ID, "sched-task")

		future := time.Now().Add(1 * time.Hour)
		_, err := s.CreateRun(ctx, dbgen.CreateRunParams{
			TaskID:      taskID,
			Attempt:     1,
			ScheduledAt: validTimestamptz(future),
		})
		require.NoError(t, err)

		claimed, err := s.ClaimRuns(ctx, dbgen.ClaimRunsParams{
			ClaimTimeoutSeconds: 60,
			QueueID:             q.ID,
			Limit:                 10,
		})
		require.NoError(t, err)
		assert.Empty(t, claimed)
	})

	t.Run("claim reclaims expired", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "reclaim-queue")
		taskID := createTestTask(t, s, q.ID, "reclaim-task")
		run := createTestRun(t, s, taskID)

		// Claim the run with a very short timeout.
		claimed, err := s.ClaimRuns(ctx, dbgen.ClaimRunsParams{
			ClaimTimeoutSeconds: 1,
			QueueID:             q.ID,
			Limit:                 1,
		})
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		assert.Equal(t, run.ID, claimed[0].ID)

		// Manually expire the claim by setting claim_expires_at to the past.
		_, err = testPool.Exec(ctx,
			"UPDATE runs SET claim_expires_at = now() - interval '1 second' WHERE id = $1",
			run.ID)
		require.NoError(t, err)

		// Reclaim should pick it up.
		reclaimed, err := s.ClaimRuns(ctx, dbgen.ClaimRunsParams{
			ClaimTimeoutSeconds: 60,
			QueueID:             q.ID,
			Limit:                 10,
		})
		require.NoError(t, err)
		require.Len(t, reclaimed, 1)
		assert.Equal(t, run.ID, reclaimed[0].ID)
	})

	t.Run("complete run", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "complete-queue")
		taskID := createTestTask(t, s, q.ID, "complete-task")
		run := createTestRun(t, s, taskID)

		result := []byte(`{"output":"done"}`)
		err := s.CompleteRun(ctx, dbgen.CompleteRunParams{
			ID:     run.ID,
			Result: result,
		})
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, dbgen.RunStatusCompleted, got.Status)
		assert.JSONEq(t, `{"output":"done"}`, string(got.Result))
		assert.True(t, got.CompletedAt.Valid)
	})

	t.Run("fail run", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "fail-queue")
		taskID := createTestTask(t, s, q.ID, "fail-task")
		run := createTestRun(t, s, taskID)

		err := s.FailRun(ctx, dbgen.FailRunParams{
			ID:    run.ID,
			Error: validText("something went wrong"),
		})
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, dbgen.RunStatusFailed, got.Status)
		assert.Equal(t, "something went wrong", got.Error.String)
		assert.True(t, got.CompletedAt.Valid)
	})

	t.Run("schedule run", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "schedule-queue")
		taskID := createTestTask(t, s, q.ID, "schedule-task")
		run := createTestRun(t, s, taskID)

		schedTime := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Microsecond)
		err := s.ScheduleRun(ctx, dbgen.ScheduleRunParams{
			ID:          run.ID,
			ScheduledAt: validTimestamptz(schedTime),
		})
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.True(t, got.ScheduledAt.Valid)
		assert.WithinDuration(t, schedTime, got.ScheduledAt.Time, time.Second)
	})
}

func TestSleepWake(t *testing.T) {
	t.Run("set run sleeping", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "sleep-queue")
		taskID := createTestTask(t, s, q.ID, "sleep-task")
		run := createTestRun(t, s, taskID)

		timeout := time.Now().Add(5 * time.Minute).UTC().Truncate(time.Microsecond)
		err := s.SetRunSleeping(ctx, dbgen.SetRunSleepingParams{
			ID:               run.ID,
			WaitingEventName: validText("user.created"),
			WaitingStepName:  validText("wait-for-user"),
			WaitingTimeoutAt: validTimestamptz(timeout),
		})
		require.NoError(t, err)

		got, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, dbgen.RunStatusSleeping, got.Status)
		assert.Equal(t, "user.created", got.WaitingEventName.String)
		assert.Equal(t, "wait-for-user", got.WaitingStepName.String)
		assert.True(t, got.WaitingTimeoutAt.Valid)
	})

	t.Run("wake runs by event", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "wake-queue")
		taskID := createTestTask(t, s, q.ID, "wake-task")
		run := createTestRun(t, s, taskID)

		err := s.SetRunSleeping(ctx, dbgen.SetRunSleepingParams{
			ID:               run.ID,
			WaitingEventName: validText("order.paid"),
			WaitingStepName:  validText("wait-payment"),
			WaitingTimeoutAt: validTimestamptz(time.Now().Add(10 * time.Minute)),
		})
		require.NoError(t, err)

		woken, err := s.WakeRunsByEvent(ctx, validText("order.paid"))
		require.NoError(t, err)
		require.Len(t, woken, 1)
		assert.Equal(t, run.ID, woken[0].ID)
		assert.Equal(t, dbgen.RunStatusPending, woken[0].Status)
		assert.False(t, woken[0].WaitingEventName.Valid)
	})

	t.Run("expire timed out waiters", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "timeout-queue")
		taskID := createTestTask(t, s, q.ID, "timeout-task")
		run := createTestRun(t, s, taskID)

		// Set sleeping with a timeout in the past.
		past := time.Now().Add(-1 * time.Minute)
		err := s.SetRunSleeping(ctx, dbgen.SetRunSleepingParams{
			ID:               run.ID,
			WaitingEventName: validText("never.happens"),
			WaitingStepName:  validText("wait-forever"),
			WaitingTimeoutAt: validTimestamptz(past),
		})
		require.NoError(t, err)

		expired, err := s.ExpireTimedOutWaiters(ctx)
		require.NoError(t, err)
		require.Len(t, expired, 1)
		assert.Equal(t, run.ID, expired[0].ID)
		assert.Equal(t, dbgen.RunStatusFailed, expired[0].Status)
		assert.Equal(t, "wait timeout expired", expired[0].Error.String)
	})
}

func TestCheckpoints(t *testing.T) {
	t.Run("upsert and get checkpoint", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "cp-queue")
		taskID := createTestTask(t, s, q.ID, "cp-task")
		run := createTestRun(t, s, taskID)

		state := []byte(`{"step":1,"progress":50}`)
		cp, err := s.UpsertCheckpoint(ctx, dbgen.UpsertCheckpointParams{
			TaskID:     taskID,
			StepName:   "process-items",
			State:      state,
			OwnerRunID: run.ID,
		})
		require.NoError(t, err)
		assert.True(t, cp.ID.Valid)
		assert.Equal(t, "process-items", cp.StepName)
		assert.JSONEq(t, `{"step":1,"progress":50}`, string(cp.State))

		got, err := s.GetCheckpoint(ctx, dbgen.GetCheckpointParams{
			TaskID:   taskID,
			StepName: "process-items",
		})
		require.NoError(t, err)
		assert.Equal(t, cp.ID, got.ID)
		assert.JSONEq(t, `{"step":1,"progress":50}`, string(got.State))
	})

	t.Run("upsert updates existing checkpoint", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "cp-upd-queue")
		taskID := createTestTask(t, s, q.ID, "cp-upd-task")
		run := createTestRun(t, s, taskID)

		_, err := s.UpsertCheckpoint(ctx, dbgen.UpsertCheckpointParams{
			TaskID:     taskID,
			StepName:   "my-step",
			State:      []byte(`{"v":1}`),
			OwnerRunID: run.ID,
		})
		require.NoError(t, err)

		updated, err := s.UpsertCheckpoint(ctx, dbgen.UpsertCheckpointParams{
			TaskID:     taskID,
			StepName:   "my-step",
			State:      []byte(`{"v":2}`),
			OwnerRunID: run.ID,
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"v":2}`, string(updated.State))

		got, err := s.GetCheckpoint(ctx, dbgen.GetCheckpointParams{
			TaskID:   taskID,
			StepName: "my-step",
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"v":2}`, string(got.State))
	})

	t.Run("extend run claim", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		q := createTestQueue(t, s, "extend-queue")
		taskID := createTestTask(t, s, q.ID, "extend-task")
		run := createTestRun(t, s, taskID)

		// Claim the run first.
		_, err := s.ClaimRuns(ctx, dbgen.ClaimRunsParams{
			ClaimTimeoutSeconds: 10,
			QueueID:             q.ID,
			Limit:                 1,
		})
		require.NoError(t, err)

		beforeExtend, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)

		err = s.ExtendRunClaim(ctx, dbgen.ExtendRunClaimParams{
			ID:              run.ID,
			ExtendBySeconds: 300,
		})
		require.NoError(t, err)

		afterExtend, err := s.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.True(t, afterExtend.ClaimExpiresAt.Time.After(beforeExtend.ClaimExpiresAt.Time))
	})
}

func TestEvents(t *testing.T) {
	t.Run("create and get by name", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		payload := []byte(`{"user_id":"123"}`)
		ev, err := s.CreateEvent(ctx, dbgen.CreateEventParams{
			EventName: "user.signup",
			Payload:   payload,
		})
		require.NoError(t, err)
		assert.True(t, ev.ID.Valid)
		assert.Equal(t, "user.signup", ev.EventName)
		assert.JSONEq(t, `{"user_id":"123"}`, string(ev.Payload))

		got, err := s.GetEventByName(ctx, "user.signup")
		require.NoError(t, err)
		assert.Equal(t, ev.ID, got.ID)
		assert.Equal(t, ev.EventName, got.EventName)
	})
}

func TestWorkflowRuns(t *testing.T) {
	t.Run("create and get by ID", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		inputs, _ := json.Marshal(map[string]string{"foo": "bar"})
		tags, _ := json.Marshal([]string{"tag1", "tag2"})

		wr, err := s.CreateWorkflowRun(ctx, dbgen.CreateWorkflowRunParams{
			WorkflowName:    "order-flow",
			WorkflowVersion: validText("v2"),
			Inputs:          inputs,
			CreatedBy:       validText("admin"),
			Tags:            tags,
		})
		require.NoError(t, err)
		assert.True(t, wr.ID.Valid)
		assert.Equal(t, "order-flow", wr.WorkflowName)
		assert.Equal(t, "v2", wr.WorkflowVersion.String)
		assert.Equal(t, dbgen.WorkflowRunStatusPending, wr.Status)
		assert.JSONEq(t, `{"foo":"bar"}`, string(wr.Inputs))

		got, err := s.GetWorkflowRun(ctx, wr.ID)
		require.NoError(t, err)
		assert.Equal(t, wr.ID, got.ID)
		assert.Equal(t, wr.WorkflowName, got.WorkflowName)
	})

	t.Run("update status and result", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		wr, err := s.CreateWorkflowRun(ctx, dbgen.CreateWorkflowRunParams{
			WorkflowName: "update-flow",
		})
		require.NoError(t, err)

		now := time.Now().UTC().Truncate(time.Microsecond)
		result := []byte(`{"total":42}`)

		updated, err := s.UpdateWorkflowRun(ctx, dbgen.UpdateWorkflowRunParams{
			ID:          wr.ID,
			Status:      dbgen.NullWorkflowRunStatus{WorkflowRunStatus: dbgen.WorkflowRunStatusCompleted, Valid: true},
			Result:      result,
			StartedAt:   validTimestamptz(now),
			CompletedAt: validTimestamptz(now),
		})
		require.NoError(t, err)
		assert.Equal(t, dbgen.WorkflowRunStatusCompleted, updated.Status)
		assert.JSONEq(t, `{"total":42}`, string(updated.Result))
		assert.True(t, updated.StartedAt.Valid)
		assert.True(t, updated.CompletedAt.Valid)
	})
}

func TestExecTx(t *testing.T) {
	t.Run("commit makes both writes visible", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		err := s.ExecTx(ctx, func(q *dbgen.Queries) error {
			_, err := q.CreateQueue(ctx, dbgen.CreateQueueParams{
				Name:            "tx-queue-1",
				TaskTtlSeconds:  3600,
				EventTtlSeconds: 3600,
			})
			if err != nil {
				return err
			}
			_, err = q.CreateQueue(ctx, dbgen.CreateQueueParams{
				Name:            "tx-queue-2",
				TaskTtlSeconds:  3600,
				EventTtlSeconds: 3600,
			})
			return err
		})
		require.NoError(t, err)

		_, err = s.GetQueueByName(ctx, "tx-queue-1")
		require.NoError(t, err)
		_, err = s.GetQueueByName(ctx, "tx-queue-2")
		require.NoError(t, err)
	})

	t.Run("rollback on error", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		err := s.ExecTx(ctx, func(q *dbgen.Queries) error {
			_, err := q.CreateQueue(ctx, dbgen.CreateQueueParams{
				Name:            "rollback-queue",
				TaskTtlSeconds:  3600,
				EventTtlSeconds: 3600,
			})
			if err != nil {
				return err
			}
			return errors.New("intentional failure")
		})
		require.Error(t, err)

		_, err = s.GetQueueByName(ctx, "rollback-queue")
		require.Error(t, err, "queue should not exist after rollback")
	})
}
