package db

import (
	"context"
	"testing"
	"time"

	dbgen "github.com/cornelmarck/durable-execution/internal/db/gen"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListener(t *testing.T) {
	t.Run("notifies correct queue on run insert", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		q1 := createTestQueue(t, s, "listen-q1")
		q2 := createTestQueue(t, s, "listen-q2")

		listener := NewListener(testPool)
		listenCtx, listenCancel := context.WithCancel(ctx)
		defer listenCancel()
		go listener.Run(listenCtx)

		// Give the listener time to establish the LISTEN connection.
		time.Sleep(100 * time.Millisecond)

		// Subscribe to both queues before creating any runs.
		sigQ1 := listener.Signal("listen-q1")
		sigQ2 := listener.Signal("listen-q2")

		// Insert a run into q1.
		taskID := createTestTask(t, s, q1.ID, "listen-task-1")
		createTestRun(t, s, taskID)

		// q1 signal should fire.
		select {
		case <-sigQ1:
			// expected
		case <-time.After(3 * time.Second):
			t.Fatal("expected notification for listen-q1")
		}

		// q2 signal should NOT have fired.
		select {
		case <-sigQ2:
			t.Fatal("unexpected notification for listen-q2")
		case <-time.After(100 * time.Millisecond):
			// expected — no notification
		}

		// Now insert into q2.
		sigQ2 = listener.Signal("listen-q2") // re-obtain in case
		taskID2 := createTestTask(t, s, q2.ID, "listen-task-2")
		createTestRun(t, s, taskID2)

		select {
		case <-sigQ2:
			// expected
		case <-time.After(3 * time.Second):
			t.Fatal("expected notification for listen-q2")
		}
	})

	t.Run("notifies when sleeping run is woken", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		q := createTestQueue(t, s, "wake-notify-q")
		taskID := createTestTask(t, s, q.ID, "wake-notify-task")
		run := createTestRun(t, s, taskID)

		// Put the run to sleep.
		err := s.SetRunSleeping(ctx, dbgen.SetRunSleepingParams{
			ID:               run.ID,
			WaitingEventName: validText("approval"),
			WaitingStepName:  validText("wait-approval"),
			WaitingTimeoutAt: validTimestamptz(time.Now().Add(10 * time.Minute)),
		})
		require.NoError(t, err)

		listener := NewListener(testPool)
		listenCtx, listenCancel := context.WithCancel(ctx)
		defer listenCancel()
		go listener.Run(listenCtx)

		time.Sleep(100 * time.Millisecond)

		sig := listener.Signal("wake-notify-q")

		// Wake the run — this UPDATEs status to 'pending', which should trigger a notification.
		woken, err := s.WakeRunsByEvent(ctx, validText("approval"))
		require.NoError(t, err)
		require.Len(t, woken, 1)

		select {
		case <-sig:
			// expected — notification received
		case <-time.After(3 * time.Second):
			t.Fatal("expected notification after waking sleeping run")
		}
	})

	t.Run("returns error after max consecutive failures", func(t *testing.T) {
		// Point at a port where nothing is listening.
		badPool, err := pgxpool.New(context.Background(), "postgres://localhost:1/bad")
		require.NoError(t, err)
		defer badPool.Close()

		listener := NewListener(badPool)
		listener.MaxConsecFailures = 2

		err = listener.Run(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "2 consecutive failures")
	})

	t.Run("signal before subscribe creates channel on demand", func(t *testing.T) {
		_ = newTestStore(t)
		ctx := context.Background()

		listener := NewListener(testPool)
		listenCtx, listenCancel := context.WithCancel(ctx)
		defer listenCancel()
		go listener.Run(listenCtx)

		time.Sleep(100 * time.Millisecond)

		// Signal for a queue that hasn't received any notifications yet.
		ch := listener.Signal("brand-new-queue")
		assert.NotNil(t, ch)

		// Should not be closed yet.
		select {
		case <-ch:
			t.Fatal("channel should not be closed yet")
		default:
			// expected
		}
	})
}
