package db

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Listener uses PostgreSQL LISTEN/NOTIFY to signal when new runs are created.
// Notifications carry the queue name as payload, so only waiters for that
// specific queue are woken up.
type Listener struct {
	pool               *pgxpool.Pool
	MaxConsecFailures  int
	mu                 sync.Mutex
	queues             map[string]chan struct{}
}

// NewListener creates a Listener backed by the given connection pool.
func NewListener(pool *pgxpool.Pool) *Listener {
	return &Listener{
		pool:              pool,
		MaxConsecFailures: 5,
		queues:            make(map[string]chan struct{}),
	}
}

// Signal returns a channel that is closed when a new run is created in the
// given queue. Callers should obtain the channel before attempting a claim so
// that notifications arriving between the claim and the wait are not missed.
func (l *Listener) Signal(queueName string) <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch, ok := l.queues[queueName]
	if !ok {
		ch = make(chan struct{})
		l.queues[queueName] = ch
	}
	return ch
}

func (l *Listener) broadcast(queueName string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ch, ok := l.queues[queueName]; ok {
		close(ch)
		l.queues[queueName] = make(chan struct{})
	}
}

// Run listens for new_run notifications, reconnecting on error.
// It blocks until ctx is canceled. Returns a non-nil error if the maximum
// number of consecutive connection failures is reached.
func (l *Listener) Run(ctx context.Context) error {
	failures := 0
	for {
		if err := l.listen(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			slog.Error("listen error, reconnecting", "error", err, "consecutive_failures", failures)
			if failures >= l.MaxConsecFailures {
				return fmt.Errorf("listener: %d consecutive failures, last: %w", failures, err)
			}
			time.Sleep(time.Second)
		} else {
			failures = 0
		}
	}
}

func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN new_run")
	if err != nil {
		return err
	}

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		l.broadcast(n.Payload)
	}
}
