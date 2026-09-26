package river

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
)

const (
	queueDrainKeyLength           = 16
	queueDrainPollIntervalDefault = 1 * time.Second
)

// QueueDrainState is the state of a queue drain (handoff) as returned by
// QueueDrain and QueueDrainStatus.
type QueueDrainState string

const (
	// QueueDrainStateDraining indicates that a drain is active and the queue's
	// producers are not fetching new jobs while running jobs are allowed to
	// finish.
	QueueDrainStateDraining QueueDrainState = riverdriver.QueueDrainStateDraining

	// QueueDrainStateDrained indicates that a drain's running job count has
	// reached zero. The queue remains gated for fetching until explicitly
	// resumed with QueueDrainResume.
	QueueDrainStateDrained QueueDrainState = riverdriver.QueueDrainStateDrained

	// QueueDrainStateResumed indicates that a completed drain was explicitly
	// resumed and the handoff has ended. It's only returned when re-attaching a
	// previously resumed drain via its idempotency key.
	QueueDrainStateResumed QueueDrainState = riverdriver.QueueDrainStateResumed
)

// QueueDrainOpts are optional settings for a queue drain operation.
type QueueDrainOpts struct {
	// Key is an optional idempotency key identifying this handoff. When empty,
	// a random key is generated and returned in QueueDrainResult. Retrying
	// QueueDrain with the same key (including from another client) attaches to
	// the same drain rather than creating a second one.
	Key string
}

// QueueDrainResumeOpts are optional settings for resuming a drained queue.
type QueueDrainResumeOpts struct{}

// QueueDrainResult is the result of a QueueDrain or QueueDrainStatus call.
type QueueDrainResult struct {
	// DrainedAt is the time the drain completed (running count reached zero),
	// if it has.
	DrainedAt *time.Time

	// Key is the stable identifier of this handoff. It's stable for the
	// lifetime of the drain and should be supplied on retries to avoid creating
	// a second handoff.
	Key string

	// Queue is the name of the drained queue.
	Queue string

	// RunningJobs is the number of jobs currently in a running state for the
	// queue at the time the result was read from the database. It's zero once
	// the drain is drained.
	RunningJobs int

	// StartedAt is the time the drain was initiated.
	StartedAt time.Time

	// State is the current state of the drain.
	State QueueDrainState
}

// QueueDrain initiates a drain (handoff) for the given queue and waits for it
// to drain. When a drain is active, all clients working the queue immediately
// stop fetching new jobs for it, while jobs which have already been fetched
// continue to run to completion normally. The drain transitions to a drained
// state only once the queue has no running jobs, and the queue remains gated
// for fetching at that point; call QueueDrainResume to restore fetching.
//
// QueueDrain is idempotent: retries carrying opts.Key of an existing drain
// attach to that same drain (from this client or another) and wait on it
// instead of creating a second handoff. If the caller omitted a key and an
// active drain already exists, or supplies a key different from the active
// drain's, QueueDrain returns a *QueueDrainAlreadyActiveError carrying the
// active key.
//
// The database is polled on a short interval in addition to receiving control
// notifications, so lost notifications only add latency. If the provided
// context times out or is cancelled, QueueDrain stops waiting and returns the
// most recent drain snapshot together with the context error; the drain itself
// stays active in the database and the queue remains gated. Call QueueDrain
// again with the same key to resume waiting.
//
// Unlike QueuePause, the "*" queue name is not supported; drains target a
// single named queue.
func (c *Client[TTx]) QueueDrain(ctx context.Context, name string, opts *QueueDrainOpts) (*QueueDrainResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}
	if err := validateQueueName(name); err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &QueueDrainOpts{}
	}

	key := opts.Key
	generatedKey := key == ""
	if generatedKey {
		key = randutil.Hex(queueDrainKeyLength)
	}

	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	row, err := tx.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
		Key:    key,
		Now:    c.baseService.Time.NowOrNil(),
		Queue:  name,
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, err
	}

	// notifyAction is the control event to broadcast so producers converge
	// even if the original notification was lost. A new drain broadcasts
	// "drain"; an attach to an already drained drain broadcasts
	// "drain_completed"; a resumed replay notifies nothing.
	var notifyAction controlAction
	if row != nil {
		notifyAction = controlActionDrain
	}

	if row == nil {
		// Insert didn't produce a row: either this is an idempotent retry with
		// the same key, an attach attempt with a mismatched key, or a replay of
		// a key whose handoff already resumed.
		activeRow, err := tx.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
			Queue:  name,
			Schema: c.config.Schema,
		})
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		switch {
		case activeRow != nil && activeRow.Key == key:
			row = activeRow
			if row.State == riverdriver.QueueDrainStateDrained {
				notifyAction = controlActionDrainCompleted
			} else {
				notifyAction = controlActionDrain
			}
		case activeRow != nil:
			return nil, &QueueDrainAlreadyActiveError{Name: name, Key: activeRow.Key}
		case generatedKey:
			// A freshly generated key cannot collide with anything; losing the
			// race here means an active drain appeared between insert and read.
			return nil, &QueueDrainAlreadyActiveError{Name: name}
		default:
			historicalRow, err := tx.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
				Key:    key,
				Queue:  name,
				Schema: c.config.Schema,
			})
			if err != nil {
				return nil, err
			}
			if historicalRow.State != riverdriver.QueueDrainStateResumed {
				// The active drain transitioned between the two reads.
				return nil, &QueueDrainAlreadyActiveError{Name: name}
			}
			row = historicalRow
		}
	}

	var controlEvent *controlEventPayload
	if notifyAction != "" {
		controlEvent, err = c.notifyQueueDrainControlEvent(ctx, tx, notifyAction, name, row.Key)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	if controlEvent != nil {
		c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)
	}

	c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Queue drain initiated",
		slog.String("queue", name),
		slog.String("key", row.Key),
		slog.String("state", row.State),
		slog.Int64("running_jobs", row.RunningCount),
	)

	result := queueDrainResultFromRow(row)
	if row.State != riverdriver.QueueDrainStateDraining {
		// Drained attach or resumed replay: report immediately without waiting.
		return result, nil
	}

	return c.queueDrainWait(ctx, name, row)
}

// QueueDrainResume explicitly resumes a drained queue so clients resume
// fetching new jobs for it. This is the only way to restore fetching after a
// drain completes; QueueResume does not clear a drain. QueueDrainResume does
// not modify the queue's paused state.
//
// It returns a *QueueDrainInProgressError if the drain is still draining (the
// running count hasn't reached zero), and is an idempotent no-op if no active
// drain exists.
func (c *Client[TTx]) QueueDrainResume(ctx context.Context, name string, opts *QueueDrainResumeOpts) error {
	if !c.driver.PoolIsSet() {
		return errNoDriverDBPool
	}
	if err := validateQueueName(name); err != nil {
		return err
	}

	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	resumeParams := &riverdriver.QueueDrainResumeParams{
		Now:    c.baseService.Time.NowOrNil(),
		Queue:  name,
		Schema: c.config.Schema,
	}

	rowsAffected, err := tx.QueueDrainResume(ctx, resumeParams)
	if err != nil {
		return err
	}

	if rowsAffected < 1 {
		activeRow, err := tx.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
			Queue:  name,
			Schema: c.config.Schema,
		})
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		switch {
		case activeRow != nil && activeRow.State == riverdriver.QueueDrainStateDraining:
			return &QueueDrainInProgressError{Name: name}
		case activeRow != nil:
			// The active row is drained, yet the conditional update matched no
			// row: a concurrent completion might have committed the
			// draining→drained transition between that UPDATE and this read
			// (each statement gets a fresh snapshot under READ COMMITTED).
			// Retry once rather than reporting a misleading idempotent success
			// while the queue stays gated.
			rowsAffected, err = tx.QueueDrainResume(ctx, resumeParams)
			if err != nil {
				return err
			}
			if rowsAffected < 1 {
				// Another caller resumed it concurrently: idempotent success.
				return tx.Commit(ctx)
			}
		default:
			// No active drain: a successful idempotent no-op.
			return tx.Commit(ctx)
		}
	}

	controlEvent, err := c.notifyQueueDrainControlEvent(ctx, tx, controlActionDrainResume, name, "")
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Queue drain resumed", slog.String("queue", name))

	return nil
}

// QueueDrainStatus returns the currently active drain for the queue and its
// running job count. If no drain is active (never drained or already resumed),
// it returns ErrNotFound.
func (c *Client[TTx]) QueueDrainStatus(ctx context.Context, name string) (*QueueDrainResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}
	if err := validateQueueName(name); err != nil {
		return nil, err
	}

	row, err := c.driver.GetExecutor().QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
		Queue:  name,
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, err
	}
	return queueDrainResultFromRow(row), nil
}

// queueDrainWait waits for an active drain to reach a drained state. Control
// notifications provide a fast wake, but the database is polled on a fixed
// interval regardless so the wait converges even if notifications are lost.
// On context end, the most recent snapshot is returned with the context error
// and the drain is left untouched.
func (c *Client[TTx]) queueDrainWait(ctx context.Context, queueName string, initialRow *riverdriver.QueueDrainRow) (*QueueDrainResult, error) {
	lastRow := initialRow

	// The drain key is fixed for the lifetime of this wait; read it here
	// because the notification callback runs on another goroutine and must not
	// touch lastRow while the wait loop updates it.
	drainKey := initialRow.Key

	wakeChan := make(chan struct{}, 1)
	if c.notifier != nil {
		controlSub, err := c.notifier.Listen(ctx, notifier.NotificationTopicControl, func(topic notifier.NotificationTopic, payload string) {
			var event controlEventPayload
			if jsonErr := json.Unmarshal([]byte(payload), &event); jsonErr != nil {
				return
			}
			if event.Action != controlActionDrainCompleted {
				return
			}
			if event.Queue != rivercommon.AllQueuesString && event.Queue != queueName {
				return
			}
			if event.Key != "" && event.Key != drainKey {
				return
			}
			select {
			case <-ctx.Done():
			case wakeChan <- struct{}{}:
			default:
			}
		})
		if err != nil {
			c.baseService.Logger.ErrorContext(ctx, c.baseService.Name+": Error listening for queue drain control events", slog.String("queue", queueName), slog.String("err", err.Error()))
		} else {
			defer controlSub.Unlisten(context.WithoutCancel(ctx))
		}
	}

	// Attempt completion immediately, then on wake or poll tick.
	attempt := func() bool {
		row, completed, err := c.queueDrainCompleteOnce(ctx, queueName, lastRow.Key)
		if err != nil {
			c.baseService.Logger.ErrorContext(ctx, c.baseService.Name+": Error polling queue drain state", slog.String("queue", queueName), slog.String("err", err.Error()))
			return false
		}
		lastRow = row
		return completed
	}

	if attempt() {
		return queueDrainResultFromRow(lastRow), nil
	}
	if ctx.Err() != nil {
		return queueDrainResultFromRow(lastRow), ctx.Err()
	}

	ticker := time.NewTicker(queueDrainPollIntervalDefault)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Queue drain wait ended before completion",
				slog.String("queue", queueName),
				slog.String("key", lastRow.Key),
				slog.String("state", lastRow.State),
				slog.Int64("running_jobs", lastRow.RunningCount),
			)
			return queueDrainResultFromRow(lastRow), ctx.Err()
		case <-wakeChan:
		case <-ticker.C:
		}

		if attempt() {
			c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Queue drain completed",
				slog.String("queue", queueName),
				slog.String("key", lastRow.Key),
			)
			return queueDrainResultFromRow(lastRow), nil
		}
	}
}

// queueDrainCompleteOnce makes one conditional attempt to complete the drain
// for the queue, emitting a control notification if this call completed it,
// then reads and returns the active drain row. The returned completed flag is
// true when the drain is now in a drained state.
func (c *Client[TTx]) queueDrainCompleteOnce(ctx context.Context, queueName, key string) (*riverdriver.QueueDrainRow, bool, error) {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	rowsAffected, err := tx.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
		Now:    c.baseService.Time.NowOrNil(),
		Queue:  queueName,
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, false, err
	}

	var controlEvent *controlEventPayload
	if rowsAffected > 0 {
		controlEvent, err = c.notifyQueueDrainControlEvent(ctx, tx, controlActionDrainCompleted, queueName, key)
		if err != nil {
			return nil, false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}

	if rowsAffected > 0 {
		c.notifyProducerWithoutListenerQueueControlEvent(queueName, controlEvent)
	}

	row, err := c.driver.GetExecutor().QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
		Queue:  queueName,
		Schema: c.config.Schema,
	})
	if errors.Is(err, ErrNotFound) {
		// The drain may have been explicitly resumed between the conditional
		// update and this read; fall back to a lookup by key.
		row, err = c.driver.GetExecutor().QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
			Key:    key,
			Queue:  queueName,
			Schema: c.config.Schema,
		})
	}
	if err != nil {
		return nil, false, err
	}
	return row, row.State != riverdriver.QueueDrainStateDraining, nil
}

func (c *Client[TTx]) notifyQueueDrainControlEvent(ctx context.Context, tx riverdriver.ExecutorTx, action controlAction, queueName, key string) (*controlEventPayload, error) {
	controlEvent := &controlEventPayload{
		Action: action,
		Key:    key,
		Queue:  queueName,
	}

	if !c.driver.SupportsListenNotify() {
		return controlEvent, nil
	}

	payload, err := json.Marshal(controlEvent)
	if err != nil {
		return nil, err
	}

	if err := tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: []string{string(payload)},
		Schema:  c.config.Schema,
		Topic:   string(notifier.NotificationTopicControl),
	}); err != nil {
		c.baseService.Logger.ErrorContext(
			ctx,
			c.baseService.Name+": Failed to send queue drain control notification",
			slog.String("queue", queueName),
			slog.String("err", err.Error()),
		)
		return nil, err
	}

	return controlEvent, nil
}

func queueDrainResultFromRow(row *riverdriver.QueueDrainRow) *QueueDrainResult {
	return &QueueDrainResult{
		DrainedAt:   row.DrainedAt,
		Key:         row.Key,
		Queue:       row.Queue,
		RunningJobs: int(row.RunningCount),
		StartedAt:   row.CreatedAt,
		State:       QueueDrainState(row.State),
	}
}
