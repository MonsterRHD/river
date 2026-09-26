package river

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

const (
	queueDrainOtherQueueName = "queue_drain_other_queue"
	queueDrainQueueName      = "queue_drain_test_queue"
	queueDrainRescueKind     = "queue_drain_rescue_ghost"
)

type queueDrainTestArgs struct{}

func (queueDrainTestArgs) Kind() string { return "queueDrainTest" }

type queueDrainOtherArgs struct{}

func (queueDrainOtherArgs) Kind() string { return "queueDrainOther" }

type queueDrainRescueArgs struct{}

func (queueDrainRescueArgs) Kind() string { return queueDrainRescueKind }

// queueDrainWorker blocks until releaseCh is closed, recording the job ID and
// the work context state at release time.
type queueDrainWorker struct {
	WorkerDefaults[queueDrainTestArgs]

	ctxErrCh  chan error
	releaseCh chan struct{}
	startedCh chan int64
}

func (w *queueDrainWorker) Work(ctx context.Context, job *Job[queueDrainTestArgs]) error {
	w.startedCh <- job.ID
	select {
	case <-ctx.Done():
		w.ctxErrCh <- ctx.Err()
		return ctx.Err()
	case <-w.releaseCh:
		w.ctxErrCh <- ctx.Err()
		return nil
	}
}

type queueDrainOtherWorker struct {
	WorkerDefaults[queueDrainOtherArgs]

	startedCh chan int64
}

func (w *queueDrainOtherWorker) Work(ctx context.Context, job *Job[queueDrainOtherArgs]) error {
	w.startedCh <- job.ID
	return nil
}

type queueDrainRescueWorker struct {
	WorkerDefaults[queueDrainRescueArgs]

	startedCh chan int64
}

func (w *queueDrainRescueWorker) Work(ctx context.Context, job *Job[queueDrainRescueArgs]) error {
	w.startedCh <- job.ID
	return nil
}

type drainTestClient struct {
	client    *Client[pgx.Tx]
	ctxErrCh  chan error
	eventCh   <-chan *Event
	releaseCh chan struct{}
	rescueCh  chan int64
	startedCh chan int64
}

type drainTestBundle struct {
	allCtxErrs <-chan error
	allEvents  <-chan *Event
	allRescued <-chan int64
	allStarted <-chan int64
	clients    []*drainTestClient
	exec       riverdriver.Executor
	schema     string
}

func setupQueueDrainTest(ctx context.Context, t *testing.T, numClients int, settingPollInterval ...time.Duration) *drainTestBundle {
	t.Helper()

	pollInterval := queueSettingPollIntervalNotifyModeDefault
	if len(settingPollInterval) > 0 {
		pollInterval = settingPollInterval[0]
	}

	pool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(pool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	exec := driver.GetExecutor()

	bundle := &drainTestBundle{
		clients: make([]*drainTestClient, 0, numClients),
		exec:    exec,
		schema:  schema,
	}

	var (
		allStarted = make(chan int64, 256)
		allCtxErrs = make(chan error, 256)
		allRescued = make(chan int64, 256)
		allEvents  = make(chan *Event, 256)
	)

	for range numClients {
		var (
			startedCh = make(chan int64, 64)
			releaseCh = make(chan struct{})
			ctxErrCh  = make(chan error, 64)
			rescueCh  = make(chan int64, 64)
		)

		// Registered before the client's Stop cleanup so these channels are only
		// closed once the client can no longer send on them.
		t.Cleanup(func() {
			close(startedCh)
			close(ctxErrCh)
			close(rescueCh)
		})

		workers := NewWorkers()
		AddWorker(workers, &queueDrainWorker{
			ctxErrCh:  ctxErrCh,
			releaseCh: releaseCh,
			startedCh: startedCh,
		})
		AddWorker(workers, &queueDrainOtherWorker{startedCh: startedCh})
		AddWorker(workers, &queueDrainRescueWorker{startedCh: rescueCh})

		config := &Config{
			FetchCooldown:     10 * time.Millisecond,
			FetchPollInterval: 50 * time.Millisecond,
			Logger:            riversharedtest.LoggerWarn(t),
			Queues: map[string]QueueConfig{
				queueDrainQueueName:      {MaxWorkers: 10},
				queueDrainOtherQueueName: {MaxWorkers: 10},
			},
			Schema:   schema,
			TestOnly: true,
			Workers:  workers,

			queueSettingPollInterval: pollInterval,
			schedulerInterval:        riverinternaltest.SchedulerShortInterval,
		}

		client := newTestClient(t, pool, config)
		for _, queueName := range []string{queueDrainQueueName, queueDrainOtherQueueName} {
			client.producersByQueueName[queueName].testSignals.Init(t)
		}
		startClient(ctx, t, client)

		eventCh, cancel := client.Subscribe(
			EventKindJobCompleted,
			EventKindQueueDrainStarted,
			EventKindQueueDrained,
			EventKindQueueDrainResumed,
		)
		t.Cleanup(cancel)

		testClient := &drainTestClient{
			client:    client,
			ctxErrCh:  ctxErrCh,
			eventCh:   eventCh,
			releaseCh: releaseCh,
			rescueCh:  rescueCh,
			startedCh: startedCh,
		}
		bundle.clients = append(bundle.clients, testClient)

		go func(ch chan int64) {
			for id := range ch {
				allStarted <- id
			}
		}(startedCh)
		go func(ch chan error) {
			for err := range ch {
				allCtxErrs <- err
			}
		}(ctxErrCh)
		go func(ch chan int64) {
			for id := range ch {
				allRescued <- id
			}
		}(rescueCh)
		go func(ch <-chan *Event) {
			for event := range ch {
				allEvents <- event
			}
		}(eventCh)
	}

	bundle.allStarted = allStarted
	bundle.allCtxErrs = allCtxErrs
	bundle.allRescued = allRescued
	bundle.allEvents = allEvents

	return bundle
}

func waitCount[T any](t *testing.T, valueCh <-chan T, count int, what string) []T {
	t.Helper()

	values := make([]T, 0, count)
	for range count {
		select {
		case value := <-valueCh:
			values = append(values, value)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %s (%d of %d observed)", what, len(values), count)
		}
	}
	return values
}

func waitDrainSignals(t *testing.T, clients []*drainTestClient, queueName string, waitFor func(p *producer)) {
	t.Helper()

	for _, client := range clients {
		waitFor(client.client.producersByQueueName[queueName])
	}
}

func waitEventKind(t *testing.T, ch <-chan *Event, kind EventKind, queueName string) *Event {
	t.Helper()

	for {
		select {
		case event := <-ch:
			if event.Kind != kind {
				continue
			}
			if queueName != "" && event.Queue != nil && event.Queue.Name != queueName && (event.Job == nil || event.Job.Queue != queueName) {
				continue
			}
			return event
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for event %s", kind)
		}
	}
}

type drainCallResult struct {
	err    error
	result *QueueDrainResult
}

func startDrainAsync(ctx context.Context, client *Client[pgx.Tx], queueName, key string) chan *drainCallResult {
	resultCh := make(chan *drainCallResult, 1)
	go func() {
		result, err := client.QueueDrain(ctx, queueName, &QueueDrainOpts{Key: key})
		resultCh <- &drainCallResult{err: err, result: result}
	}()
	return resultCh
}

func waitDrainResult(t *testing.T, ch chan *drainCallResult) *drainCallResult {
	t.Helper()

	select {
	case result := <-ch:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for QueueDrain to return")
		return nil
	}
}

func drainExpectTimeout(ctx context.Context, t *testing.T, client *Client[pgx.Tx], key string) *QueueDrainResult {
	t.Helper()

	drainCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	result, err := client.QueueDrain(drainCtx, queueDrainQueueName, &QueueDrainOpts{Key: key})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, result)
	require.Equal(t, QueueDrainStateDraining, result.State)
	return result
}

func releaseAll(t *testing.T, clients ...*drainTestClient) {
	t.Helper()

	for _, client := range clients {
		close(client.releaseCh)
	}
}

func insertDrainJobs(t *testing.T, ctx context.Context, client *Client[pgx.Tx], numJobs int) []int64 {
	t.Helper()

	ids := make([]int64, numJobs)
	for i := range numJobs {
		result, err := client.Insert(ctx, queueDrainTestArgs{}, &InsertOpts{Queue: queueDrainQueueName})
		require.NoError(t, err)
		ids[i] = result.Job.ID
	}
	return ids
}

func requireJobsState(t *testing.T, ctx context.Context, exec riverdriver.Executor, schema string, ids []int64, state rivertype.JobState) {
	t.Helper()

	for _, id := range ids {
		job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, state, job.State, "job %d has unexpected state", id)
	}
}

func waitJobStates(t *testing.T, ctx context.Context, exec riverdriver.Executor, schema string, ids []int64) {
	t.Helper()

	for _, id := range ids {
		require.Eventually(t, func() bool {
			job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: schema})
			require.NoError(t, err)
			return job.State == rivertype.JobStateCompleted
		}, 10*time.Second, 20*time.Millisecond, "job %d never reached state %s", id, rivertype.JobStateCompleted)
	}
}

func countDrainRows(t *testing.T, ctx context.Context, exec riverdriver.Executor, schema, queueName, key string) int {
	t.Helper()

	var count int
	require.NoError(t, exec.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s.river_queue_drain WHERE queue = $1 AND key = $2", schema),
		queueName, key).Scan(&count))
	return count
}

// requireNoNewStarts asserts no jobs are observed as started across any client
// within a short window while a gate is expected to be active.
func requireNoNewStarts(t *testing.T, ch <-chan int64) {
	t.Helper()

	select {
	case id := <-ch:
		require.FailNowf(t, "unexpected job start", "job %d started while the queue was gated", id)
	case <-time.After(250 * time.Millisecond):
	}
}

// rescueStubWorkUnit is a minimal work unit used to run the rescuer against
// the ghost job kind in tests.
type rescueStubWorkUnit struct{}

func (rescueStubWorkUnit) PluginLookup(*pluginlookup.JobPluginLookup) *pluginlookup.PluginLookup {
	return nil
}
func (rescueStubWorkUnit) Middleware() []rivertype.WorkerMiddleware { return nil }
func (rescueStubWorkUnit) NextRetry() time.Time                     { return time.Now() }
func (rescueStubWorkUnit) Timeout() time.Duration                   { return 0 }
func (rescueStubWorkUnit) UnmarshalJob() error                      { return nil }
func (rescueStubWorkUnit) Work(context.Context) error               { return nil }

type rescueStubWorkUnitFactory struct{}

func (rescueStubWorkUnitFactory) MakeUnit(*rivertype.JobRow) workunit.WorkUnit {
	return rescueStubWorkUnit{}
}

func TestQueueDrain(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("BasicFlow", func(t *testing.T) {
		t.Parallel()

		bundle := setupQueueDrainTest(ctx, t, 2)
		clients := bundle.clients

		runningIDs := insertDrainJobs(t, ctx, clients[0].client, 2)
		waitCount(t, bundle.allStarted, 2, "initial running jobs")

		drainCh := startDrainAsync(ctx, clients[0].client, queueDrainQueueName, "basic-flow-key")

		waitDrainSignals(t, clients, queueDrainQueueName, func(p *producer) {
			p.testSignals.DrainStarted.WaitOrTimeout()
		})

		startedEvent := waitEventKind(t, bundle.allEvents, EventKindQueueDrainStarted, queueDrainQueueName)
		require.Equal(t, "basic-flow-key", startedEvent.DrainKey)

		// New jobs into the drained queue aren't fetched, but the other queue
		// keeps working.
		gatedIDs := insertDrainJobs(t, ctx, clients[0].client, 3)
		otherResult, err := clients[0].client.Insert(ctx, queueDrainOtherArgs{}, &InsertOpts{Queue: queueDrainOtherQueueName})
		require.NoError(t, err)
		otherID := []int64{otherResult.Job.ID}
		waitCount(t, bundle.allStarted, 1, "other queue job")
		waitJobStates(t, ctx, bundle.exec, bundle.schema, otherID)
		requireNoNewStarts(t, bundle.allStarted)
		requireJobsState(t, ctx, bundle.exec, bundle.schema, gatedIDs, rivertype.JobStateAvailable)

		// Running jobs finish normally with a non-cancelled work context.

		releaseAll(t, clients...)

		drainResult := waitDrainResult(t, drainCh)
		require.NoError(t, drainResult.err)
		require.Equal(t, QueueDrainStateDrained, drainResult.result.State)
		require.Equal(t, 0, drainResult.result.RunningJobs)
		require.Equal(t, "basic-flow-key", drainResult.result.Key)
		require.NotNil(t, drainResult.result.DrainedAt)

		// Drained and job completion events are emitted by different clients
		// and may arrive in any order, so collect until both observations are
		// made instead of expecting one kind before the other.
		var (
			completedDrainJobs int
			sawDrainedEvent    bool
		)
		for completedDrainJobs < 2 || !sawDrainedEvent {
			select {
			case event := <-bundle.allEvents:
				switch event.Kind { //nolint:exhaustive // only drained and completion events are collected here
				case EventKindQueueDrained:
					require.Equal(t, "basic-flow-key", event.DrainKey)
					sawDrainedEvent = true
				case EventKindJobCompleted:
					if event.Job != nil && event.Job.Queue == queueDrainQueueName {
						completedDrainJobs++
					}
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("timed out collecting drain events (completed jobs=%d, drained=%t)", completedDrainJobs, sawDrainedEvent)
			}
		}
		for range 2 {
			select {
			case err := <-bundle.allCtxErrs:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for work context results")
			}
		}
		waitJobStates(t, ctx, bundle.exec, bundle.schema, runningIDs)
		requireJobsState(t, ctx, bundle.exec, bundle.schema, gatedIDs, rivertype.JobStateAvailable)

		// Explicit resume reopens fetching without touching the pause state.
		require.NoError(t, clients[0].client.QueueDrainResume(ctx, queueDrainQueueName, nil))
		waitDrainSignals(t, clients, queueDrainQueueName, func(p *producer) {
			p.testSignals.DrainResumed.WaitOrTimeout()
		})
		resumedEvent := waitEventKind(t, bundle.allEvents, EventKindQueueDrainResumed, queueDrainQueueName)
		require.Equal(t, "basic-flow-key", resumedEvent.DrainKey)

		waitCount(t, bundle.allStarted, 3, "resumed drain jobs")
		waitJobStates(t, ctx, bundle.exec, bundle.schema, gatedIDs)
	})

	t.Run("IdempotentAttachAndConflict", func(t *testing.T) {
		t.Parallel()

		bundle := setupQueueDrainTest(ctx, t, 1)
		client := bundle.clients[0].client

		insertDrainJobs(t, ctx, bundle.clients[0].client, 1)
		waitCount(t, bundle.allStarted, 1, "initial running job")

		result1 := drainExpectTimeout(ctx, t, client, "idem-key")
		require.Equal(t, 1, result1.RunningJobs)

		// Same key retried attaches to the same handoff.
		result2 := drainExpectTimeout(ctx, t, client, "idem-key")
		require.Equal(t, result1.Key, result2.Key)
		require.WithinDuration(t, result1.StartedAt, result2.StartedAt, time.Second)

		// Different and generated keys conflict and carry the active key.
		_, err := client.QueueDrain(ctx, queueDrainQueueName, &QueueDrainOpts{Key: "other-key"})
		var conflictErr *QueueDrainAlreadyActiveError
		require.ErrorAs(t, err, &conflictErr)
		require.Equal(t, queueDrainQueueName, conflictErr.Name)
		require.Equal(t, "idem-key", conflictErr.Key)

		_, err = client.QueueDrain(ctx, queueDrainQueueName, nil)
		require.ErrorAs(t, err, &conflictErr)
		require.Equal(t, "idem-key", conflictErr.Key)

		// Status exposes the same handoff and running count.
		status, err := client.QueueDrainStatus(ctx, queueDrainQueueName)
		require.NoError(t, err)
		require.Equal(t, "idem-key", status.Key)
		require.Equal(t, QueueDrainStateDraining, status.State)
		require.Equal(t, 1, status.RunningJobs)

		// Resume while still draining is rejected.
		err = client.QueueDrainResume(ctx, queueDrainQueueName, nil)
		var inProgressErr *QueueDrainInProgressError
		require.ErrorAs(t, err, &inProgressErr)

		// Once running jobs finish, the same key drains immediately.
		releaseAll(t, bundle.clients...)
		result3, err := client.QueueDrain(ctx, queueDrainQueueName, &QueueDrainOpts{Key: "idem-key"})
		require.NoError(t, err)
		require.Equal(t, QueueDrainStateDrained, result3.State)
		require.Equal(t, 0, result3.RunningJobs)
		require.Equal(t, 1, countDrainRows(t, ctx, bundle.exec, bundle.schema, queueDrainQueueName, "idem-key"))

		// Resume, then replay with the same key: historical handoff, no new row.
		require.NoError(t, client.QueueDrainResume(ctx, queueDrainQueueName, nil))
		replayed, err := client.QueueDrain(ctx, queueDrainQueueName, &QueueDrainOpts{Key: "idem-key"})
		require.NoError(t, err)
		require.Equal(t, QueueDrainStateResumed, replayed.State)
		require.Equal(t, 1, countDrainRows(t, ctx, bundle.exec, bundle.schema, queueDrainQueueName, "idem-key"))

		// Missing active drain status surfaces ErrNotFound; resume is a no-op.
		_, err = client.QueueDrainStatus(ctx, queueDrainQueueName)
		require.ErrorIs(t, err, ErrNotFound)
		require.NoError(t, client.QueueDrainResume(ctx, queueDrainQueueName, nil))
	})

	t.Run("OrthogonalToPauseAndResume", func(t *testing.T) {
		t.Parallel()

		bundle := setupQueueDrainTest(ctx, t, 1)
		client := bundle.clients[0].client

		insertDrainJobs(t, ctx, bundle.clients[0].client, 1)
		waitCount(t, bundle.allStarted, 1, "initial running job")

		drainCh := startDrainAsync(ctx, client, queueDrainQueueName, "pause-key")
		client.producersByQueueName[queueDrainQueueName].testSignals.DrainStarted.WaitOrTimeout()
		releaseAll(t, bundle.clients...)
		drained := waitDrainResult(t, drainCh)
		require.NoError(t, drained.err)
		require.Equal(t, QueueDrainStateDrained, drained.result.State)

		gatedID := insertDrainJobs(t, ctx, client, 1)[0]

		// Pausing and resuming doesn't clear the drain gate.
		require.NoError(t, client.QueuePause(ctx, queueDrainQueueName, nil))
		require.NoError(t, client.QueueResume(ctx, queueDrainQueueName, nil))
		requireNoNewStarts(t, bundle.allStarted)

		queueRow, err := bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Nil(t, queueRow.PausedAt)

		// Resuming the drain finally releases the job.
		require.NoError(t, client.QueueDrainResume(ctx, queueDrainQueueName, nil))
		waitCount(t, bundle.allStarted, 1, "resumed drain job")
		waitJobStates(t, ctx, bundle.exec, bundle.schema, []int64{gatedID})
	})

	t.Run("PollConvergesAcrossKeyTransition", func(t *testing.T) {
		t.Parallel()

		// A long poll interval makes the boundary deterministic: after the
		// producer observes the first handoff, the next poll is far enough away
		// that a full K1 draining -> K2 drained transition can happen in between.
		bundle := setupQueueDrainTest(ctx, t, 1, 500*time.Millisecond)
		client := bundle.clients[0].client
		exec := bundle.exec
		producer := client.producersByQueueName[queueDrainQueueName]

		_, err := exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:    "cross-key-1",
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		producer.testSignals.DrainStarted.WaitOrTimeout()

		// Between two polls the old handoff completes and resumes while a new
		// handoff starts and completes. No notifications are emitted, so the
		// fallback poll must observe both transitions within a single snapshot.
		_, err = exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		_, err = exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		_, err = exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:    "cross-key-2",
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		_, err = exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)

		// The poll replays an ordered resume(K1) + completion(K2); the producer
		// adopts the new handoff instead of rejecting both as stale keys and
		// staying gated forever. The old handoff's resume signal is delivered
		// first on the single control channel.
		producer.testSignals.DrainResumed.WaitOrTimeout()
		producer.testSignals.DrainCompleted.WaitOrTimeout()
		require.Equal(t, queueDrainStateDrained, producer.drainState)
		require.Equal(t, "cross-key-2", producer.drainKey)

		_, err = exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		producer.testSignals.DrainResumed.WaitOrTimeout()
		require.Equal(t, queueDrainStateNone, producer.drainState)

		jobID := insertDrainJobs(t, ctx, client, 1)[0]
		waitCount(t, bundle.allStarted, 1, "job fetched after cross-key resume")
		releaseAll(t, bundle.clients...)
		waitJobStates(t, ctx, exec, bundle.schema, []int64{jobID})
	})

	t.Run("PollConvergesWithoutNotifications", func(t *testing.T) {
		t.Parallel()

		bundle := setupQueueDrainTest(ctx, t, 1, 50*time.Millisecond)
		client := bundle.clients[0].client
		producer := client.producersByQueueName[queueDrainQueueName]

		// Start the drain straight in the database: no control notification is
		// emitted, so only the fallback poll can converge producer state.
		_, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:    "poll-key",
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)

		producer.testSignals.DrainStarted.WaitOrTimeout()
		require.Equal(t, queueDrainStateDraining, producer.drainState)

		gatedID := insertDrainJobs(t, ctx, client, 1)[0]
		requireNoNewStarts(t, bundle.allStarted)

		// Complete without a notification: poll observes the drained state.
		_, err = bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		producer.testSignals.DrainCompleted.WaitOrTimeout()
		require.Equal(t, queueDrainStateDrained, producer.drainState)

		// Resume without a notification: poll observes it and reopens fetch.
		_, err = bundle.exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
			Queue:  queueDrainQueueName,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		producer.testSignals.DrainResumed.WaitOrTimeout()
		require.Equal(t, queueDrainStateNone, producer.drainState)

		close(bundle.clients[0].releaseCh)
		waitCount(t, bundle.allStarted, 1, "poll-resumed job")
		waitJobStates(t, ctx, bundle.exec, bundle.schema, []int64{gatedID})
	})

	t.Run("RescueUnblocksDrain", func(t *testing.T) {
		t.Parallel()

		bundle := setupQueueDrainTest(ctx, t, 1)
		client := bundle.clients[0].client

		// A running job belonging to a dead producer, old enough to rescue.
		ghostAttemptedAt := time.Now().UTC().Add(-time.Hour)
		ghostJob := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			AttemptedAt: &ghostAttemptedAt,
			AttemptedBy: []string{"dead-producer"},
			Kind:        new(queueDrainRescueKind),
			Queue:       new(queueDrainQueueName),
			Schema:      bundle.schema,
			State:       queueDrainStateRunning(),
		})

		// Drain blocks while the ghost job is still in a running state.
		result := drainExpectTimeout(ctx, t, client, "rescue-key")
		require.GreaterOrEqual(t, result.RunningJobs, 1)

		rescuer := maintenance.NewRescuer(riversharedtest.BaseServiceArchetype(t), &maintenance.JobRescuerConfig{
			ClientJobTimeout:  time.Minute,
			ClientRetryPolicy: &DefaultClientRetryPolicy{},
			Interval:          time.Minute,
			RescueAfter:       time.Minute,
			Schema:            bundle.schema,
			WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
				if kind == queueDrainRescueKind {
					return rescueStubWorkUnitFactory{}
				}
				return nil
			},
		}, bundle.exec)
		rescuer.StaggerStartupDisable(true)
		rescuer.TestSignals.Init(t)
		t.Cleanup(rescuer.Stop)
		require.NoError(t, rescuer.Start(ctx))
		rescuer.TestSignals.UpdatedBatch.WaitOrTimeout()

		jobAfter, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: ghostJob.ID, Schema: bundle.schema})
		require.NoError(t, err)
		require.NotEqual(t, rivertype.JobStateRunning, jobAfter.State, "rescued job should no longer be running")

		// The drain now completes; the rescued job still isn't fetched while
		// the queue remains drained.
		drainResult, err := client.QueueDrain(ctx, queueDrainQueueName, &QueueDrainOpts{Key: "rescue-key"})
		require.NoError(t, err)
		require.Equal(t, QueueDrainStateDrained, drainResult.State)
		require.Equal(t, 0, drainResult.RunningJobs)

		select {
		case <-bundle.allRescued:
			t.Fatal("rescued job was fetched while the queue was still drained")
		case <-time.After(250 * time.Millisecond):
		}

		require.NoError(t, client.QueueDrainResume(ctx, queueDrainQueueName, nil))
		waitCount(t, bundle.allRescued, 1, "rescued job after resume")
	})

	t.Run("ResumeRetriesAfterMidCallCompletion", func(t *testing.T) {
		t.Parallel()

		pool := riversharedtest.DBPool(ctx, t)
		driver := riverpgxv5.New(pool)
		schema := riverdbtest.TestSchema(ctx, t, driver, nil)

		var (
			startedCh = make(chan int64, 16)
			releaseCh = make(chan struct{})
			ctxErrCh  = make(chan error, 16)
			rescueCh  = make(chan int64, 16)
		)
		t.Cleanup(func() {
			close(startedCh)
			close(ctxErrCh)
			close(rescueCh)
		})

		workers := NewWorkers()
		AddWorker(workers, &queueDrainWorker{ctxErrCh: ctxErrCh, releaseCh: releaseCh, startedCh: startedCh})
		AddWorker(workers, &queueDrainRescueWorker{startedCh: rescueCh})

		config := &Config{
			FetchCooldown:     10 * time.Millisecond,
			FetchPollInterval: 50 * time.Millisecond,
			Logger:            riversharedtest.LoggerWarn(t),
			Queues:            map[string]QueueConfig{queueDrainQueueName: {MaxWorkers: 10}},
			Schema:            schema,
			TestOnly:          true,
			Workers:           workers,

			queueSettingPollInterval: 50 * time.Millisecond,
			schedulerInterval:        riverinternaltest.SchedulerShortInterval,
		}

		client, err := NewClient(&resumeFlipDriver{Driver: driver}, config)
		require.NoError(t, err)
		client.producersByQueueName[queueDrainQueueName].testSignals.Init(t)
		startClient(ctx, t, client)

		exec := driver.GetExecutor()

		// An active draining handoff with no running jobs.
		_, err = exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:    "flip-key",
			Queue:  queueDrainQueueName,
			Schema: schema,
		})
		require.NoError(t, err)

		producer := client.producersByQueueName[queueDrainQueueName]
		producer.testSignals.DrainStarted.WaitOrTimeout()

		// The first conditional update observes draining and the follow-up
		// read observes drained; QueueDrainResume must retry rather than
		// return a misleading success while the queue stays gated.
		require.NoError(t, client.QueueDrainResume(ctx, queueDrainQueueName, nil))

		row, err := exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
			Key:    "flip-key",
			Queue:  queueDrainQueueName,
			Schema: schema,
		})
		require.NoError(t, err)
		require.Equal(t, riverdriver.QueueDrainStateResumed, row.State)

		producer.testSignals.DrainResumed.WaitOrTimeout()
		require.Equal(t, queueDrainStateNone, producer.drainState)

		jobID := insertDrainJobs(t, ctx, client, 1)[0]
		waitCount(t, startedCh, 1, "job fetched after resume")
		close(releaseCh)
		waitJobStates(t, ctx, exec, schema, []int64{jobID})
	})

	t.Run("StartsGatedWithExistingDrain", func(t *testing.T) {
		t.Parallel()

		pool := riversharedtest.DBPool(ctx, t)
		driver := riverpgxv5.New(pool)
		schema := riverdbtest.TestSchema(ctx, t, driver, nil)
		exec := driver.GetExecutor()

		_, err := exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:    "startup-key",
			Queue:  queueDrainQueueName,
			Schema: schema,
		})
		require.NoError(t, err)

		var (
			startedCh = make(chan int64, 16)
			releaseCh = make(chan struct{})
			ctxErrCh  = make(chan error, 16)
			rescueCh  = make(chan int64, 16)
		)
		workers := NewWorkers()
		AddWorker(workers, &queueDrainWorker{ctxErrCh: ctxErrCh, releaseCh: releaseCh, startedCh: startedCh})
		AddWorker(workers, &queueDrainRescueWorker{startedCh: rescueCh})

		config := &Config{
			FetchCooldown:     10 * time.Millisecond,
			FetchPollInterval: 50 * time.Millisecond,
			Logger:            riversharedtest.LoggerWarn(t),
			Queues:            map[string]QueueConfig{queueDrainQueueName: {MaxWorkers: 10}},
			Schema:            schema,
			TestOnly:          true,
			Workers:           workers,

			queueSettingPollInterval: 50 * time.Millisecond,
			schedulerInterval:        riverinternaltest.SchedulerShortInterval,
		}
		client := newTestClient(t, pool, config)
		client.producersByQueueName[queueDrainQueueName].testSignals.Init(t)
		startClient(ctx, t, client)

		producer := client.producersByQueueName[queueDrainQueueName]
		require.Equal(t, queueDrainStateDraining, producer.drainState)
		require.Equal(t, "startup-key", producer.drainKey)

		jobID := insertDrainJobs(t, ctx, client, 1)[0]
		requireNoNewStarts(t, startedCh)
		job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: jobID, Schema: schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, job.State)

		// Complete the drain straight in the database (no notification); the
		// producer's fallback poll observes it and stays gated.
		rowsAffected, err := exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
			Queue:  queueDrainQueueName,
			Schema: schema,
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)
		producer.testSignals.DrainCompleted.WaitOrTimeout()
		require.Equal(t, queueDrainStateDrained, producer.drainState)

		require.NoError(t, client.QueueDrainResume(ctx, queueDrainQueueName, nil))
		select {
		case <-startedCh:
		case <-time.After(10 * time.Second):
			t.Fatal("gated job never started after drain resume")
		}
		close(releaseCh)
	})

	t.Run("TakeoverAcrossClients", func(t *testing.T) {
		t.Parallel()

		bundle := setupQueueDrainTest(ctx, t, 2)
		clients := bundle.clients

		insertDrainJobs(t, ctx, clients[0].client, 1)
		waitCount(t, bundle.allStarted, 1, "initial running job")

		// Client A starts the drain but its wait times out (as if the instance
		// gave up mid-drain).
		resultA := drainExpectTimeout(ctx, t, clients[0].client, "takeover-key")

		// Client B attaches using the same key and observes the same handoff.
		resultB := drainExpectTimeout(ctx, t, clients[1].client, "takeover-key")
		require.Equal(t, resultA.Key, resultB.Key)
		require.WithinDuration(t, resultA.StartedAt, resultB.StartedAt, time.Second)
		require.Equal(t, resultA.RunningJobs, resultB.RunningJobs)

		releaseAll(t, clients...)

		// B continues waiting to completion on the same handoff.
		result, err := clients[1].client.QueueDrain(ctx, queueDrainQueueName, &QueueDrainOpts{Key: "takeover-key"})
		require.NoError(t, err)
		require.Equal(t, QueueDrainStateDrained, result.State)
		require.Equal(t, 0, result.RunningJobs)
		require.Equal(t, 1, countDrainRows(t, ctx, bundle.exec, bundle.schema, queueDrainQueueName, "takeover-key"))
	})
}

// resumeCompleteFlipTx simulates the drain flipping from draining to drained
// between QueueDrainResume's conditional update and its follow-up read: the
// first resume attempt completes the drain and reports no rows, while the
// second attempt performs the actual resume.
type resumeCompleteFlipTx struct {
	riverdriver.ExecutorTx

	resumeAttempt int
}

func (tx *resumeCompleteFlipTx) QueueDrainResume(ctx context.Context, params *riverdriver.QueueDrainResumeParams) (int64, error) {
	tx.resumeAttempt++
	if tx.resumeAttempt == 1 {
		if _, err := tx.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
			Now:    params.Now,
			Queue:  params.Queue,
			Schema: params.Schema,
		}); err != nil {
			return 0, err
		}
		return 0, nil
	}
	return tx.ExecutorTx.QueueDrainResume(ctx, params)
}

type resumeFlipExecutor struct {
	riverdriver.Executor
}

func (exec *resumeFlipExecutor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := exec.Executor.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &resumeCompleteFlipTx{ExecutorTx: tx}, nil
}

type resumeFlipDriver struct {
	riverdriver.Driver[pgx.Tx]
}

func (driver *resumeFlipDriver) GetExecutor() riverdriver.Executor {
	return &resumeFlipExecutor{Executor: driver.Driver.GetExecutor()}
}

func queueDrainStateRunning() *rivertype.JobState {
	state := rivertype.JobStateRunning
	return &state
}
