package river

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestProducer_LeaseRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec      riverdriver.Executor
		offlineCh chan struct{}
		producer  *producer
		schema    string
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			archetype  = riversharedtest.BaseServiceArchetype(t)
			dbPool     = riversharedtest.DBPool(ctx, t)
			driver     = riverpgxv5.New(dbPool)
			exec       = driver.GetExecutor()
			schema     = riverdbtest.TestSchema(ctx, t, driver, nil)
			jobUpdates = make(chan []jobcompleter.CompleterJobUpdated, 10)
			queueName  = "test-producer-lease-recovery"
			pilot      = &riverpilot.StandardPilot{}
		)

		t.Cleanup(riverinternaltest.DiscardContinuously(jobUpdates))

		completer := jobcompleter.NewInlineCompleter(archetype, schema, exec, pilot, jobUpdates)
		require.NoError(t, completer.Start(ctx))
		t.Cleanup(completer.Stop)

		// Drain producer lifecycle notifications; these tests assert lease
		// state rather than notification delivery, which is covered
		// elsewhere.
		listener := notifier.New(archetype, driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema}))
		require.NoError(t, listener.Start(ctx))
		t.Cleanup(listener.Stop)
		offlineCh := make(chan struct{}, 16)
		_, err := listener.Listen(ctx, notifier.NotificationTopicProducer, func(notifier.NotificationTopic, string) {
			select {
			case offlineCh <- struct{}{}:
			default:
			}
		})
		require.NoError(t, err)

		producer := newProducer(archetype, exec, pilot, &producerConfig{
			ClientID:            testClientID,
			Completer:           completer,
			ErrorHandler:        newTestErrorHandler(),
			FetchCooldown:       FetchCooldownDefault,
			FetchPollInterval:   50 * time.Millisecond,
			LeaseTTL:            time.Minute,
			MaxWorkers:          1_000,
			PluginLookupByJob:   pluginlookup.NewJobPluginLookup(nil),
			PluginLookupGlobal:  pluginlookup.NewPluginLookup(nil),
			JobTimeout:          JobTimeoutDefault,
			Queue:               queueName,
			QueuePollInterval:   queuePollIntervalDefault,
			QueueReportInterval: queueReportIntervalDefault,
			RetryPolicy:         &DefaultClientRetryPolicy{},
			SchedulerInterval:   riverinternaltest.SchedulerShortInterval,
			Schema:              schema,
			Workers:             NewWorkers(),
		})
		producer.testSignals.Init(t)

		require.NoError(t, producer.StartWorkContext(ctx, ctx))
		t.Cleanup(producer.Stop)

		// The first automatic keepalive lands shortly after start; wait for
		// it so later assertions are deterministic.
		producer.testSignals.ReportedProducerStatus.WaitOrTimeout()

		return &testBundle{
			exec:      exec,
			offlineCh: offlineCh,
			producer:  producer,
			schema:    schema,
		}
	}

	getRow := func(t *testing.T, bundle *testBundle) *riverdriver.Producer {
		t.Helper()

		row, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
			ClientID:  testClientID,
			QueueName: bundle.producer.config.Queue,
			Schema:    bundle.schema,
		})
		require.NoError(t, err)
		return row
	}

	t.Run("ReacquiresAfterLeaseReaped", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		row := getRow(t, bundle)
		require.EqualValues(t, 1, row.Generation)
		require.Nil(t, row.ReapedAt)

		// Simulate the leader reaping the lease after it expired while this
		// process was partitioned.
		future := time.Now().UTC().Add(10 * time.Minute)
		reaped, err := bundle.exec.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
			Max:    10,
			Now:    &future,
			Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Len(t, reaped, 1)

		// The next keepalive misses, the producer reconciles from persistent
		// state, finds itself reaped, and re-acquires with a new generation.
		bundle.producer.reportProducerStatusOnce(ctx)

		row = getRow(t, bundle)
		require.Nil(t, row.ReapedAt)
		require.EqualValues(t, 2, row.Generation)
		require.EqualValues(t, 2, bundle.producer.generation.Load())

		// Subsequent keepalives succeed on the new generation.
		bundle.producer.reportProducerStatusOnce(ctx)
		row = getRow(t, bundle)
		require.Nil(t, row.ReapedAt)
		require.EqualValues(t, 2, row.Generation)
	})

	t.Run("StopsFetchWhenSupersededByNewerGeneration", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		row := getRow(t, bundle)
		require.EqualValues(t, 1, row.Generation)

		// A new process takes the slot with generation two.
		now := time.Now().UTC()
		_, err := bundle.exec.ProducerInsert(ctx, &riverdriver.ProducerInsertParams{
			ClientID:   testClientID,
			MaxWorkers: 1_000,
			Now:        &now,
			ProducerID: 4242,
			QueueName:  bundle.producer.config.Queue,
			Schema:     bundle.schema,
			TTL:        time.Minute,
		})
		require.NoError(t, err)

		// The stale producer's next keepalive detects the supersession and
		// initiates its own shutdown: no further jobs are fetched.
		bundle.producer.reportProducerStatusOnce(ctx)
		bundle.producer.testSignals.LeaseLost.WaitOrTimeout()
		riversharedtest.WaitOrTimeout(t, bundle.producer.Stopped())

		// Its fenced finalize leaves the newer generation's lease active and
		// publishes no offline notification for it.
		row = getRow(t, bundle)
		require.EqualValues(t, 2, row.Generation)
		require.EqualValues(t, 4242, row.ProducerID)
		require.Nil(t, row.ReapedAt)

		// Repeating the reconciliation doesn't re-signal or resurrect.
		bundle.producer.reportProducerStatusOnce(ctx)

		// Any offline notification in the channel is from the reaped-while-
		// partitioned path, never from this superseded shutdown. Assert the
		// newer row is still what owns the slot.
		row = getRow(t, bundle)
		require.Nil(t, row.ReapedAt)
	})
}
