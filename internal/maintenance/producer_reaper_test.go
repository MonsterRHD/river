package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivertype"
)

func TestProducerReaper(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver    *riverpgxv5.Driver
		exec      riverdriver.Executor
		listener  *notifier.Notifier
		offlineCh chan *riverpilot.ProducerNotificationPayload
		schema    string
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			driver    = riverpgxv5.New(riversharedtest.DBPool(ctx, t))
			schema    = riverdbtest.TestSchema(ctx, t, driver, nil)
			exec      = driver.GetExecutor()
			offlineCh = make(chan *riverpilot.ProducerNotificationPayload, 32)
		)

		listener := notifier.New(riversharedtest.BaseServiceArchetype(t), driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema}))
		require.NoError(t, listener.Start(ctx))
		t.Cleanup(listener.Stop)

		_, err := listener.Listen(ctx, notifier.NotificationTopicProducer, func(_ notifier.NotificationTopic, payload string) {
			var notification riverpilot.ProducerNotificationPayload
			if err := json.Unmarshal([]byte(payload), &notification); err == nil {
				offlineCh <- &notification
			}
		})
		require.NoError(t, err)

		return &testBundle{
			driver:    driver,
			exec:      exec,
			listener:  listener,
			offlineCh: offlineCh,
			schema:    schema,
		}
	}

	newReaper := func(t *testing.T, bundle *testBundle, exec riverdriver.Executor) *ProducerReaper {
		t.Helper()

		reaper := NewProducerReaper(riversharedtest.BaseServiceArchetype(t), &ProducerReaperConfig{
			Interval:              50 * time.Millisecond,
			ReapedRetentionPeriod: ProducerReapedRetentionPeriodDefault,
			Schema:                bundle.schema,
		}, exec)
		reaper.StaggerStartupDisable(true)
		reaper.TestSignals.Init(t)
		t.Cleanup(reaper.Stop)
		return reaper
	}

	requireNoNotification := func(t *testing.T, ch <-chan *riverpilot.ProducerNotificationPayload) {
		t.Helper()

		select {
		case notification := <-ch:
			require.Failf(t, "unexpected producer offline notification", "%+v", notification)
		case <-time.After(300 * time.Millisecond):
		}
	}

	insertActiveProducer := func(t *testing.T, bundle *testBundle, clientID string, now time.Time, ttl time.Duration) *riverdriver.Producer {
		t.Helper()

		producer, err := bundle.exec.ProducerInsert(ctx, &riverdriver.ProducerInsertParams{
			ClientID:   clientID,
			MaxWorkers: 7,
			Now:        &now,
			ProducerID: 1,
			QueueName:  "reaper-queue",
			Schema:     bundle.schema,
			TTL:        ttl,
		})
		require.NoError(t, err)
		return producer
	}

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		reaper := NewProducerReaper(riversharedtest.BaseServiceArchetype(t), &ProducerReaperConfig{}, nil)

		require.Equal(t, ProducerReaperIntervalDefault, reaper.Config.Interval)
		require.Equal(t, ProducerReapedRetentionPeriodDefault, reaper.Config.ReapedRetentionPeriod)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		reaper := newReaper(t, bundle, bundle.exec)
		reaper.Logger = riversharedtest.LoggerWarn(t)
		reaper.TestSignals = ProducerReaperTestSignals{} // deinit so channels don't fill

		startstoptest.Stress(ctx, t, reaper)
	})

	t.Run("ReapsExpiredProducersAndNotifiesOnce", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		reaper := newReaper(t, bundle, bundle.exec)

		now := time.Now().UTC()

		// Expired and active: reaped with an offline notification.
		expired := insertActiveProducer(t, bundle, "client-expired", now.Add(-10*time.Minute), 5*time.Minute)
		// Not expired: left alone.
		fresh := insertActiveProducer(t, bundle, "client-fresh", now, 5*time.Minute)
		// Already reaped: transitioned again nor re-notified.
		reaped := insertActiveProducer(t, bundle, "client-reaped", now.Add(-20*time.Minute), 5*time.Minute)
		_, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
			ClientID:   "client-reaped",
			Generation: reaped.Generation,
			Now:        &now,
			QueueName:  "reaper-queue",
			Schema:     bundle.schema,
		})
		require.NoError(t, err)

		require.NoError(t, reaper.runOnce(ctx))

		notification := riversharedtest.WaitOrTimeout(t, bundle.offlineCh)
		require.Equal(t, riverpilot.ProducerNotificationActionOffline, notification.Action)
		require.Equal(t, "client-expired", notification.ClientID)
		require.Equal(t, "reaper-queue", notification.Queue)
		require.Equal(t, expired.Generation, notification.Generation)
		require.Equal(t, expired.ProducerID, notification.ProducerID)

		reapedExpired := riversharedtest.WaitOrTimeout(t, reaper.TestSignals.ReapedProducer.WaitC())
		require.Equal(t, "client-expired", reapedExpired.ClientID)

		// A second run (as after a leader change) transitions nothing and
		// doesn't re-publish.
		require.NoError(t, reaper.runOnce(ctx))
		requireNoNotification(t, bundle.offlineCh)

		expiredGet, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-expired", QueueName: "reaper-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.NotNil(t, expiredGet.ReapedAt)

		freshGet, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-fresh", QueueName: "reaper-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.Nil(t, freshGet.ReapedAt)
		require.WithinDuration(t, fresh.ExpiresAt, freshGet.ExpiresAt, time.Millisecond)

		reapedGet, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-reaped", QueueName: "reaper-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.NotNil(t, reapedGet.ReapedAt)
		require.WithinDuration(t, now, *reapedGet.ReapedAt, time.Millisecond)
	})

	t.Run("RetriesFailedBatchWithoutMissingOrDuplicatingNotification", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		var failNextNotify atomic.Bool
		failNextNotify.Store(true)
		failingExec := newNotifyFailingExecutor(bundle.exec, &failNextNotify)
		reaper := newReaper(t, bundle, failingExec)

		expired := insertActiveProducer(t, bundle, "client-fail-then-recover", time.Now().UTC().Add(-10*time.Minute), 5*time.Minute)

		// First attempt: notification fails, so the batch rolls back and no
		// offline change is published.
		err := reaper.runOnce(ctx)
		require.Error(t, err)

		stillActive, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-fail-then-recover", QueueName: "reaper-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.Nil(t, stillActive.ReapedAt)
		requireNoNotification(t, bundle.offlineCh)

		// Second attempt succeeds: exactly one transition and notification.
		require.NoError(t, reaper.runOnce(ctx))
		notification := riversharedtest.WaitOrTimeout(t, bundle.offlineCh)
		require.Equal(t, "client-fail-then-recover", notification.ClientID)
		require.Equal(t, expired.Generation, notification.Generation)
		requireNoNotification(t, bundle.offlineCh)
	})

	t.Run("ConcurrentKeepAliveAndReap", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		reaper := newReaper(t, bundle, bundle.exec)

		const count = 16

		// Each lease sits right at the expiry boundary, so whether the
		// keepalive or the reap wins is genuinely contended.
		start := time.Now().UTC().Add(-2 * time.Second)
		for i := range count {
			insertActiveProducer(t, bundle,
				fmt.Sprintf("client-race-%02d", i),
				start, 2*time.Second)
		}

		keepAliveDone := make(chan struct{})
		go func() {
			defer close(keepAliveDone)
			for i := range count {
				renewTime := time.Now().UTC()
				_, err := bundle.exec.ProducerKeepAlive(ctx, &riverdriver.ProducerKeepAliveParams{
					ClientID:   fmt.Sprintf("client-race-%02d", i),
					Generation: 1,
					Now:        &renewTime,
					QueueName:  "reaper-queue",
					Schema:     bundle.schema,
					TTL:        time.Hour,
				})
				// ErrNotFound means the reaper committed first; that's one of
				// the two valid outcomes.
				if err != nil {
					require.ErrorIs(t, err, rivertype.ErrNotFound)
				}
			}
		}()

		reapNow := time.Now().UTC()
		require.NoError(t, reaper.runOnce(ctx))
		<-keepAliveDone

		// Drain every notification produced during the race.
		notificationsByClient := map[string]int{}
	drain:
		for {
			select {
			case notification := <-bundle.offlineCh:
				notificationsByClient[notification.ClientID]++
			case <-time.After(300 * time.Millisecond):
				break drain
			}
		}

		// Exactly one of two outcomes per row: keepalive won (active lease far
		// in the future, no notification) or reap won (reaped, exactly one
		// notification). A reaped row must never come back active and a kept
		// lease must never be reaped or notified.
		for i := range count {
			clientID := fmt.Sprintf("client-race-%02d", i)
			row, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: clientID, QueueName: "reaper-queue", Schema: bundle.schema})
			require.NoError(t, err)
			switch {
			case row.ReapedAt != nil:
				require.Equal(t, 1, notificationsByClient[clientID], "reaped row must have exactly one offline notification")
			default:
				require.Nil(t, row.ReapedAt)
				require.True(t, row.ExpiresAt.After(reapNow))
				require.NotContains(t, notificationsByClient, clientID)
			}
		}

		// A second run transitions and notifies nothing.
		require.NoError(t, reaper.runOnce(ctx))
		select {
		case notification := <-bundle.offlineCh:
			require.Failf(t, "unexpected offline notification after race settled", "%+v", notification)
		case <-time.After(300 * time.Millisecond):
		}
	})

	t.Run("DeletesOnlyRowsReapedBeforeRetentionHorizon", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		reaper := newReaper(t, bundle, bundle.exec)

		now := time.Now().UTC()

		// Active with an unexpired lease: never deleted.
		insertActiveProducer(t, bundle, "client-active", now, 5*time.Minute)

		// Reaped recently: retained.
		recent := insertActiveProducer(t, bundle, "client-reaped-recent", now.Add(-72*time.Hour), 5*time.Minute)
		recentReap := now.Add(-time.Hour)
		_, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
			ClientID:   "client-reaped-recent",
			Generation: recent.Generation,
			Now:        &recentReap,
			QueueName:  "reaper-queue",
			Schema:     bundle.schema,
		})
		require.NoError(t, err)

		// Reaped longer than the retention period ago: deleted.
		old := insertActiveProducer(t, bundle, "client-reaped-old", now.Add(-96*time.Hour), 5*time.Minute)
		oldReap := now.Add(-25 * time.Hour)
		_, err = bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
			ClientID:   "client-reaped-old",
			Generation: old.Generation,
			Now:        &oldReap,
			QueueName:  "reaper-queue",
			Schema:     bundle.schema,
		})
		require.NoError(t, err)

		require.NoError(t, reaper.runOnce(ctx))
		riversharedtest.WaitOrTimeout(t, reaper.TestSignals.DeletedReapedBatch.WaitC())

		// Both already-reaped rows produced no offline transition here, and
		// the reaper must not publish offline for rows a prior leader (or a
		// graceful release) already transitioned.
		requireNoNotification(t, bundle.offlineCh)

		_, err = bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-reaped-old", QueueName: "reaper-queue", Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		for _, clientID := range []string{"client-active", "client-reaped-recent"} {
			_, err = bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: clientID, QueueName: "reaper-queue", Schema: bundle.schema})
			require.NoError(t, err)
		}
	})

	t.Run("LeaderChangeNeitherMissesNorDuplicatesOfflineNotification", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Leader A reaps and notifies for the first set of expired leases,
		// then stops (as after a leader election loss).
		reaperA := newReaper(t, bundle, bundle.exec)
		first := insertActiveProducer(t, bundle, "client-handoff-1", time.Now().UTC().Add(-10*time.Minute), 5*time.Minute)
		require.NoError(t, reaperA.runOnce(ctx))
		notification1 := riversharedtest.WaitOrTimeout(t, bundle.offlineCh)
		require.Equal(t, "client-handoff-1", notification1.ClientID)
		require.Equal(t, first.Generation, notification1.Generation)
		reaperA.Stop()

		// Leader B takes over. Its first reconciliation pass finds the rows
		// A already transitioned and must not re-notify, while an expired row
		// A never got to is transitioned and notified exactly once.
		reaperB := newReaper(t, bundle, bundle.exec)
		pending := insertActiveProducer(t, bundle, "client-handoff-2", time.Now().UTC().Add(-10*time.Minute), 5*time.Minute)
		require.NoError(t, reaperB.runOnce(ctx))
		notification2 := riversharedtest.WaitOrTimeout(t, bundle.offlineCh)
		require.Equal(t, "client-handoff-2", notification2.ClientID)
		require.Equal(t, pending.Generation, notification2.Generation)
		requireNoNotification(t, bundle.offlineCh)

		// Further runs from B stay quiet.
		require.NoError(t, reaperB.runOnce(ctx))
		requireNoNotification(t, bundle.offlineCh)
	})
}

// notifyFailingExecutor wraps an executor so the first NotifyMany issued on a
// transaction (after fail flips back to false) fails, simulating a transient
// notification outage. All other calls pass through.
type notifyFailingExecutor struct {
	riverdriver.Executor

	fail *atomic.Bool
}

func newNotifyFailingExecutor(exec riverdriver.Executor, fail *atomic.Bool) riverdriver.Executor {
	return &notifyFailingExecutor{Executor: exec, fail: fail}
}

func (e *notifyFailingExecutor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.Executor.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &notifyFailingExecutorTx{ExecutorTx: tx, fail: e.fail}, nil
}

type notifyFailingExecutorTx struct {
	riverdriver.ExecutorTx

	fail *atomic.Bool
}

func (tx *notifyFailingExecutorTx) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	if tx.fail.CompareAndSwap(true, false) {
		return errors.New("injected notification failure")
	}
	return tx.ExecutorTx.NotifyMany(ctx, params)
}
