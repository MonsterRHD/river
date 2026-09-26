package river

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

func TestStandardPilot_ProducerLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec      riverdriver.Executor
		pilot     *riverpilot.StandardPilot
		offlineCh chan *riverpilot.ProducerNotificationPayload
		schema    string
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			driver    = riverpgxv5.New(riversharedtest.DBPool(ctx, t))
			schema    = riverdbtest.TestSchema(ctx, t, driver, nil)
			exec      = driver.GetExecutor()
			offlineCh = make(chan *riverpilot.ProducerNotificationPayload, 16)
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
			exec:      exec,
			pilot:     &riverpilot.StandardPilot{},
			offlineCh: offlineCh,
			schema:    schema,
		}
	}

	t.Run("AcquireKeepAliveAndRelease", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		startTime := time.Now().UTC().Add(-time.Second)
		producerID, state, err := bundle.pilot.ProducerInit(ctx, bundle.exec, &riverpilot.ProducerInitParams{
			ClientID:   "pilot-client",
			MaxWorkers: 5,
			Now:        &startTime,
			Queue:      "pilot-queue",
			Schema:     bundle.schema,
			TTL:        5 * time.Minute,
		})
		require.NoError(t, err)
		require.NotZero(t, producerID)
		require.EqualValues(t, 1, state.ProducerGeneration())

		row, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "pilot-client", QueueName: "pilot-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.Equal(t, producerID, row.ProducerID)
		require.EqualValues(t, 1, row.Generation)
		require.Equal(t, 5, int(row.MaxWorkers))
		require.Nil(t, row.ReapedAt)
		require.WithinDuration(t, startTime.Add(5*time.Minute), row.ExpiresAt, time.Millisecond)

		renewTime := startTime.Add(time.Minute)
		require.NoError(t, bundle.pilot.ProducerKeepAlive(ctx, bundle.exec, &riverdriver.ProducerKeepAliveParams{
			ClientID:   "pilot-client",
			Generation: state.ProducerGeneration(),
			Now:        &renewTime,
			QueueName:  "pilot-queue",
			Schema:     bundle.schema,
			TTL:        5 * time.Minute,
		}))
		row, err = bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "pilot-client", QueueName: "pilot-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.WithinDuration(t, renewTime.Add(5*time.Minute), row.ExpiresAt, time.Millisecond)
		require.Nil(t, row.ReapedAt)

		// Graceful release marks the lease and publishes exactly one offline
		// notification in the same transaction.
		require.NoError(t, bundle.pilot.ProducerShutdown(ctx, bundle.exec, &riverpilot.ProducerShutdownParams{
			ClientID:   "pilot-client",
			Generation: state.ProducerGeneration(),
			ProducerID: producerID,
			Queue:      "pilot-queue",
			Schema:     bundle.schema,
		}))
		notification := riversharedtest.WaitOrTimeout(t, bundle.offlineCh)
		require.Equal(t, riverpilot.ProducerNotificationActionOffline, notification.Action)
		require.Equal(t, "pilot-client", notification.ClientID)
		require.Equal(t, "pilot-queue", notification.Queue)
		require.Equal(t, producerID, notification.ProducerID)
		require.EqualValues(t, 1, notification.Generation)

		row, err = bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "pilot-client", QueueName: "pilot-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.NotNil(t, row.ReapedAt)

		// A repeat release is fenced: no error, no second notification.
		require.NoError(t, bundle.pilot.ProducerShutdown(ctx, bundle.exec, &riverpilot.ProducerShutdownParams{
			ClientID:   "pilot-client",
			Generation: state.ProducerGeneration(),
			ProducerID: producerID,
			Queue:      "pilot-queue",
			Schema:     bundle.schema,
		}))
		select {
		case notification := <-bundle.offlineCh:
			require.Failf(t, "unexpected second offline notification", "%+v", notification)
		case <-time.After(300 * time.Millisecond):
		}
	})

	t.Run("StaleGenerationCannotRenewOrReleaseNewerLease", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// An older process (generation one) and the current process
		// (generation two) share the same client ID/queue slot.
		oldID, oldState, err := bundle.pilot.ProducerInit(ctx, bundle.exec, &riverpilot.ProducerInitParams{
			ClientID:   "shared-client",
			MaxWorkers: 3,
			Now:        func() *time.Time { t := time.Now().UTC().Add(-time.Minute); return &t }(),
			Queue:      "shared-queue",
			Schema:     bundle.schema,
			TTL:        5 * time.Minute,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, oldState.ProducerGeneration())

		newID, newState, err := bundle.pilot.ProducerInit(ctx, bundle.exec, &riverpilot.ProducerInitParams{
			ClientID:   "shared-client",
			MaxWorkers: 3,
			Now:        func() *time.Time { t := time.Now().UTC(); return &t }(),
			Queue:      "shared-queue",
			Schema:     bundle.schema,
			TTL:        5 * time.Minute,
		})
		require.NoError(t, err)
		require.NotEqual(t, oldID, newID)
		require.EqualValues(t, 2, newState.ProducerGeneration())

		// The old generation's late heartbeat can't touch the new row.
		err = bundle.pilot.ProducerKeepAlive(ctx, bundle.exec, &riverdriver.ProducerKeepAliveParams{
			ClientID:   "shared-client",
			Generation: oldState.ProducerGeneration(),
			Now:        func() *time.Time { t := time.Now().UTC(); return &t }(),
			QueueName:  "shared-queue",
			Schema:     bundle.schema,
			TTL:        5 * time.Minute,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		// Nor can its release mark the new row reaped or publish anything.
		require.NoError(t, bundle.pilot.ProducerShutdown(ctx, bundle.exec, &riverpilot.ProducerShutdownParams{
			ClientID:   "shared-client",
			Generation: oldState.ProducerGeneration(),
			ProducerID: oldID,
			Queue:      "shared-queue",
			Schema:     bundle.schema,
		}))
		select {
		case <-bundle.offlineCh:
			require.Fail(t, "stale generation must not publish an offline notification")
		case <-time.After(300 * time.Millisecond):
		}

		row, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "shared-client", QueueName: "shared-queue", Schema: bundle.schema})
		require.NoError(t, err)
		require.EqualValues(t, 2, row.Generation)
		require.Nil(t, row.ReapedAt)

		// The current generation can renew and then release.
		require.NoError(t, bundle.pilot.ProducerKeepAlive(ctx, bundle.exec, &riverdriver.ProducerKeepAliveParams{
			ClientID:   "shared-client",
			Generation: newState.ProducerGeneration(),
			Now:        func() *time.Time { t := time.Now().UTC(); return &t }(),
			QueueName:  "shared-queue",
			Schema:     bundle.schema,
			TTL:        5 * time.Minute,
		}))
		require.NoError(t, bundle.pilot.ProducerShutdown(ctx, bundle.exec, &riverpilot.ProducerShutdownParams{
			ClientID:   "shared-client",
			Generation: newState.ProducerGeneration(),
			ProducerID: newID,
			Queue:      "shared-queue",
			Schema:     bundle.schema,
		}))
		notification := riversharedtest.WaitOrTimeout(t, bundle.offlineCh)
		require.EqualValues(t, 2, notification.Generation)
	})
}
