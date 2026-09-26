package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

func exerciseProducer[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
		exec   riverdriver.Executor
	}

	setup := func(ctx context.Context, t *testing.T) *testBundle {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		return &testBundle{
			driver: driver,
			exec:   exec,
		}
	}

	insertProducer := func(t *testing.T, exec riverdriver.Executor, queueName, clientID string, producerID int64, now time.Time, ttl time.Duration) *riverdriver.Producer {
		t.Helper()

		producer, err := exec.ProducerInsert(ctx, &riverdriver.ProducerInsertParams{
			ClientID:   clientID,
			MaxWorkers: 10,
			Now:        &now,
			ProducerID: producerID,
			QueueName:  queueName,
			TTL:        ttl,
		})
		require.NoError(t, err)
		return producer
	}

	t.Run("ProducerInsert", func(t *testing.T) {
		t.Parallel()

		t.Run("InsertsNewProducerWithGenerationOne", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			now := time.Now().UTC()
			producer := insertProducer(t, bundle.exec, "queue-insert", "client-insert", 1001, now, 5*time.Minute)

			require.Equal(t, "queue-insert", producer.QueueName)
			require.Equal(t, "client-insert", producer.ClientID)
			require.EqualValues(t, 1001, producer.ProducerID)
			require.EqualValues(t, 1, producer.Generation)
			require.EqualValues(t, 10, producer.MaxWorkers)
			require.Nil(t, producer.ReapedAt)
			require.WithinDuration(t, now, producer.CreatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now, producer.UpdatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now.Add(5*time.Minute), producer.ExpiresAt, bundle.driver.TimePrecision())

			fetched, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  "client-insert",
				QueueName: "queue-insert",
			})
			require.NoError(t, err)
			require.Equal(t, producer.Generation, fetched.Generation)
			require.Equal(t, producer.ProducerID, fetched.ProducerID)
		})

		t.Run("IncrementsGenerationOnConflict", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			firstNow := time.Now().UTC().Add(-2 * time.Minute)
			first := insertProducer(t, bundle.exec, "queue-conflict", "client-conflict", 2001, firstNow, 5*time.Minute)
			require.EqualValues(t, 1, first.Generation)

			secondNow := time.Now().UTC()
			second, err := bundle.exec.ProducerInsert(ctx, &riverdriver.ProducerInsertParams{
				ClientID:   "client-conflict",
				MaxWorkers: 20,
				Now:        &secondNow,
				ProducerID: 2002,
				QueueName:  "queue-conflict",
				TTL:        5 * time.Minute,
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, second.Generation)
			require.EqualValues(t, 2002, second.ProducerID)
			require.EqualValues(t, 20, second.MaxWorkers)
			require.Nil(t, second.ReapedAt)
			require.WithinDuration(t, first.CreatedAt, second.CreatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, secondNow.Add(5*time.Minute), second.ExpiresAt, bundle.driver.TimePrecision())

			fetched, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  "client-conflict",
				QueueName: "queue-conflict",
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, fetched.Generation)
			require.EqualValues(t, 2002, fetched.ProducerID)
		})

		t.Run("ClearsReapedAtOnConflict", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			reapTime := time.Now().UTC().Add(-time.Minute)
			insertProducer(t, bundle.exec, "queue-reaped-reinsert", "client-reaped-reinsert", 3001, reapTime.Add(-10*time.Minute), 5*time.Minute)
			finished, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
				ClientID:   "client-reaped-reinsert",
				Generation: 1,
				Now:        &reapTime,
				QueueName:  "queue-reaped-reinsert",
			})
			require.NoError(t, err)
			require.NotNil(t, finished.ReapedAt)

			reacquired := insertProducer(t, bundle.exec, "queue-reaped-reinsert", "client-reaped-reinsert", 3002, time.Now().UTC(), 5*time.Minute)
			require.EqualValues(t, 2, reacquired.Generation)
			require.Nil(t, reacquired.ReapedAt)
		})
	})

	t.Run("ProducerGet", func(t *testing.T) {
		t.Parallel()

		t.Run("GetsExistingProducer", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			now := time.Now().UTC()
			insertProducer(t, bundle.exec, "queue-get", "client-get", 4001, now, 5*time.Minute)

			producer, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  "client-get",
				QueueName: "queue-get",
			})
			require.NoError(t, err)
			require.Equal(t, "queue-get", producer.QueueName)
			require.Equal(t, "client-get", producer.ClientID)
			require.EqualValues(t, 4001, producer.ProducerID)
			require.EqualValues(t, 1, producer.Generation)
			require.Nil(t, producer.ReapedAt)
		})

		t.Run("ReturnsNotFoundForMissingProducer", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			producer, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  "client-missing",
				QueueName: "queue-missing",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, producer)
		})
	})

	t.Run("ProducerKeepAlive", func(t *testing.T) {
		t.Parallel()

		t.Run("RenewsSameGeneration", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			startTime := time.Now().UTC().Add(-2 * time.Minute)
			producer := insertProducer(t, bundle.exec, "queue-renew", "client-renew", 5001, startTime, 5*time.Minute)
			require.WithinDuration(t, startTime.Add(5*time.Minute), producer.ExpiresAt, bundle.driver.TimePrecision())

			renewTime := startTime.Add(time.Minute)
			renewed, err := bundle.exec.ProducerKeepAlive(ctx, &riverdriver.ProducerKeepAliveParams{
				ClientID:   "client-renew",
				Generation: 1,
				Now:        &renewTime,
				QueueName:  "queue-renew",
				TTL:        10 * time.Minute,
			})
			require.NoError(t, err)
			require.EqualValues(t, 1, renewed.Generation)
			require.WithinDuration(t, renewTime, renewed.UpdatedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, renewTime.Add(10*time.Minute), renewed.ExpiresAt, bundle.driver.TimePrecision())
			require.Nil(t, renewed.ReapedAt)
		})

		t.Run("DoesNotRenewStaleGeneration", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			startTime := time.Now().UTC().Add(-2 * time.Minute)
			insertProducer(t, bundle.exec, "queue-stale", "client-stale", 5002, startTime, 5*time.Minute)
			current := insertProducer(t, bundle.exec, "queue-stale", "client-stale", 5003, time.Now().UTC(), 5*time.Minute)
			require.EqualValues(t, 2, current.Generation)

			renewTime := time.Now().UTC()
			_, err := bundle.exec.ProducerKeepAlive(ctx, &riverdriver.ProducerKeepAliveParams{
				ClientID:   "client-stale",
				Generation: 1,
				Now:        &renewTime,
				QueueName:  "queue-stale",
				TTL:        10 * time.Minute,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			fetched, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  "client-stale",
				QueueName: "queue-stale",
			})
			require.NoError(t, err)
			require.EqualValues(t, 2, fetched.Generation)
			require.WithinDuration(t, current.ExpiresAt, fetched.ExpiresAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, current.UpdatedAt, fetched.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("DoesNotRenewReapedLease", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			insertProducer(t, bundle.exec, "queue-renew-reaped", "client-renew-reaped", 5004, time.Now().UTC().Add(-10*time.Minute), 5*time.Minute)
			reapTime := time.Now().UTC().Add(-5 * time.Minute)
			_, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
				ClientID:   "client-renew-reaped",
				Generation: 1,
				Now:        &reapTime,
				QueueName:  "queue-renew-reaped",
			})
			require.NoError(t, err)

			_, err = bundle.exec.ProducerKeepAlive(ctx, &riverdriver.ProducerKeepAliveParams{
				ClientID:   "client-renew-reaped",
				Generation: 1,
				Now:        func() *time.Time { t := time.Now().UTC(); return &t }(),
				QueueName:  "queue-renew-reaped",
				TTL:        10 * time.Minute,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("ProducerFinish", func(t *testing.T) {
		t.Parallel()

		t.Run("MarksReapedAtForCurrentGeneration", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			insertProducer(t, bundle.exec, "queue-finish", "client-finish", 6001, time.Now().UTC(), 5*time.Minute)

			finishTime := time.Now().UTC()
			finished, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
				ClientID:   "client-finish",
				Generation: 1,
				Now:        &finishTime,
				QueueName:  "queue-finish",
			})
			require.NoError(t, err)
			require.NotNil(t, finished.ReapedAt)
			require.WithinDuration(t, finishTime, *finished.ReapedAt, bundle.driver.TimePrecision())
		})

		t.Run("IsIdempotent", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			insertProducer(t, bundle.exec, "queue-finish-idem", "client-finish-idem", 6002, time.Now().UTC(), 5*time.Minute)
			finishTime := time.Now().UTC()
			params := &riverdriver.ProducerFinishParams{
				ClientID:   "client-finish-idem",
				Generation: 1,
				Now:        &finishTime,
				QueueName:  "queue-finish-idem",
			}
			_, err := bundle.exec.ProducerFinish(ctx, params)
			require.NoError(t, err)

			_, err = bundle.exec.ProducerFinish(ctx, params)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("DoesNotFinishNewerGeneration", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			insertProducer(t, bundle.exec, "queue-finish-stale", "client-finish-stale", 6003, time.Now().UTC().Add(-10*time.Minute), 5*time.Minute)
			current := insertProducer(t, bundle.exec, "queue-finish-stale", "client-finish-stale", 6004, time.Now().UTC(), 5*time.Minute)

			finishTime := time.Now().UTC()
			_, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
				ClientID:   "client-finish-stale",
				Generation: 1,
				Now:        &finishTime,
				QueueName:  "queue-finish-stale",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			fetched, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  "client-finish-stale",
				QueueName: "queue-finish-stale",
			})
			require.NoError(t, err)
			require.Nil(t, fetched.ReapedAt)
			require.EqualValues(t, 2, fetched.Generation)
			require.Equal(t, current.ProducerID, fetched.ProducerID)
		})
	})

	t.Run("ProducerReapExpired", func(t *testing.T) {
		t.Parallel()

		t.Run("ReapsOnlyExpiredActiveProducers", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			now := time.Now().UTC()

			// Expired and still active: should be reaped.
			expired := insertProducer(t, bundle.exec, "queue-reap", "client-expired", 7001, now.Add(-10*time.Minute), 5*time.Minute)
			require.True(t, expired.ExpiresAt.Before(now))

			// Not yet expired: should be left alone.
			fresh := insertProducer(t, bundle.exec, "queue-reap", "client-fresh", 7002, now, 5*time.Minute)

			// Already reaped (and expired): must not be reaped or reported again.
			reaped := insertProducer(t, bundle.exec, "queue-reap", "client-reaped", 7003, now.Add(-20*time.Minute), 5*time.Minute)
			_, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
				ClientID:   "client-reaped",
				Generation: 1,
				Now:        &now,
				QueueName:  "queue-reap",
			})
			require.NoError(t, err)
			require.NotNil(t, reaped)

			reap1, err := bundle.exec.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
				Max: 10,
				Now: &now,
			})
			require.NoError(t, err)
			require.Len(t, reap1, 1)
			require.Equal(t, "client-expired", reap1[0].ClientID)
			require.NotNil(t, reap1[0].ReapedAt)
			require.EqualValues(t, 1, reap1[0].Generation)

			// Idempotent: a second reaping run transitions nothing.
			reap2, err := bundle.exec.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
				Max: 10,
				Now: &now,
			})
			require.NoError(t, err)
			require.Empty(t, reap2)

			expiredFetched, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-expired", QueueName: "queue-reap"})
			require.NoError(t, err)
			require.NotNil(t, expiredFetched.ReapedAt)

			freshFetched, err := bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-fresh", QueueName: "queue-reap"})
			require.NoError(t, err)
			require.Nil(t, freshFetched.ReapedAt)
			require.WithinDuration(t, fresh.ExpiresAt, freshFetched.ExpiresAt, bundle.driver.TimePrecision())
		})

		t.Run("RespectsBatchLimit", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			now := time.Now().UTC()
			for i, clientID := range []string{"client-batch-1", "client-batch-2", "client-batch-3"} {
				insertProducer(t, bundle.exec, "queue-reap-batch", clientID, int64(7100+i), now.Add(-10*time.Minute), 5*time.Minute)
			}

			clientIDs := map[string]struct{}{}
			for range 3 {
				reaped, err := bundle.exec.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
					Max: 1,
					Now: &now,
				})
				require.NoError(t, err)
				require.Len(t, reaped, 1)
				clientIDs[reaped[0].ClientID] = struct{}{}
			}
			require.Len(t, clientIDs, 3)

			reaped, err := bundle.exec.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
				Max: 1,
				Now: &now,
			})
			require.NoError(t, err)
			require.Empty(t, reaped)
		})
	})

	t.Run("ProducerDeleteReaped", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		now := time.Now().UTC()

		// Active producer: must never be deleted even though it's old.
		insertProducer(t, bundle.exec, "queue-delete", "client-active", 8001, now.Add(-48*time.Hour), 5*time.Minute)

		// Reaped recently: retained.
		insertProducer(t, bundle.exec, "queue-delete", "client-reaped-recent", 8002, now.Add(-48*time.Hour), 5*time.Minute)
		recentReap := now.Add(-time.Hour)
		_, err := bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
			ClientID:   "client-reaped-recent",
			Generation: 1,
			Now:        &recentReap,
			QueueName:  "queue-delete",
		})
		require.NoError(t, err)

		// Reaped longer ago than the horizon: should be deleted.
		insertProducer(t, bundle.exec, "queue-delete", "client-reaped-old", 8003, now.Add(-72*time.Hour), 5*time.Minute)
		oldReap := now.Add(-25 * time.Hour)
		_, err = bundle.exec.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
			ClientID:   "client-reaped-old",
			Generation: 1,
			Now:        &oldReap,
			QueueName:  "queue-delete",
		})
		require.NoError(t, err)

		horizon := now.Add(-24 * time.Hour)
		deleted, err := bundle.exec.ProducerDeleteReaped(ctx, &riverdriver.ProducerDeleteReapedParams{
			Max:             10,
			ReapedAtHorizon: horizon,
		})
		require.NoError(t, err)
		require.Len(t, deleted, 1)
		require.Equal(t, "client-reaped-old", deleted[0].ClientID)

		_, err = bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: "client-reaped-old", QueueName: "queue-delete"})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		for _, clientID := range []string{"client-active", "client-reaped-recent"} {
			_, err = bundle.exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: clientID, QueueName: "queue-delete"})
			require.NoError(t, err)
		}

		// Idempotent: nothing more to delete.
		deleted2, err := bundle.exec.ProducerDeleteReaped(ctx, &riverdriver.ProducerDeleteReapedParams{
			Max:             10,
			ReapedAtHorizon: horizon,
		})
		require.NoError(t, err)
		require.Empty(t, deleted2)
	})
}
