package river

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestClient_ProducerLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	producerCount := func(t *testing.T, exec riverdriver.Executor, schema string) int {
		t.Helper()

		var count int64
		require.NoError(t, exec.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.river_producer", schema)).Scan(&count))
		return int(count)
	}

	t.Run("EnqueueOnlyClientCreatesNoLease", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		// A client configured without queues never starts a producer or the
		// leader-only reaper, so inserting jobs must not create a lease row.
		client, err := NewClient(driver, &Config{
			Logger:   riversharedtest.LoggerWarn(t),
			TestOnly: true,
			Schema:   schema,
		})
		require.NoError(t, err)

		_, err = client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)
		_, err = client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		require.Zero(t, producerCount(t, driver.GetExecutor(), schema))

		// Stop on a client that never started working jobs is a no-op and
		// still doesn't create a lease.
		require.NoError(t, client.Stop(ctx))
		require.Zero(t, producerCount(t, driver.GetExecutor(), schema))
	})

	t.Run("WorkingClientAcquiresAndReleasesLease", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			exec   = driver.GetExecutor()
		)

		config := newTestConfig(t, schema)
		client := newTestClient(t, dbPool, config)
		require.NoError(t, client.Start(ctx))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_ = client.Stop(ctx)
		})

		// The producer acquires an active lease shortly after start.
		var producer *riverdriver.Producer
		require.Eventually(t, func() bool {
			row, err := exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
				ClientID:  client.config.ID,
				QueueName: QueueDefault,
				Schema:    schema,
			})
			if err != nil {
				return false
			}
			producer = row
			return true
		}, 3*time.Second, 20*time.Millisecond)
		require.Nil(t, producer.ReapedAt)
		require.EqualValues(t, 1, producer.Generation)
		require.Equal(t, 50, int(producer.MaxWorkers))
		require.True(t, producer.ExpiresAt.After(time.Now().UTC()))

		// Graceful client stop actively releases the lease rather than waiting
		// for expiry and reaping.
		require.NoError(t, client.Stop(ctx))

		released, err := exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{
			ClientID:  client.config.ID,
			QueueName: QueueDefault,
			Schema:    schema,
		})
		require.NoError(t, err)
		require.NotNil(t, released.ReapedAt)
		require.Equal(t, producer.Generation, released.Generation)
	})
}
