package river

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivertype"
)

func TestStandardPilot_ProducerLeaseSQLite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		driver = riversqlite.New(nil)
		schema = riverdbtest.TestSchema(ctx, t, driver, &riverdbtest.TestSchemaOpts{
			ProcurePool: func(ctx context.Context, schema string) (any, string) {
				return riversharedtest.DBPoolSQLite(ctx, t, schema), ""
			},
		})
		exec  = driver.GetExecutor()
		pilot = &riverpilot.StandardPilot{}
	)

	const (
		clientID = "sqlite-client"
		queue    = "sqlite-queue"
	)

	startTime := time.Now().UTC().Add(-time.Second)
	producerID, state, err := pilot.ProducerInit(ctx, exec, &riverpilot.ProducerInitParams{
		ClientID:   clientID,
		MaxWorkers: 4,
		Now:        &startTime,
		Queue:      queue,
		Schema:     schema,
		TTL:        time.Minute,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, state.ProducerGeneration())

	// Same-generation renewal extends the lease.
	renewTime := startTime.Add(30 * time.Second)
	require.NoError(t, pilot.ProducerKeepAlive(ctx, exec, &riverdriver.ProducerKeepAliveParams{
		ClientID:   clientID,
		Generation: state.ProducerGeneration(),
		Now:        &renewTime,
		QueueName:  queue,
		Schema:     schema,
		TTL:        time.Minute,
	}))

	// The leader reaps the lease once it's expired.
	reaped, err := exec.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
		Max:    10,
		Now:    func() *time.Time { t := renewTime.Add(10 * time.Minute); return &t }(),
		Schema: schema,
	})
	require.NoError(t, err)
	require.Len(t, reaped, 1)
	require.NotNil(t, reaped[0].ReapedAt)

	// A new startup acquires generation two and clears the reap marker.
	newID, newState, err := pilot.ProducerInit(ctx, exec, &riverpilot.ProducerInitParams{
		ClientID:   clientID,
		MaxWorkers: 4,
		Now:        func() *time.Time { t := time.Now().UTC(); return &t }(),
		Queue:      queue,
		Schema:     schema,
		TTL:        time.Minute,
	})
	require.NoError(t, err)
	require.NotEqual(t, producerID, newID)
	require.EqualValues(t, 2, newState.ProducerGeneration())
	row, err := exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: clientID, QueueName: queue, Schema: schema})
	require.NoError(t, err)
	require.Nil(t, row.ReapedAt)

	// The old generation's heartbeat is fenced.
	err = pilot.ProducerKeepAlive(ctx, exec, &riverdriver.ProducerKeepAliveParams{
		ClientID:   clientID,
		Generation: state.ProducerGeneration(),
		Now:        func() *time.Time { t := time.Now().UTC(); return &t }(),
		QueueName:  queue,
		Schema:     schema,
		TTL:        time.Minute,
	})
	require.ErrorIs(t, err, rivertype.ErrNotFound)

	// Graceful release of the current generation marks the row and writes the
	// offline notification to the SQLite outbox in the same transaction.
	require.NoError(t, pilot.ProducerShutdown(ctx, exec, &riverpilot.ProducerShutdownParams{
		ClientID:   clientID,
		Generation: newState.ProducerGeneration(),
		ProducerID: newID,
		Queue:      queue,
		Schema:     schema,
	}))
	row, err = exec.ProducerGet(ctx, &riverdriver.ProducerGetParams{ClientID: clientID, QueueName: queue, Schema: schema})
	require.NoError(t, err)
	require.NotNil(t, row.ReapedAt)

	notificationTable := "river_notification"
	if schema != "" {
		notificationTable = dbutil.SafeIdentifier(schema) + "." + notificationTable
	}

	var notificationCount int
	require.NoError(t, exec.QueryRow(ctx,
		"SELECT count(*) FROM "+notificationTable+" WHERE topic = 'river_producer'",
	).Scan(&notificationCount))
	require.Equal(t, 1, notificationCount)

	var payload string
	require.NoError(t, exec.QueryRow(ctx,
		"SELECT payload FROM "+notificationTable+" WHERE topic = 'river_producer'",
	).Scan(&payload))
	require.Contains(t, payload, `"generation":2`)
	require.Contains(t, payload, `"action":"offline"`)
	require.Contains(t, payload, `"client_id":"sqlite-client"`)
}
