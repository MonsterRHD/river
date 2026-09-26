package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

func exerciseQueueDrain[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
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

	t.Run("Complete", func(t *testing.T) {
		t.Parallel()

		t.Run("BlockedWhileRunningJobsExist", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			now := time.Now().UTC()
			runningJob := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				Queue: new("drain-complete-blocked"),
				State: new(rivertype.JobStateRunning),
			})

			drain, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-blocked",
				Now:   &now,
				Queue: runningJob.Queue,
			})
			require.NoError(t, err)
			require.Equal(t, riverdriver.QueueDrainStateDraining, drain.State)

			rowsAffected, err := bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Now:   &now,
				Queue: runningJob.Queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), rowsAffected)

			drain, err = bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: runningJob.Queue,
			})
			require.NoError(t, err)
			require.Equal(t, riverdriver.QueueDrainStateDraining, drain.State)
			require.Nil(t, drain.DrainedAt)
			require.Equal(t, int64(1), drain.RunningCount)
		})

		t.Run("CompletesAtZeroAndOnlyOnce", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			queue := "drain-complete-zero"
			drainedAt := time.Now().UTC()

			// An available job is not running and must not block completion.
			testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				Queue: new(queue),
				State: new(rivertype.JobStateAvailable),
			})

			_, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-zero",
				Queue: queue,
			})
			require.NoError(t, err)

			rowsAffected, err := bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Now:   &drainedAt,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), rowsAffected)

			drain, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, riverdriver.QueueDrainStateDrained, drain.State)
			require.NotNil(t, drain.DrainedAt)
			require.WithinDuration(t, drainedAt, *drain.DrainedAt, bundle.driver.TimePrecision())
			require.Equal(t, int64(0), drain.RunningCount)

			// Completing again affects nothing because the row is already drained.
			rowsAffected, err = bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Now:   &drainedAt,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), rowsAffected)
		})

		t.Run("UnblocksWhenRunningJobFinalizes", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			queue := "drain-complete-finalized"
			runningJob := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				Queue: new(queue),
				State: new(rivertype.JobStateRunning),
			})

			_, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-finalized",
				Queue: queue,
			})
			require.NoError(t, err)

			rowsAffected, err := bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), rowsAffected)

			finalizedAt := time.Now().UTC()
			_, err = bundle.exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
				FinalizedAt:         &finalizedAt,
				FinalizedAtDoUpdate: true,
				ID:                  runningJob.ID,
				State:               rivertype.JobStateCompleted,
				StateDoUpdate:       true,
			})
			require.NoError(t, err)

			rowsAffected, err = bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), rowsAffected)

			drain, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, riverdriver.QueueDrainStateDrained, drain.State)
		})
	})

	t.Run("DeleteResumed", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		oldTime := time.Now().UTC().Add(-2 * time.Hour)
		horizon := time.Now().UTC().Add(-time.Hour)

		// Resumed drain older than the horizon: deleted.
		insertResumed := func(queue string, updatedAt time.Time) {
			_, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-" + queue,
				Now:   &oldTime,
				Queue: queue,
			})
			require.NoError(t, err)
			_, err = bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Now: &oldTime,

				Queue: queue,
			})
			require.NoError(t, err)
			_, err = bundle.exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
				Now:   &updatedAt,
				Queue: queue,
			})
			require.NoError(t, err)
		}

		insertResumed("drain-delete-old", oldTime)
		insertResumed("drain-delete-recent", time.Now().UTC())

		// Active drains are never deleted regardless of age.
		_, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:   "key-active",
			Now:   &oldTime,
			Queue: "drain-delete-active",
		})
		require.NoError(t, err)

		rowsAffected, err := bundle.exec.QueueDrainDeleteResumed(ctx, &riverdriver.QueueDrainDeleteResumedParams{
			UpdatedAtHorizon: horizon,
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		_, err = bundle.exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
			Key:   "key-drain-delete-old",
			Queue: "drain-delete-old",
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		recent, err := bundle.exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
			Key:   "key-drain-delete-recent",
			Queue: "drain-delete-recent",
		})
		require.NoError(t, err)
		require.Equal(t, riverdriver.QueueDrainStateResumed, recent.State)

		active, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
			Queue: "drain-delete-active",
		})
		require.NoError(t, err)
		require.Equal(t, riverdriver.QueueDrainStateDraining, active.State)
	})

	t.Run("GetActive", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		queue := "drain-get-active"
		now := time.Now().UTC()

		_, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
			Queue: queue,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		_, err = bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:   "key-get-active",
			Now:   &now,
			Queue: queue,
		})
		require.NoError(t, err)

		drain, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
			Queue: queue,
		})
		require.NoError(t, err)
		require.Equal(t, queue, drain.Queue)
		require.Equal(t, "key-get-active", drain.Key)
		require.Equal(t, riverdriver.QueueDrainStateDraining, drain.State)
		require.WithinDuration(t, now, drain.CreatedAt, bundle.driver.TimePrecision())
		require.Nil(t, drain.DrainedAt)
		require.Nil(t, drain.ResumedAt)
		require.Equal(t, int64(0), drain.RunningCount)

		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Queue: new(queue),
			State: new(rivertype.JobStateRunning),
		})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Queue: new(queue),
			State: new(rivertype.JobStateRunning),
		})
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Queue: new(queue),
			State: new(rivertype.JobStateAvailable),
		})
		// Running jobs on another queue don't count.
		testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Queue: new("drain-get-active-other-queue"),
			State: new(rivertype.JobStateRunning),
		})

		drain, err = bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
			Queue: queue,
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), drain.RunningCount)
	})

	t.Run("GetByKey", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		queue := "drain-get-by-key"

		_, err := bundle.exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
			Key:   "missing-key",
			Queue: queue,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		_, err = bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
			Key:   "key-get-by-key",
			Queue: queue,
		})
		require.NoError(t, err)

		drain, err := bundle.exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
			Key:   "key-get-by-key",
			Queue: queue,
		})
		require.NoError(t, err)
		require.Equal(t, queue, drain.Queue)
		require.Equal(t, "key-get-by-key", drain.Key)
		require.Equal(t, riverdriver.QueueDrainStateDraining, drain.State)
	})

	t.Run("Insert", func(t *testing.T) {
		t.Parallel()

		t.Run("SameKeyIsIdempotent", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			queue := "drain-insert-idempotent"
			now := time.Now().UTC()

			drain, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-idempotent",
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)
			require.NotNil(t, drain)

			conflictDrain, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-idempotent",
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Nil(t, conflictDrain) // ON CONFLICT DO NOTHING returns no row

			active, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, drain.Key, active.Key)
			require.WithinDuration(t, drain.CreatedAt, active.CreatedAt, bundle.driver.TimePrecision())
			require.Equal(t, drain.State, active.State)
		})

		t.Run("DifferentKeyWhileActiveConflicts", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			queue := "drain-insert-conflict"

			_, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key: "key-one",

				Queue: queue,
			})
			require.NoError(t, err)

			conflictDrain, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-two",
				Queue: queue,
			})
			require.NoError(t, err)
			require.Nil(t, conflictDrain) // blocked by the partial unique index

			active, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, "key-one", active.Key)
		})
	})

	t.Run("Resume", func(t *testing.T) {
		t.Parallel()

		t.Run("DrainsThenResumesThenReinserts", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			queue := "drain-resume-lifecycle"
			now := time.Now().UTC()

			// Resume while draining affects nothing.
			drain, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-lifecycle",
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)

			rowsAffected, err := bundle.exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), rowsAffected)

			// Drain, then resume successfully.
			_, err = bundle.exec.QueueDrainComplete(ctx, &riverdriver.QueueDrainCompleteParams{
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)

			rowsAffected, err = bundle.exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(1), rowsAffected)

			// Resuming again affects nothing.
			rowsAffected, err = bundle.exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), rowsAffected)

			// The resumed row is no longer active but remains fetchable by key.
			_, err = bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: queue,
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)

			historical, err := bundle.exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
				Key:   drain.Key,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, riverdriver.QueueDrainStateResumed, historical.State)
			require.NotNil(t, historical.DrainedAt)
			require.NotNil(t, historical.ResumedAt)
			require.WithinDuration(t, now, *historical.ResumedAt, bundle.driver.TimePrecision())

			// A new drain can start once the previous handoff is resumed.
			newDrain, err := bundle.exec.QueueDrainInsert(ctx, &riverdriver.QueueDrainInsertParams{
				Key:   "key-lifecycle-2",
				Now:   &now,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, "key-lifecycle-2", newDrain.Key)

			active, err := bundle.exec.QueueDrainGetActive(ctx, &riverdriver.QueueDrainGetParams{
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, "key-lifecycle-2", active.Key)

			// The historical row is still present under its own key.
			historical, err = bundle.exec.QueueDrainGetByKey(ctx, &riverdriver.QueueDrainGetByKeyParams{
				Key:   drain.Key,
				Queue: queue,
			})
			require.NoError(t, err)
			require.Equal(t, riverdriver.QueueDrainStateResumed, historical.State)
		})

		t.Run("MissingDrainIsNoop", func(t *testing.T) {
			t.Parallel()

			bundle := setup(ctx, t)

			rowsAffected, err := bundle.exec.QueueDrainResume(ctx, &riverdriver.QueueDrainResumeParams{
				Queue: "drain-resume-missing",
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), rowsAffected)
		})
	})
}
