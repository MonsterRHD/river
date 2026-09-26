package riverpilot

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/timeoututil"
	"github.com/riverqueue/river/rivertype"
)

type StandardPilot struct{}

func (p *StandardPilot) JobCleanerQueuesExcluded() []string { return nil }

func (p *StandardPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state ProducerState, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	if params.MaxToLock <= 0 {
		return nil, nil
	}

	return timeoututil.WithTimeoutV(ctx, rivercommon.HotOperationTimeout, "StandardPilot.JobGetAvailable", func(ctx context.Context) ([]*rivertype.JobRow, error) {
		return exec.JobGetAvailable(ctx, params)
	})
}

func (p *StandardPilot) JobGetStuck(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	return exec.JobGetStuck(ctx, params)
}

func (p *StandardPilot) JobCancel(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return exec.JobCancel(ctx, params)
}

func (p *StandardPilot) JobInsertMany(
	ctx context.Context,
	exec riverdriver.Executor,
	params *riverdriver.JobInsertFastManyParams,
) ([]*riverdriver.JobInsertFastResult, error) {
	return exec.JobInsertFastMany(ctx, params)
}

func (p *StandardPilot) JobRescueMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	return exec.JobRescueMany(ctx, params)
}

func (p *StandardPilot) JobRetry(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	return exec.JobRetry(ctx, params)
}

func (p *StandardPilot) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return exec.JobSetStateIfRunningMany(ctx, params)
}

func (p *StandardPilot) PeriodicJobKeepAliveAndReap(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobKeepAliveAndReapParams) ([]*PeriodicJob, error) {
	return nil, nil
}

func (p *StandardPilot) PeriodicJobGetAll(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobGetAllParams) ([]*PeriodicJob, error) {
	return nil, nil
}

func (p *StandardPilot) PeriodicJobUpsertMany(ctx context.Context, exec riverdriver.Executor, params *PeriodicJobUpsertManyParams) ([]*PeriodicJob, error) {
	return nil, nil
}

func (p *StandardPilot) PilotInit(archetype *baseservice.Archetype, params *PilotInitParams) {
	// No-op
}

func (p *StandardPilot) ProducerInit(ctx context.Context, exec riverdriver.Executor, params *ProducerInitParams) (int64, ProducerState, error) {
	// Producer ID only needs to identify this particular process startup; the
	// (queue, client ID, generation) tuple is what fences lease operations, so
	// a per-acquisition random ID is sufficient.
	producerID := int64(randutil.IntBetween(1, math.MaxInt32))

	producer, err := exec.ProducerInsert(ctx, &riverdriver.ProducerInsertParams{
		ClientID:   params.ClientID,
		MaxWorkers: params.MaxWorkers,
		Now:        params.Now,
		ProducerID: producerID,
		QueueName:  params.Queue,
		Schema:     params.Schema,
		TTL:        params.TTL,
	})
	if err != nil {
		return 0, nil, err
	}

	return producer.ProducerID, &standardProducerState{generation: producer.Generation}, nil
}

func (p *StandardPilot) ProducerKeepAlive(ctx context.Context, exec riverdriver.Executor, params *riverdriver.ProducerKeepAliveParams) error {
	_, err := exec.ProducerKeepAlive(ctx, params)
	return err
}

func (p *StandardPilot) ProducerShutdown(ctx context.Context, exec riverdriver.Executor, params *ProducerShutdownParams) error {
	execTx, err := exec.Begin(ctx)
	if err != nil {
		return err
	}
	defer dbutil.RollbackWithoutCancel(ctx, execTx)

	producer, err := execTx.ProducerFinish(ctx, &riverdriver.ProducerFinishParams{
		ClientID:   params.ClientID,
		Generation: params.Generation,
		QueueName:  params.Queue,
		Schema:     params.Schema,
	})
	if err != nil {
		// The lease was already reaped by the leader after expiring, or a
		// newer generation took the slot. Either way the offline transition
		// belongs to whoever owns the current row, so don't publish another
		// notification for it.
		if errors.Is(err, rivertype.ErrNotFound) {
			return execTx.Commit(ctx)
		}
		return err
	}

	payload, err := json.Marshal(&ProducerNotificationPayload{
		Action:     ProducerNotificationActionOffline,
		ClientID:   producer.ClientID,
		Generation: producer.Generation,
		ProducerID: producer.ProducerID,
		Queue:      producer.QueueName,
	})
	if err != nil {
		return err
	}

	if err := execTx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: []string{string(payload)},
		Schema:  params.Schema,
		Topic:   ProducerNotificationTopic,
	}); err != nil {
		return err
	}

	return execTx.Commit(ctx)
}

func (p *StandardPilot) QueueMetadataChanged(ctx context.Context, exec riverdriver.Executor, params *QueueMetadataChangedParams) error {
	return nil
}

type standardProducerState struct {
	generation int64
}

func (s *standardProducerState) JobFinish(job *rivertype.JobRow) {
	// No-op
}

func (s *standardProducerState) ProducerGeneration() int64 {
	return s.generation
}
