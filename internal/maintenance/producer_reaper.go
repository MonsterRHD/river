package maintenance

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/circuitbreaker"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeoututil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
)

const (
	ProducerReaperIntervalDefault = 5 * time.Second

	// ProducerReapedRetentionPeriodDefault is how long a reaped producer row
	// is retained after going offline. The row backs generation fencing and
	// exactly-once offline notifications, so it's kept for a while after a
	// leader change, then physically deleted.
	ProducerReapedRetentionPeriodDefault = 24 * time.Hour
)

// ProducerReaperTestSignals are internal signals used exclusively in tests.
type ProducerReaperTestSignals struct {
	DeletedReapedBatch testsignal.TestSignal[struct{}]              // notifies each time a batch of reaped rows is physically deleted
	ReapedProducer     testsignal.TestSignal[*riverdriver.Producer] // notifies for each producer reaped after lease expiry
}

func (ts *ProducerReaperTestSignals) Init(tb testutil.TestingTB) {
	ts.DeletedReapedBatch.Init(tb)
	ts.ReapedProducer.Init(tb)
}

// ProducerReaperConfig is the configuration for ProducerReaper.
type ProducerReaperConfig struct {
	riversharedmaintenance.BatchSizes

	// Interval is the amount of time to wait between reaper runs.
	Interval time.Duration

	// ReapedRetentionPeriod is how long a producer row is retained after
	// being reaped before it's physically deleted.
	ReapedRetentionPeriod time.Duration

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string
}

func (c *ProducerReaperConfig) mustValidate() *ProducerReaperConfig {
	c.MustValidate()

	if c.Interval <= 0 {
		panic("ProducerReaperConfig.Interval must be above zero")
	}
	if c.ReapedRetentionPeriod <= 0 {
		panic("ProducerReaperConfig.ReapedRetentionPeriod must be above zero")
	}

	return c
}

// ProducerReaper reaps producer leases that have expired without a successful
// keepalive, publishing an offline notification for each one. It also
// physically deletes rows that were reaped long ago. It runs only on the
// client which has been elected leader at any given time.
//
// State changes and their notifications always commit together, and the
// reaping predicates match only active (`reaped_at IS NULL`) rows, so a
// leader change can't duplicate an offline notification: a row either hasn't
// transitioned yet (new leader transitions it and notifies) or already has
// (new leader skips it).
type ProducerReaper struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *ProducerReaperConfig
	TestSignals ProducerReaperTestSignals

	exec riverdriver.Executor

	// See QueueCleaner.reducedBatchSizeBreaker.
	reducedBatchSizeBreaker *circuitbreaker.CircuitBreaker
}

func NewProducerReaper(archetype *baseservice.Archetype, config *ProducerReaperConfig, exec riverdriver.Executor) *ProducerReaper {
	batchSizes := config.WithDefaults()

	return baseservice.Init(archetype, &ProducerReaper{
		Config: (&ProducerReaperConfig{
			BatchSizes:            batchSizes,
			Interval:              cmp.Or(config.Interval, ProducerReaperIntervalDefault),
			ReapedRetentionPeriod: cmp.Or(config.ReapedRetentionPeriod, ProducerReapedRetentionPeriodDefault),
			Schema:                config.Schema,
		}).mustValidate(),
		exec:                    exec,
		reducedBatchSizeBreaker: riversharedmaintenance.ReducedBatchSizeBreaker(batchSizes),
	})
}

func (s *ProducerReaper) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStarted)
		defer s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStopped)

		ticker := timeutil.NewTickerWithInitialTick(ctx, s.Config.Interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			if err := s.runOnce(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.Logger.ErrorContext(ctx, s.Name+": Error reaping producers", slog.String("error", err.Error()))
				}
				// Nothing in-memory to reset: the durable rows are
				// authoritative and the next tick reconciles from them.
				continue
			}
		}
	}()

	return nil
}

func (s *ProducerReaper) batchSize() int {
	if s.reducedBatchSizeBreaker.Open() {
		return s.Config.Reduced
	}
	return s.Config.Default
}

func (s *ProducerReaper) runOnce(ctx context.Context) error {
	now := time.Now().UTC()

	if err := s.reapExpiredProducers(ctx, now); err != nil {
		return err
	}

	return s.deleteReapedProducers(ctx, now.Add(-s.Config.ReapedRetentionPeriod))
}

// reapExpiredProducers marks expired, still-active producer leases as reaped
// and publishes an offline notification for each. Every batch transitions and
// notifies in one transaction, so a failure (including a failed notification)
// rolls the batch back and is retried on the next run without missing or
// duplicating the offline change.
func (s *ProducerReaper) reapExpiredProducers(ctx context.Context, now time.Time) error {
	for {
		var reaped []*riverdriver.Producer

		err := timeoututil.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault, s.Name+".reapExpiredProducers", func(ctx context.Context) error {
			var err error
			reaped, err = s.reapExpiredBatch(ctx, now)
			return err
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.reducedBatchSizeBreaker.Trip()
			}

			return fmt.Errorf("error reaping expired producers: %w", err)
		}

		s.reducedBatchSizeBreaker.ResetIfNotOpen()

		for _, producer := range reaped {
			s.TestSignals.ReapedProducer.Signal(producer)
		}

		if len(reaped) < s.batchSize() {
			return nil
		}

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}
}

func (s *ProducerReaper) reapExpiredBatch(ctx context.Context, now time.Time) ([]*riverdriver.Producer, error) {
	execTx, err := s.exec.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}
	defer dbutil.RollbackWithoutCancel(ctx, execTx)

	reaped, err := execTx.ProducerReapExpired(ctx, &riverdriver.ProducerReapExpiredParams{
		Max:    s.batchSize(),
		Now:    &now,
		Schema: s.Config.Schema,
	})
	if err != nil {
		return nil, err
	}

	if len(reaped) > 0 {
		payloads := make([]string, len(reaped))
		for i, producer := range reaped {
			payload, err := json.Marshal(&riverpilot.ProducerNotificationPayload{
				Action:     riverpilot.ProducerNotificationActionOffline,
				ClientID:   producer.ClientID,
				Generation: producer.Generation,
				ProducerID: producer.ProducerID,
				Queue:      producer.QueueName,
			})
			if err != nil {
				return nil, err
			}
			payloads[i] = string(payload)
		}

		if err := execTx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: payloads,
			Schema:  s.Config.Schema,
			Topic:   string(riverpilot.ProducerNotificationTopic),
		}); err != nil {
			return nil, err
		}
	}

	if err := execTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return reaped, nil
}

// deleteReapedProducers physically deletes producer rows reaped before the
// given retention horizon.
func (s *ProducerReaper) deleteReapedProducers(ctx context.Context, horizon time.Time) error {
	for {
		var deleted int

		err := timeoututil.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault, s.Name+".deleteReapedProducers", func(ctx context.Context) error {
			deletedProducers, err := s.exec.ProducerDeleteReaped(ctx, &riverdriver.ProducerDeleteReapedParams{
				Max:             s.batchSize(),
				ReapedAtHorizon: horizon,
				Schema:          s.Config.Schema,
			})
			if err != nil {
				return err
			}

			s.reducedBatchSizeBreaker.ResetIfNotOpen()
			deleted = len(deletedProducers)
			return nil
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.reducedBatchSizeBreaker.Trip()
			}

			return fmt.Errorf("error deleting reaped producers: %w", err)
		}

		if deleted > 0 {
			s.TestSignals.DeletedReapedBatch.Signal(struct{}{})
		}

		// Deleted was less than query `LIMIT` which means work is done.
		if deleted < s.batchSize() {
			return nil
		}

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}
}
