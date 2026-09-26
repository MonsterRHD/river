package jobcompleter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/timeoututil"
	"github.com/riverqueue/river/rivertype"
)

// JobCompleter is an interface to a service that "completes" jobs by marking
// them with an appropriate state and any other necessary metadata in the
// database. It's a generic interface to let us experiment with the speed of a
// number of implementations, although River will likely always prefer our most
// optimized one.
type JobCompleter interface {
	startstop.Service

	// JobSetState sets a new state for the given job, as long as it's
	// still running (i.e. its state has not changed to something else already).
	JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error

	// ResetSubscribeChan resets the subscription channel for the completer. It
	// must only be called when the completer is stopped.
	ResetSubscribeChan(subscribeCh SubscribeChan)
}

// ServiceWithStopError is implemented by completers that can fail to finish
// all accepted work during Stop. StopError is only meaningful once Stop has
// returned. A non-nil result means one or more accepted job results were not
// persisted; the affected jobs remain in their previous (usually running)
// state in the database and are recoverable by the job rescuer.
type ServiceWithStopError interface {
	StopError() error
}

type SubscribeChan chan<- []CompleterJobUpdated

// SubscribeFunc will be invoked whenever a job is updated.
type SubscribeFunc func(update CompleterJobUpdated)

type CompleterJobUpdated struct {
	Job      *rivertype.JobRow
	JobStats *jobstats.JobStatistics
	Reason   riverdriver.JobSetStateReason
}

func completerJobUpdatedFromStateAndReason(job *rivertype.JobRow, stats *jobstats.JobStatistics, requestedReason riverdriver.JobSetStateReason) CompleterJobUpdated {
	var reason riverdriver.JobSetStateReason
	switch job.State {
	case rivertype.JobStateAvailable:
		// Available is ambiguous: failed jobs being retried immediately, short
		// snoozes, and jobs interrupted by client shutdown all use it. Preserve
		// the requested reason when it describes one of those transitions.
		switch requestedReason {
		case riverdriver.JobSetStateReasonFailed, riverdriver.JobSetStateReasonInterrupted, riverdriver.JobSetStateReasonSnoozed:
			reason = requestedReason
		case riverdriver.JobSetStateReasonCancelled, riverdriver.JobSetStateReasonCompleted:
			reason = riverdriver.JobSetStateReasonFailed
		default:
			panic("completion subscriber received an unknown reason, river bug")
		}
	case rivertype.JobStateCancelled:
		reason = riverdriver.JobSetStateReasonCancelled

	case rivertype.JobStateCompleted:
		reason = riverdriver.JobSetStateReasonCompleted

	case rivertype.JobStateDiscarded, rivertype.JobStateRetryable:
		reason = riverdriver.JobSetStateReasonFailed

	case rivertype.JobStateScheduled:
		reason = riverdriver.JobSetStateReasonSnoozed

	case rivertype.JobStatePending, rivertype.JobStateRunning:
		// Neither state represents a finalized job, so emitting a completion
		// event would be misleading. Reaching this case indicates a River bug or
		// that the job's state was changed out of band during finalization.
		panic("completion subscriber received a job that wasn't finalized, river bug")

	default:
		// linter exhaustive rule prevents this from being reached.
		panic("completion subscriber received a job with an unknown state, river bug")
	}

	return CompleterJobUpdated{
		Job:      job,
		JobStats: stats,
		Reason:   reason,
	}
}

type InlineCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	disableSleep bool // disable sleep in testing
	exec         riverdriver.Executor
	pilot        riverpilot.Pilot
	schema       string
	subscribeCh  SubscribeChan

	// A waitgroup is not actually needed for the inline completer because as
	// long as the caller is waiting on each function call, completion is
	// guaranteed to be done by the time Wait is called. However, we use a
	// generic test helper for all completers that starts goroutines, so this
	// left in for now for the benefit of the test suite.
	wg sync.WaitGroup
}

func NewInlineCompleter(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan) *InlineCompleter {
	return baseservice.Init(archetype, &InlineCompleter{
		exec:        exec,
		pilot:       pilot,
		schema:      schema,
		subscribeCh: subscribeCh,
	})
}

func (c *InlineCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	c.wg.Add(1)
	defer c.wg.Done()

	start := c.Time.Now()

	jobs, err := withRetries(context.WithoutCancel(ctx), ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
		jobs, err := c.pilot.JobSetStateIfRunningMany(ctx, c.exec, setStateParamsToMany(c.Time.NowOrNil(), c.schema, params))
		if err != nil {
			return nil, err
		}

		return jobs, nil
	})
	if err != nil {
		return err
	}

	// The driver intentionally returns 0 rows when a job is deleted while the
	// completer is finalizing it (see UnknownJobIgnored shared driver test).
	// Guard against an index-out-of-range panic in that case.
	if len(jobs) < 1 {
		return nil
	}

	stats.CompleteDuration = c.Time.Now().Sub(start)
	c.subscribeCh <- []CompleterJobUpdated{completerJobUpdatedFromStateAndReason(jobs[0], stats, params.Reason)}

	return nil
}

func (c *InlineCompleter) ResetSubscribeChan(subscribeCh SubscribeChan) {
	c.subscribeCh = subscribeCh
}

// StopError always returns nil for the inline completer because every
// completion is performed synchronously by its caller, so any error is already
// returned directly from JobSetStateIfRunning.
func (c *InlineCompleter) StopError() error {
	return nil
}

func (c *InlineCompleter) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := c.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if c.subscribeCh == nil {
		panic("subscribeCh must be non-nil")
	}

	go func() {
		started()
		defer stopped()
		defer close(c.subscribeCh)

		<-ctx.Done()

		c.wg.Wait()
	}()

	return nil
}

func setStateParamsToMany(now *time.Time, schema string, params *riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
	return &riverdriver.JobSetStateIfRunningManyParams{
		Attempt:         []*int{params.Attempt},
		ErrData:         [][]byte{params.ErrData},
		FinalizedAt:     []*time.Time{params.FinalizedAt},
		ID:              []int64{params.ID},
		MetadataDoMerge: []bool{params.MetadataDoMerge},
		MetadataUpdates: [][]byte{params.MetadataUpdates},
		Now:             now,
		ScheduledAt:     []*time.Time{params.ScheduledAt},
		Schema:          schema,
		State:           []rivertype.JobState{params.State},
	}
}

// A default concurrency of 100 seems to perform better a much smaller number
// like 10, but it's quite dependent on environment (10 and 100 bench almost
// identically on MBA when it's on battery power). This number should represent
// our best known default for most use cases, but don't consider its choice to
// be particularly well informed at this point.
const asyncCompleterDefaultConcurrency = 100

type AsyncCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	concurrency  int
	disableSleep bool // disable sleep in testing
	errGroup     *errgroup.Group
	exec         riverdriver.Executor
	pilot        riverpilot.Pilot
	schema       string
	subscribeCh  SubscribeChan

	stopErrMu sync.Mutex
	stopErr   error
}

func NewAsyncCompleter(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan) *AsyncCompleter {
	return newAsyncCompleterWithConcurrency(archetype, schema, exec, pilot, asyncCompleterDefaultConcurrency, subscribeCh)
}

func newAsyncCompleterWithConcurrency(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, concurrency int, subscribeCh SubscribeChan) *AsyncCompleter {
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(concurrency)

	return baseservice.Init(archetype, &AsyncCompleter{
		concurrency: concurrency,
		errGroup:    errGroup,
		exec:        exec,
		pilot:       pilot,
		schema:      schema,
		subscribeCh: subscribeCh,
	})
}

func (c *AsyncCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	// Start clock outside of goroutine so that the time spent blocking waiting
	// for an errgroup slot is accurately measured.
	start := c.Time.Now()

	c.errGroup.Go(func() error {
		jobs, err := withRetries(context.WithoutCancel(ctx), ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
			rows, err := c.pilot.JobSetStateIfRunningMany(ctx, c.exec, setStateParamsToMany(c.Time.NowOrNil(), c.schema, params))
			if err != nil {
				return nil, err
			}

			return rows, nil
		})
		if err != nil {
			return err
		}

		// The driver intentionally returns 0 rows when a job is deleted while the
		// completer is finalizing it (see UnknownJobIgnored shared driver test).
		// Guard against an index-out-of-range panic in that case.
		if len(jobs) < 1 {
			return nil
		}

		stats.CompleteDuration = c.Time.Now().Sub(start)
		c.subscribeCh <- []CompleterJobUpdated{completerJobUpdatedFromStateAndReason(jobs[0], stats, params.Reason)}

		return nil
	})
	return nil
}

func (c *AsyncCompleter) ResetSubscribeChan(subscribeCh SubscribeChan) {
	c.subscribeCh = subscribeCh
}

// StopError returns the first error that prevented an asynchronously completed
// job from being persisted, if any. It's only meaningful after Stop has
// returned.
func (c *AsyncCompleter) StopError() error {
	c.stopErrMu.Lock()
	defer c.stopErrMu.Unlock()

	return c.stopErr
}

func (c *AsyncCompleter) setStopErr(err error) {
	c.stopErrMu.Lock()
	defer c.stopErrMu.Unlock()

	// Preserve the first failure, but allow resetting on restart.
	if err != nil && c.stopErr != nil {
		return
	}
	c.stopErr = err
}

func (c *AsyncCompleter) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := c.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if c.subscribeCh == nil {
		panic("subscribeCh must be non-nil")
	}

	// Clear any stop error from a previous run so it can't surface after a
	// successful restart.
	c.setStopErr(nil)

	go func() {
		started()
		defer stopped() // this defer should come first so it's first out
		defer close(c.subscribeCh)

		<-ctx.Done()

		if err := c.errGroup.Wait(); err != nil {
			c.Logger.ErrorContext(ctx, "Error waiting on async completer", "err", err)
			c.setStopErr(err)
		}
	}()

	return nil
}

type batchCompleterSetState struct {
	Params    *riverdriver.JobSetStateIfRunningParams
	StartTime time.Time
	Stats     *jobstats.JobStatistics
}

// BatchCompleter accumulates incoming completions, and instead of completing
// them immediately, every so often complete many of them as a single efficient
// batch. To minimize the amount of driver surface area we need, the batching is
// only performed for jobs being changed to a `completed` state, which we expect
// to the vast common case under normal operation. The completer embeds an
// AsyncCompleter to perform other non-`completed` state completions.
type BatchCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	backlogWaitThreshold int // configurable for testing purposes; backlog at which completions start waiting for the completer to catch up
	batchReadyChan       chan struct{}
	completionMaxSize    int  // configurable for testing purposes; max jobs to complete in single database operation
	disableSleep         bool // disable sleep in testing
	drainTimeout         time.Duration
	maxBacklog           int // configurable for testing purposes; emergency backlog threshold where a warning is logged
	exec                 riverdriver.Executor
	pilot                riverpilot.Pilot
	schema               string
	setStateParams       map[int64]batchCompleterSetState
	setStateParamsMu     sync.RWMutex
	subscribeCh          SubscribeChan
	waitOnBacklogChan    chan struct{}
	waitOnBacklogWaiting bool

	stopErrMu sync.Mutex
	stopErr   error

	// acceptingResults is set to false when a shutdown drain gives up. Once
	// false, JobSetStateIfRunning returns the stop error instead of buffering
	// results that would never be persisted or blocking on backpressure after
	// the run goroutine has exited.
	acceptingResults atomic.Bool
}

// DrainTimeoutDefault is the default maximum amount of time the batch
// completer will spend persisting accepted job results and emitting their
// subscription events while shutting down. It's chosen to accommodate at least
// one full retry cycle (three attempts, ten second hot operation timeout
// each) plus a quick final attempt.
const DrainTimeoutDefault = 1 * time.Minute

// drainIdleWait is how long the shutdown drain waits for a batch-ready signal
// before rechecking the backlog. In a normal client shutdown producers have
// already enqueued every result before the completer starts draining, so this
// only matters for standalone use where results may arrive concurrently.
const drainIdleWait = 50 * time.Millisecond

func NewBatchCompleter(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan, drainTimeout time.Duration) *BatchCompleter {
	const (
		completionMaxSize    = 5_000
		backlogWaitThreshold = completionMaxSize * 2
		maxBacklog           = 20_000
	)

	completer := baseservice.Init(archetype, &BatchCompleter{
		backlogWaitThreshold: backlogWaitThreshold,
		batchReadyChan:       make(chan struct{}, 1),
		completionMaxSize:    completionMaxSize,
		drainTimeout:         drainTimeout,
		exec:                 exec,
		maxBacklog:           maxBacklog,
		pilot:                pilot,
		schema:               schema,
		setStateParams:       make(map[int64]batchCompleterSetState),
		subscribeCh:          subscribeCh,
	})
	// Accept results from construction; a drain that gives up flips this off
	// until the next Start.
	completer.acceptingResults.Store(true)

	return completer
}

func (c *BatchCompleter) ResetSubscribeChan(subscribeCh SubscribeChan) {
	c.subscribeCh = subscribeCh
}

// drainTimeoutEffective returns the bounded amount of time the completer may
// spend persisting accepted results during shutdown. A non-positive value
// falls back to the default.
func (c *BatchCompleter) drainTimeoutEffective() time.Duration {
	if c.drainTimeout <= 0 {
		return DrainTimeoutDefault
	}
	return c.drainTimeout
}

// StopError returns the error that prevented accepted job results from being
// persisted during shutdown, if any. It's only meaningful after Stop has
// returned. A nil result means every accepted result was written and its
// subscription event was handed off.
func (c *BatchCompleter) StopError() error {
	c.stopErrMu.Lock()
	defer c.stopErrMu.Unlock()

	return c.stopErr
}

func (c *BatchCompleter) setAcceptingResults(accepting bool) {
	c.acceptingResults.Store(accepting)
}

func (c *BatchCompleter) setStopErr(err error) {
	c.stopErrMu.Lock()
	defer c.stopErrMu.Unlock()

	// Preserve the first failure, but allow resetting on restart.
	if err != nil && c.stopErr != nil {
		return
	}
	c.stopErr = err
}

func (c *BatchCompleter) Start(ctx context.Context) error {
	stopCtx, shouldStart, started, stopped := c.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if c.subscribeCh == nil {
		panic("subscribeCh must be non-nil")
	}

	// Clear any drain error from a previous run so it can't surface after a
	// successful restart.
	c.setStopErr(nil)
	c.setAcceptingResults(true)

	go func() {
		started()
		defer stopped() // this defer should come first so it's first out
		defer close(c.subscribeCh)

		c.Logger.DebugContext(ctx, c.Name+": Run loop started")
		defer c.Logger.DebugContext(ctx, c.Name+": Run loop stopped")

		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		backlogSize := func() int {
			c.setStateParamsMu.RLock()
			defer c.setStateParamsMu.RUnlock()
			return len(c.setStateParams)
		}

		for numTicks := 0; ; numTicks++ {
			// batchReady indicates a producer explicitly signaled that the
			// backlog reached the ready threshold, in which case a batch is
			// handled immediately regardless of the tick cadence.
			var batchReady bool

			select {
			case <-stopCtx.Done():
				// Producers have all stopped by the time the completer is
				// stopped, so every in-flight work unit's final result has
				// already been accepted. Drain them to the database within a
				// bounded deadline, emitting subscription events as each
				// batch is confirmed.
				c.drain(ctx)
				return

			case <-c.batchReadyChan:
				batchReady = true
			case <-ticker.C:
			}

			// The ticker fires quite often to make sure that given a huge glut
			// of jobs, we don't accidentally build up too much of a backlog by
			// waiting too long. However, don't start a complete operation until
			// we reach a minimum threshold unless we're on a tick that's a
			// multiple of 5. So, jobs will be completed every 250ms even if the
			// threshold hasn't been met. An explicit batch-ready signal always
			// qualifies.
			const batchCompleterStartThreshold = 100
			if !batchReady && backlogSize() < min(c.backlogWaitThresholdEffective(), batchCompleterStartThreshold) && numTicks != 0 && numTicks%5 != 0 {
				continue
			}

			for {
				if err := c.handleBatch(ctx); err != nil {
					c.Logger.ErrorContext(ctx, c.Name+": Error completing batch", "err", err)
				}

				// New jobs to complete may have come in while working the batch
				// above. If enough have to bring us above the minimum complete
				// threshold, loop again and do another batch. Otherwise, break
				// and listen for a new tick.
				if backlogSize() < batchCompleterStartThreshold {
					break
				}
			}
		}
	}()

	return nil
}

// ErrDrainDeadlineExceeded indicates that the batch completer's bounded
// shutdown drain ended before every accepted job result could be persisted.
// Jobs belonging to unpersisted results are left untouched in the database
// (typically still running), so they stay recoverable via job rescue rather
// than being reported as completed.
var ErrDrainDeadlineExceeded = errors.New("job completer shutdown drain deadline exceeded")

// drain persists every job result accepted before shutdown within the
// configured drain timeout. Confirmed batches are written to the database and
// their subscription events are emitted in order. A failed write is retried
// with backoff until every accepted result is confirmed, the drain deadline
// elapses, or a non-retryable error (like a closed pool) is encountered.
//
// On failure, unconfirmed results stay in the completer's backlog and their
// database rows are left untouched (usually in running state), so the job
// rescuer can recover them after restart. The failure is recorded and
// surfaced through StopError rather than being logged and forgotten.
func (c *BatchCompleter) drain(ctx context.Context) {
	drainTimeout := c.drainTimeoutEffective()

	// The run context is already stopping, so detach from its cancellation,
	// but enforce an explicit, controllable deadline for the drain.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancel()

	c.Logger.InfoContext(ctx, c.Name+": Draining accepted job completions", slog.Duration("drain_timeout", drainTimeout))

	var lastErr error
	for {
		err := c.handleBatch(drainCtx)
		if err != nil && isNonRetryableCompleterError(err) {
			// A non-retryable write failure (e.g. closed pool) drops the
			// in-flight items from the backlog, but their database rows were
			// never written, so they're still recoverable by rescue.
			c.abandonDrain(ctx, -1, fmt.Errorf("job completer gave up draining accepted job completions: %w", err))
			return
		} else if err != nil {
			lastErr = err
		}

		if drainCtx.Err() != nil {
			remaining := c.backlogSize()
			if lastErr != nil {
				c.abandonDrain(ctx, remaining, fmt.Errorf(
					"%w: %d accepted job result(s) still unpersisted after %s (last error: %w)",
					ErrDrainDeadlineExceeded, remaining, drainTimeout, lastErr,
				))
			} else {
				c.abandonDrain(ctx, remaining, fmt.Errorf(
					"%w: %d accepted job result(s) still unpersisted after %s",
					ErrDrainDeadlineExceeded, remaining, drainTimeout,
				))
			}
			return
		}

		if c.backlogSize() == 0 {
			c.Logger.InfoContext(ctx, c.Name+": Drained all accepted job completions")
			return
		}

		// Accepted results are normally already enqueued before drain begins
		// because the client stops producers first. This wait only matters
		// when the completer is stopped standalone while a result is arriving.
		select {
		case <-c.batchReadyChan:
		case <-time.After(drainIdleWait):
		case <-drainCtx.Done():
		}
	}
}

// abandonDrain records the drain failure, stops accepting new results, and
// unblocks any producer waiting on backpressure. The run goroutine exits right
// after, so a producer that was handed back its slot would otherwise wait
// forever for a drain that will never happen.
func (c *BatchCompleter) abandonDrain(ctx context.Context, remaining int, err error) {
	c.setStopErr(err)
	c.setAcceptingResults(false)

	c.setStateParamsMu.Lock()
	if c.waitOnBacklogWaiting {
		close(c.waitOnBacklogChan)
		c.waitOnBacklogWaiting = false
	}
	c.setStateParamsMu.Unlock()

	attrs := []any{slog.String("err", err.Error())}
	if remaining >= 0 {
		attrs = append(attrs, slog.Int("num_jobs_unpersisted", remaining))
	}
	c.Logger.ErrorContext(ctx, c.Name+": Shutdown drain incomplete; unpersisted jobs are left in running state and will be rescued after restart", attrs...)
}

func (c *BatchCompleter) backlogSize() int {
	c.setStateParamsMu.RLock()
	defer c.setStateParamsMu.RUnlock()

	return len(c.setStateParams)
}

func (c *BatchCompleter) handleBatch(ctx context.Context) error {
	var setStateBatch map[int64]batchCompleterSetState
	func() {
		c.setStateParamsMu.Lock()
		defer c.setStateParamsMu.Unlock()

		setStateBatch = c.setStateParams

		// Don't bother resetting the map if there's nothing to process,
		// allowing the completer to idle efficiently.
		if len(setStateBatch) > 0 {
			c.setStateParams = make(map[int64]batchCompleterSetState)
		} else {
			// Set nil to avoid a data race below in case the map is set as a
			// new job comes in.
			setStateBatch = nil
		}
	}()

	if len(setStateBatch) < 1 {
		return nil
	}

	// Order the accepted results so sub-batches are contiguous slices. This
	// lets a partially failed write requeue only its own unconfirmed items
	// instead of replaying items an earlier sub-batch already persisted.
	items := make([]batchCompleterSetState, 0, len(setStateBatch))
	for _, setState := range setStateBatch {
		items = append(items, setState)
	}
	params := c.mapBatch(items)

	c.Logger.DebugContext(ctx, c.Name+": Completing batch of job(s)", slog.Int("num_jobs", len(items)))

	completeTime := c.Time.Now()

	// Events confirmed by sub-batches that completed successfully. They're
	// aggregated and sent once so a partially failed write doesn't suppress
	// events for already-confirmed items.
	var confirmedEvents []CompleterJobUpdated

	// Write the batch as one or more sub-batches. A sub-batch is acknowledged
	// only after its write succeeds, so if one fails, it and every sub-batch
	// not yet attempted are requeued (or dropped), while earlier sub-batches
	// are neither rewritten nor requeued.
	for i := 0; i < len(items); i += c.completionMaxSize {
		endIndex := min(i+c.completionMaxSize, len(items)) // beginning of next sub-batch or end of slice

		// Fast path for the common case where the whole batch fits in one
		// write: reuse the already-allocated parameter slices.
		subBatch := params
		if i != 0 || endIndex != len(items) {
			subBatch = &riverdriver.JobSetStateIfRunningManyParams{
				ID:              params.ID[i:endIndex],
				Attempt:         params.Attempt[i:endIndex],
				ErrData:         params.ErrData[i:endIndex],
				FinalizedAt:     params.FinalizedAt[i:endIndex],
				MetadataDoMerge: params.MetadataDoMerge[i:endIndex],
				MetadataUpdates: params.MetadataUpdates[i:endIndex],
				ScheduledAt:     params.ScheduledAt[i:endIndex],
				Schema:          params.Schema,
				State:           params.State[i:endIndex],
			}
		}

		jobRows, err := c.completeSubBatch(ctx, subBatch)
		if err != nil {
			if !isNonRetryableCompleterError(err) {
				// Requeue the failed sub-batch and any sub-batches that were
				// never attempted: all unacknowledged items, nothing more.
				c.requeueBatch(ctx, itemsToBatchMap(items[i:]))
			}

			c.deliverEvents(ctx, confirmedEvents)
			c.releaseBacklogWaitIfReady(ctx)
			return err
		}

		confirmedEvents = append(confirmedEvents, c.jobUpdatesForRows(jobRows, setStateBatch, completeTime)...)
	}

	if !c.deliverEvents(ctx, confirmedEvents) {
		// Every item was written, but the drain deadline expired while
		// delivering its events. Report the deadline rather than succeeding.
		c.releaseBacklogWaitIfReady(ctx)
		return ctx.Err()
	}

	c.releaseBacklogWaitIfReady(ctx)

	return nil
}

// deliverEvents sends subscription events to the downstream subscription
// manager. During a bounded drain the send respects ctx, so a wedged consumer
// can't extend shutdown indefinitely; in the uncanceled run-loop context it
// behaves as an unconditional send. Returns false if ctx ended before the
// events were handed off.
func (c *BatchCompleter) deliverEvents(ctx context.Context, events []CompleterJobUpdated) bool {
	if len(events) == 0 {
		return true
	}

	select {
	case c.subscribeCh <- events:
		return true
	case <-ctx.Done():
		return false
	}
}

// completeSubBatch performs a single sub-batch write with retries. The passed
// context bounds both database attempts and backoff sleeps, which lets the
// shutdown drain enforce its deadline.
func (c *BatchCompleter) completeSubBatch(ctx context.Context, batchParams *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	start := time.Now()
	defer func() {
		c.Logger.DebugContext(ctx, c.Name+": Completed sub-batch of job(s)",
			slog.Duration("duration", time.Since(start)),
			slog.Int("num_jobs", len(batchParams.ID)),
		)
	}()

	return withRetries(ctx, ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
		rows, err := c.pilot.JobSetStateIfRunningMany(ctx, c.exec, batchParams)
		if err != nil {
			return nil, err
		}

		return rows, nil
	})
}

// jobUpdatesForRows builds subscription updates for rows returned by a
// confirmed sub-batch write.
func (c *BatchCompleter) jobUpdatesForRows(jobRows []*rivertype.JobRow, setStateBatch map[int64]batchCompleterSetState, completeTime time.Time) []CompleterJobUpdated {
	events := make([]CompleterJobUpdated, 0, len(jobRows))
	for _, jobRow := range jobRows {
		setState := setStateBatch[jobRow.ID]
		setState.Stats.CompleteDuration = completeTime.Sub(setState.StartTime)
		events = append(events, completerJobUpdatedFromStateAndReason(jobRow, setState.Stats, setState.Params.Reason))
	}
	return events
}

// mapBatch converts accepted results into the driver's many-update parameter
// shape. This could be written more simply using map helpers, but it's done
// this way to allocate as few new slices as necessary.
func (c *BatchCompleter) mapBatch(items []batchCompleterSetState) *riverdriver.JobSetStateIfRunningManyParams {
	params := &riverdriver.JobSetStateIfRunningManyParams{
		ID:              make([]int64, len(items)),
		Attempt:         make([]*int, len(items)),
		ErrData:         make([][]byte, len(items)),
		FinalizedAt:     make([]*time.Time, len(items)),
		MetadataDoMerge: make([]bool, len(items)),
		MetadataUpdates: make([][]byte, len(items)),
		ScheduledAt:     make([]*time.Time, len(items)),
		State:           make([]rivertype.JobState, len(items)),
	}
	for i, setState := range items {
		params.ID[i] = setState.Params.ID
		params.Attempt[i] = setState.Params.Attempt
		params.ErrData[i] = setState.Params.ErrData
		params.FinalizedAt[i] = setState.Params.FinalizedAt
		params.MetadataDoMerge[i] = setState.Params.MetadataDoMerge
		params.MetadataUpdates[i] = setState.Params.MetadataUpdates
		params.ScheduledAt[i] = setState.Params.ScheduledAt
		params.State[i] = setState.Params.State
	}
	params.Schema = c.schema
	return params
}

// itemsToBatchMap indexes a slice of accepted results by job ID so it can be
// requeued through requeueBatch.
func itemsToBatchMap(items []batchCompleterSetState) map[int64]batchCompleterSetState {
	batch := make(map[int64]batchCompleterSetState, len(items))
	for _, item := range items {
		batch[item.Params.ID] = item
	}
	return batch
}

func (c *BatchCompleter) releaseBacklogWaitIfReady(ctx context.Context) {
	c.setStateParamsMu.Lock()
	defer c.setStateParamsMu.Unlock()

	if c.waitOnBacklogWaiting && len(c.setStateParams) < c.backlogResumeThreshold() {
		c.Logger.DebugContext(ctx, c.Name+": Disabling waitOnBacklog; ready to complete more jobs")
		close(c.waitOnBacklogChan)
		c.waitOnBacklogWaiting = false
	}
}

func (c *BatchCompleter) requeueBatch(ctx context.Context, setStateBatch map[int64]batchCompleterSetState) {
	c.setStateParamsMu.Lock()
	for id, setState := range setStateBatch {
		if _, exists := c.setStateParams[id]; exists {
			continue
		}
		c.setStateParams[id] = setState
	}
	backlogSize := len(c.setStateParams)
	if c.waitOnBacklogWaiting && backlogSize < c.backlogResumeThreshold() {
		c.Logger.DebugContext(ctx, c.Name+": Disabling waitOnBacklog; ready to complete more jobs")
		close(c.waitOnBacklogChan)
		c.waitOnBacklogWaiting = false
	}
	c.setStateParamsMu.Unlock()

	if backlogSize >= c.batchReadyThreshold() {
		c.signalBatchReady()
	}

	c.Logger.DebugContext(ctx, c.Name+": Requeued failed batch of job(s)", "num_jobs", len(setStateBatch))
}

func (c *BatchCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	// Once the shutdown drain has given up, the run goroutine is gone. Don't
	// buffer a result that will never be persisted (or block on backpressure
	// forever); return the recorded failure to the caller. The job's database
	// row is left untouched and remains recoverable by the rescuer.
	if !c.acceptingResults.Load() {
		if stopErr := c.StopError(); stopErr != nil {
			return stopErr
		}
		return ErrDrainDeadlineExceeded
	}

	now := c.Time.Now()

	var backlogSize int
	for {
		// Keep the common enqueue path to one lock acquisition. If the
		// completer is behind, wait for the current backlog gate to open and
		// retry so the threshold is checked against fresh state.
		var waitChan <-chan struct{}
		backlogSize, waitChan = c.tryEnqueueSetState(ctx, now, stats, params)
		if waitChan != nil {
			// Wait unconditionally for backpressure to clear: during a
			// client shutdown the producers (and their executors) stop before
			// the completer drains, and the drain releases this gate. The
			// work context may be cancelled by then, but this final result
			// still has to be accepted rather than dropped.
			<-waitChan
			continue
		}
		break
	}

	if backlogSize >= c.batchReadyThreshold() {
		c.signalBatchReady()
	}

	return nil
}

func (c *BatchCompleter) tryEnqueueSetState(ctx context.Context, now time.Time, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) (int, <-chan struct{}) {
	c.setStateParamsMu.Lock()
	defer c.setStateParamsMu.Unlock()

	if c.waitOnBacklogWaiting {
		return 0, c.waitOnBacklogChan
	}

	var (
		backlogSize = len(c.setStateParams)
		waitAt      = c.backlogWaitThresholdEffective()
	)
	if backlogSize >= waitAt {
		c.initBacklogWaitLocked(ctx, backlogSize, waitAt)
	}

	statsSnapshot := *stats
	c.setStateParams[params.ID] = batchCompleterSetState{Params: params, StartTime: now, Stats: &statsSnapshot}

	return len(c.setStateParams), nil
}

// backlogResumeThreshold returns the low-water mark below which waiting
// completers are released. Keeping this below the wait threshold avoids rapidly
// cycling between waiting and not waiting when the completer is near capacity.
func (c *BatchCompleter) backlogResumeThreshold() int {
	return max(c.backlogWaitThresholdEffective()/2, 1)
}

// backlogWaitThresholdEffective returns the backlog size at which new
// completions should wait for the batch completer to catch up. It's capped at
// maxBacklog so tests and future configuration can't set a normal wait
// threshold beyond the emergency warning threshold.
func (c *BatchCompleter) backlogWaitThresholdEffective() int {
	if c.backlogWaitThreshold <= 0 {
		return c.maxBacklog
	}
	return min(c.backlogWaitThreshold, c.maxBacklog)
}

// batchReadyThreshold returns the backlog size at which the run loop should be
// nudged to process a batch immediately instead of waiting for its next ticker.
// It aims for a full database batch while still respecting low test thresholds.
func (c *BatchCompleter) batchReadyThreshold() int {
	return min(c.completionMaxSize, c.backlogWaitThresholdEffective())
}

func (c *BatchCompleter) signalBatchReady() {
	select {
	case c.batchReadyChan <- struct{}{}:
	default:
	}
}

// initBacklogWaitLocked starts a backlog wait gate and must be called with
// setStateParamsMu held.
func (c *BatchCompleter) initBacklogWaitLocked(ctx context.Context, backlogSize, waitAt int) chan struct{} {
	c.waitOnBacklogChan = make(chan struct{})
	c.waitOnBacklogWaiting = true
	if backlogSize >= c.maxBacklog {
		c.Logger.WarnContext(ctx, c.Name+": Hit maximum backlog; completions will wait until below threshold",
			"backlog_size", backlogSize,
			"backlog_wait_threshold", waitAt,
			"max_backlog", c.maxBacklog,
		)
	} else {
		c.Logger.DebugContext(ctx, c.Name+": Applying completion backlog pressure",
			"backlog_resume_threshold", c.backlogResumeThreshold(),
			"backlog_size", backlogSize,
			"backlog_wait_threshold", waitAt,
		)
	}
	return c.waitOnBacklogChan
}

func isNonRetryableCompleterError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, riverdriver.ErrClosedPool)
}

// As configured, total time asleep from initial attempt is ~7 seconds (1 + 2 +
// 4) (not including jitter). However, if each attempt times out, that's up to
// ~37 seconds (7 seconds + 3 * 10 seconds).
const numRetries = 3

// withRetries invokes retryFunc with retries and exponential backoff. opCtx
// bounds the actual database attempts (including the hot operation timeout),
// while logCtx is used for logging and backoff sleeps. Callers that want
// retries to survive service cancellation (but still respect a shutdown drain
// deadline) pass a detached-but-deadline-bounded opCtx and the original
// service context as logCtx.
func withRetries[T any](opCtx, logCtx context.Context, baseService *baseservice.BaseService, disableSleep bool, retryFunc func(ctx context.Context) (T, error)) (T, error) {
	var (
		defaultVal T
		lastErr    error
	)

	for attempt := 1; attempt <= numRetries; attempt++ {
		// I've found that we want at least ten seconds for a large batch,
		// although it usually doesn't need that long.
		retVal, err := timeoututil.WithTimeoutV(opCtx, rivercommon.HotOperationTimeout, baseService.Name+".withRetries", retryFunc)
		if err != nil {
			// A cancelled context or a closed pool will never succeed.
			if isNonRetryableCompleterError(err) {
				return defaultVal, err
			}

			lastErr = err
			sleepDuration := serviceutil.ExponentialBackoff(attempt, serviceutil.MaxAttemptsBeforeResetDefault)
			baseService.Logger.ErrorContext(logCtx, baseService.Name+": Completer error (will retry after sleep)",
				slog.Int("attempt", attempt),
				slog.String("err", err.Error()),
				slog.String("sleep_duration", sleepDuration.String()),
				slog.String("timeout", rivercommon.HotOperationTimeout.String()),
			)
			if !disableSleep {
				serviceutil.CancellableSleep(logCtx, sleepDuration)
			}
			continue
		}

		return retVal, nil
	}

	baseService.Logger.ErrorContext(logCtx, baseService.Name+": Too many errors; giving up")

	return defaultVal, lastErr
}
