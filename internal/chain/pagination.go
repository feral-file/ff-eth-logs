package chain

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-eth-logs/internal/logger"
)

// SingleBlockOverflowError reports a block whose matching logs exceed the
// provider's per-query result cap: the walk halved down to one block and the
// provider still refused it. The fetcher serves such a block from receipts.
type SingleBlockOverflowError struct {
	Block uint64
	Err   error
}

func (e *SingleBlockOverflowError) Error() string {
	return fmt.Sprintf("too many results in single block %d: %v", e.Block, e.Err)
}

func (e *SingleBlockOverflowError) Unwrap() error { return e.Err }

const (
	// defaultStepSize is the indexer's step for a topic0-only query. Ingestion
	// ranges are ≤ 10 blocks, so the step only matters through SpanCap.
	defaultStepSize = uint64(1_000_000)

	// noDeadlineTimeout bounds a walk whose caller set no deadline. Each RPC
	// call is already bounded by its own retry budget (~5 min); this is a
	// backstop against a wedged walk, not a pace-setter.
	noDeadlineTimeout = 4 * time.Hour

	// paceLogEvery is the heartbeat cadence in successful windows.
	paceLogEvery = 250
)

// Paginator walks eth_getLogs over a block range with the indexer's adaptive
// step sizing: halve on too-many-results / range-cap rejections (sleeping 1 s
// between attempts), ramp back up ×2 after successes, and hoist a discovered
// span cap to the rest of the walk so it is never re-probed.
type Paginator struct {
	client EthClient
	clock  Clock
	// SpanCap is the provider's known eth_getLogs block-range cap as the
	// maximum accepted toBlock-fromBlock (Chainstack: 10100). 0 = unknown;
	// the walk then discovers it through rejections.
	SpanCap uint64
}

// NewPaginator creates a Paginator.
func NewPaginator(client EthClient, clock Clock, spanCap uint64) *Paginator {
	return &Paginator{client: client, clock: clock, SpanCap: spanCap}
}

type paceState struct {
	walkStart   time.Time
	target      uint64
	windowsDone int
}

func (p *paceState) windowDone(ctx context.Context, atBlock, stepSize uint64) {
	p.windowsDone++
	if p.windowsDone%paceLogEvery != 0 {
		return
	}
	logger.InfoCtx(ctx, "Log pagination walk progress",
		zap.Uint64("atBlock", atBlock), zap.Uint64("targetBlock", p.target),
		zap.Uint64("stepSize", stepSize), zap.Int("windowsDone", p.windowsDone),
		zap.Duration("elapsed", time.Since(p.walkStart)))
}

// FilterLogs fetches every log matching query in [query.FromBlock, query.ToBlock]
// (both required). On a context deadline it returns the partial logs with the
// context error, as the indexer does.
func (p *Paginator) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	timeoutCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, noDeadlineTimeout)
		defer cancel()
	}
	if query.FromBlock == nil || query.ToBlock == nil {
		return nil, fmt.Errorf("pagination requires explicit fromBlock and toBlock")
	}

	toBlock := query.ToBlock.Uint64()
	stepSize := p.initialStep()
	pace := paceState{walkStart: time.Now(), target: toBlock}
	var allLogs []types.Log
	for currentFrom := query.FromBlock.Uint64(); currentFrom <= toBlock; {
		if timeoutCtx.Err() != nil {
			logger.WarnCtx(ctx, "Context deadline exceeded during log pagination, returning partial logs",
				zap.Int("partialLogsCount", len(allLogs)), zap.Uint64("processedUpToBlock", currentFrom-1),
				zap.Uint64("targetToBlock", toBlock))
			return allLogs, timeoutCtx.Err()
		}
		// An outer window spans exactly stepSize blocks (from..from+stepSize-1),
		// matching the inner walk's window arithmetic.
		currentTo := min(currentFrom+stepSize-1, toBlock)
		window := query
		window.FromBlock = new(big.Int).SetUint64(currentFrom)
		window.ToBlock = new(big.Int).SetUint64(currentTo)

		logs, cappedStep, err := p.walkWindow(timeoutCtx, window, stepSize, &pace)
		if err != nil {
			if timeoutCtx.Err() != nil {
				return allLogs, timeoutCtx.Err()
			}
			return nil, fmt.Errorf("failed to get logs for range %d-%d: %w", currentFrom, currentTo, err)
		}
		// A provider's block-range cap holds for the whole walk, not just the
		// window that discovered it; hoist it so later windows skip the cascade.
		stepSize = min(stepSize, cappedStep)
		allLogs = append(allLogs, logs...)
		currentFrom = currentTo + 1
	}
	return allLogs, nil
}

// initialStep clamps the default step to the configured span cap: a step of N
// covers from..from+N-1, i.e. a span of N-1, so the largest accepted step is
// SpanCap+1.
func (p *Paginator) initialStep() uint64 {
	if p.SpanCap > 0 && defaultStepSize > p.SpanCap+1 {
		return p.SpanCap + 1
	}
	return defaultStepSize
}

// walkWindow fetches one outer window, halving the step on rejections. The
// second result is the step ceiling after the walk: the given stepSize unless
// the provider reported a block-range cap, in which case the discovered cap.
func (p *Paginator) walkWindow(ctx context.Context, query ethereum.FilterQuery, stepSize uint64, pace *paceState) ([]types.Log, uint64, error) {
	currentStepSize, maxStepSize := stepSize, stepSize
	toBlock := query.ToBlock.Uint64()
	var allLogs []types.Log
	for currentFrom := query.FromBlock.Uint64(); currentFrom <= toBlock; {
		if ctx.Err() != nil {
			return allLogs, maxStepSize, ctx.Err()
		}
		currentTo := min(currentFrom+currentStepSize-1, toBlock)
		attempt := query
		attempt.FromBlock = new(big.Int).SetUint64(currentFrom)
		attempt.ToBlock = new(big.Int).SetUint64(currentTo)

		logs, err := p.client.FilterLogs(ctx, attempt)
		if err == nil {
			allLogs = append(allLogs, logs...)
			currentFrom = currentTo + 1
			pace.windowDone(ctx, currentFrom, currentStepSize)
			// Ramp back up gradually instead of resetting: against a hard
			// span cap a full reset re-pays the whole halving cascade.
			currentStepSize = min(currentStepSize*2, maxStepSize)
			continue
		}
		if ctx.Err() != nil {
			return allLogs, maxStepSize, ctx.Err()
		}
		if !IsTooManyResultsError(err) {
			return nil, maxStepSize, err
		}
		if currentFrom == currentTo {
			return nil, maxStepSize, &SingleBlockOverflowError{Block: currentFrom, Err: err}
		}
		currentStepSize /= 2
		if currentStepSize == 0 {
			return nil, maxStepSize, fmt.Errorf("step size exhausted at block range [%d, %d]: %w", currentFrom, currentTo, err)
		}
		if IsBlockRangeCapError(err) {
			maxStepSize = currentStepSize
		}
		p.clock.Sleep(time.Second)
	}
	return allLogs, maxStepSize, nil
}
