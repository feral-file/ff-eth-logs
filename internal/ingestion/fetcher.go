// Package ingestion keeps the warehouse current: it follows the chain head
// over a newHeads subscription and, for every block that has reached the
// confirmation depth, fetches the warehouse's event set and writes it in one
// transaction with the block's metadata and the cursor.
//
// The head-tracking, confirmation-lag, reorg accounting, catch-up batching,
// dense-block fallback and error classification are ported from
// ff-indexer-v2 (internal/providers/ethereum/subscriber.go and client.go at
// 58d4b04) so both services read the chain identically. Only the sink differs:
// the indexer enqueues parsed events, the warehouse stores raw logs.
package ingestion

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	"github.com/feral-file/ff-eth-logs/internal/chain"
	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/logger"
)

// Fetcher pulls the ingestion topic set for a block range, serving a block
// that overflows the provider's eth_getLogs result cap from its receipts.
type Fetcher struct {
	client    chain.EthClient
	paginator *chain.Paginator
}

// NewFetcher creates a Fetcher over client with the given span cap (0 = unknown).
func NewFetcher(client chain.EthClient, clock chain.Clock, spanCap uint64) *Fetcher {
	return &Fetcher{client: client, paginator: chain.NewPaginator(client, clock, spanCap)}
}

// FetchIngestionLogs returns every log with a warehouse topic0 in [fromBlock,
// toBlock], sorted by (block, log index). The filter is topic0-only and
// address-unrestricted — exactly the indexer's ingestion filter — so callers
// must still apply eventset.Keep before storing.
func (f *Fetcher) FetchIngestionLogs(ctx context.Context, fromBlock, toBlock uint64) ([]types.Log, error) {
	logs, err := f.paginator.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Topics:    [][]common.Hash{eventset.Topics()},
	})
	var overflow *chain.SingleBlockOverflowError
	if errors.As(err, &overflow) {
		logs, err = f.fetchAroundDenseBlock(ctx, fromBlock, toBlock, overflow.Block)
	}
	if err != nil {
		return nil, err
	}
	// Providers return logs in chain order per call and the paginator walks
	// windows ascending, so this is normally a no-op; it makes the ordering
	// contract explicit instead of provider-dependent.
	sort.SliceStable(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		return logs[i].Index < logs[j].Index
	})
	return logs, nil
}

// fetchAroundDenseBlock serves [fromBlock, toBlock] when block `dense` alone
// has more matching logs than the provider's result cap (the unrestricted
// filter includes the ERC-20 Transfer signature, so an airdrop block can reach
// it; the densest NFT-only block seen in the backfill had 20,803 logs). The
// blocks on either side go through the normal paginated path (recursively, in
// case another dense block sits there); the dense block itself is read from
// its receipts, which have no cap.
func (f *Fetcher) fetchAroundDenseBlock(ctx context.Context, fromBlock, toBlock, dense uint64) ([]types.Log, error) {
	var logs []types.Log
	if dense > fromBlock {
		left, err := f.FetchIngestionLogs(ctx, fromBlock, dense-1)
		if err != nil {
			return nil, err
		}
		logs = append(logs, left...)
	}
	mid, err := f.denseBlockLogs(ctx, dense)
	if err != nil {
		return nil, err
	}
	logs = append(logs, mid...)
	if dense < toBlock {
		right, err := f.FetchIngestionLogs(ctx, dense+1, toBlock)
		if err != nil {
			return nil, err
		}
		logs = append(logs, right...)
	}
	return logs, nil
}

// denseBlockLogs applies the ingestion topic filter to a block's receipts —
// the same selection eth_getLogs would have made, without the result cap.
func (f *Fetcher) denseBlockLogs(ctx context.Context, block uint64) ([]types.Log, error) {
	receipts, err := f.client.BlockReceipts(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("block receipts for dense block %d: %w", block, err)
	}
	wanted := map[common.Hash]struct{}{}
	for _, topic := range eventset.Topics() {
		wanted[topic] = struct{}{}
	}
	var logs []types.Log
	total := 0
	for _, receipt := range receipts {
		for _, vLog := range receipt.Logs {
			total++
			if len(vLog.Topics) == 0 {
				continue
			}
			if _, ok := wanted[vLog.Topics[0]]; ok {
				logs = append(logs, *vLog)
			}
		}
	}
	logger.InfoCtx(ctx, "Dense block served from receipts (eth_getLogs result cap)",
		zap.Uint64("block", block), zap.Int("receiptLogs", total), zap.Int("matched", len(logs)))
	return logs, nil
}
