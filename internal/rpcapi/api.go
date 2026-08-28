package rpcapi

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/feral-file/ff-eth-logs/internal/eventset"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

// Warehouse is what the API reads: the head and the two lookups.
type Warehouse interface {
	Head(ctx context.Context) (uint64, bool, error)
	FilterLogs(ctx context.Context, q logstore.Query, limit int) ([]types.Log, error)
	BlockByHash(ctx context.Context, hash common.Hash) (logstore.Block, bool, error)
}

// Config tunes the API.
type Config struct {
	// ChainID is what eth_chainId reports (1 for mainnet).
	ChainID uint64
	// MaxResults caps one eth_getLogs response. Exceeding it returns the
	// Infura-style "query returned more than N results" error so a client
	// with the indexer's pagination halves its window instead of failing.
	// 0 = unlimited (not recommended: a genesis-wide Transfer query is
	// 293 M rows).
	MaxResults int
	// QueryTimeout bounds one eth_getLogs database query.
	QueryTimeout time.Duration
}

// API is the `eth` namespace receiver registered with rpc.Server. Method
// names map to JSON-RPC methods by geth's convention: GetLogs → eth_getLogs.
type API struct {
	store Warehouse
	cfg   Config
}

// NewAPI creates the receiver.
func NewAPI(store Warehouse, cfg Config) *API { return &API{store: store, cfg: cfg} }

// ScopeError is returned for a request the warehouse cannot answer exactly:
// a block above the head, a signature outside the event set, or a filter
// without a topic0. It carries JSON-RPC code -32000 (geth's default for
// handler errors) and a message that deliberately avoids the words a
// range-cap classifier keys on ("range", "limit", "too many"), so a client
// treats it as out-of-scope rather than as a window to halve.
type ScopeError struct{ Reason string }

func (e *ScopeError) Error() string { return "out of warehouse scope: " + e.Reason }

// ErrorCode implements rpc.Error.
func (e *ScopeError) ErrorCode() int { return -32000 }

// ChainId implements eth_chainId.
func (a *API) ChainId() *hexutil.Big { //nolint:revive // geth method naming maps to eth_chainId
	return (*hexutil.Big)(new(big.Int).SetUint64(a.cfg.ChainID))
}

// BlockNumber implements eth_blockNumber: the warehouse head, i.e. the last
// block whose logs are fully stored — not the chain tip. A client that needs
// the tip must ask the chain; this is the split point for a routing client.
func (a *API) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	head, ok, err := a.store.Head(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, &ScopeError{Reason: "warehouse is empty"}
	}
	return hexutil.Uint64(head), nil
}

// GetLogs implements eth_getLogs with go-ethereum's semantics on the stored
// set, refusing (rather than partially answering) anything outside it.
func (a *API) GetLogs(ctx context.Context, crit FilterCriteria) ([]*types.Log, error) {
	if len(crit.Topics) > maxTopics {
		return nil, errExceedMaxTopics
	}
	if err := checkTopicScope(crit.Topics); err != nil {
		return nil, err
	}
	head, ok, err := a.store.Head(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &ScopeError{Reason: "warehouse is empty"}
	}
	q, empty, err := a.resolveRange(ctx, crit, head)
	if err != nil || empty {
		return []*types.Log{}, err
	}
	if a.cfg.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.cfg.QueryTimeout)
		defer cancel()
	}
	logs, err := a.store.FilterLogs(ctx, q, a.cfg.MaxResults)
	if errors.Is(err, logstore.ErrTooManyResults) {
		return nil, fmt.Errorf("query returned more than %d results", a.cfg.MaxResults)
	}
	if err != nil {
		return nil, err
	}
	out := make([]*types.Log, len(logs))
	for i := range logs {
		out[i] = &logs[i]
	}
	return out, nil
}

// checkTopicScope enforces the one rule the warehouse adds to geth's: the
// filter must pin topic0 to warehouse signatures. Without it the answer would
// silently omit every other event on the chain.
func checkTopicScope(topics [][]common.Hash) error {
	if len(topics) == 0 || len(topics[0]) == 0 {
		return &ScopeError{Reason: "filter must name at least one warehouse event signature in topics[0]"}
	}
	for _, sig := range topics[0] {
		if !eventset.IsWarehouseSignature(sig) {
			return &ScopeError{Reason: fmt.Sprintf("topic0 %s is not a warehouse event signature", sig.Hex())}
		}
	}
	return nil
}

// resolveRange turns the criteria into a concrete block range, following
// geth's GetLogs/Filter.Logs order of checks, then bounds it by the head.
// empty=true reproduces geth returning [] for begin > end after resolution.
func (a *API) resolveRange(ctx context.Context, crit FilterCriteria, head uint64) (logstore.Query, bool, error) {
	q := logstore.Query{Addresses: crit.Addresses, Topics: crit.Topics}
	if crit.BlockHash != nil {
		block, ok, err := a.store.BlockByHash(ctx, *crit.BlockHash)
		if err != nil {
			return q, false, err
		}
		if !ok {
			return q, false, errUnknownBlock
		}
		q.FromBlock, q.ToBlock = block.Number, block.Number
		return q, false, nil
	}
	begin, end := rpc.LatestBlockNumber.Int64(), rpc.LatestBlockNumber.Int64()
	if crit.FromBlock != nil {
		begin = crit.FromBlock.Int64()
	}
	if crit.ToBlock != nil {
		end = crit.ToBlock.Int64()
	}
	if begin > 0 && end > 0 && begin > end {
		return q, false, errInvalidBlockRange
	}
	if begin == rpc.PendingBlockNumber.Int64() || end == rpc.PendingBlockNumber.Int64() {
		return q, false, errPendingLogsUnsupported
	}
	from, err := resolveSpecial(begin, head)
	if err != nil {
		return q, false, err
	}
	to, err := resolveSpecial(end, head)
	if err != nil {
		return q, false, err
	}
	if from > head || to > head {
		return q, false, &ScopeError{Reason: fmt.Sprintf("blocks %d-%d extend above the warehouse head %d", from, to, head)}
	}
	q.FromBlock, q.ToBlock = from, to
	return q, from > to, nil
}

// resolveSpecial maps geth's block tags onto the warehouse: latest, safe and
// finalized all resolve to the head (every stored block is at least
// confirmation_blocks deep, which is shallower than "safe"/"finalized" on a
// node — documented in docs/api_design.md); earliest is 0.
func resolveSpecial(number int64, head uint64) (uint64, error) {
	switch number {
	case rpc.LatestBlockNumber.Int64(), rpc.SafeBlockNumber.Int64(), rpc.FinalizedBlockNumber.Int64():
		return head, nil
	case rpc.EarliestBlockNumber.Int64():
		return 0, nil
	default:
		if number < 0 {
			return 0, errors.New("negative block number")
		}
		return uint64(number), nil
	}
}
