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

// Warehouse is what the API reads. Every request runs inside one Read so
// the coverage check, the blockHash lookup and the log selection see the
// same snapshot — a rewind committing mid-request cannot turn an authorized
// range into a silently partial answer (see logstore.View).
type Warehouse interface {
	Read(ctx context.Context, fn func(logstore.View) error) error
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

// ScopeError is returned for a request the warehouse cannot answer from its
// stored set: a block outside the covered interval, a signature outside the
// event set, a filter without a topic0, or a CryptoPunks signature not
// pinned to the CryptoPunks contract. Within scope the answer is the vendor's
// minus the shapes the warehouse never stores (eventset.OmittedShapes), a
// fixed delta documented in docs/api_design.md. It carries JSON-RPC code -32000 (geth's default for
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
	cov, err := a.coverage(ctx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(cov.Head), nil
}

// bounded applies rpc.query_timeout to a warehouse read. Every read goes
// through it — eth_getLogs, eth_blockNumber and /health alike — so a live
// but unresponsive database connection cannot hold a request (or the
// routing client's head lookup, or the health check) past the timeout.
// config.Validate rejects a non-positive timeout; the fallback here only
// covers a Config built without validation (tests).
func (a *API) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	if a.cfg.QueryTimeout > 0 {
		return context.WithTimeout(ctx, a.cfg.QueryTimeout)
	}
	return context.WithTimeout(ctx, DefaultQueryTimeout)
}

// DefaultQueryTimeout bounds warehouse reads when the API is constructed
// without a configured timeout.
const DefaultQueryTimeout = 60 * time.Second

// coverage reads the covered interval, refusing an empty warehouse.
func (a *API) coverage(ctx context.Context) (logstore.Coverage, error) {
	ctx, cancel := a.bounded(ctx)
	defer cancel()
	var cov logstore.Coverage
	err := a.store.Read(ctx, func(v logstore.View) error {
		c, ok, err := v.Coverage(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return &ScopeError{Reason: "warehouse is empty"}
		}
		cov = c
		return nil
	})
	return cov, scopeIfMaintenance(err)
}

// scopeIfMaintenance turns the store's maintenance refusal into the scope
// error a routing client falls through on.
func scopeIfMaintenance(err error) error {
	if errors.Is(err, logstore.ErrMaintenance) {
		return &ScopeError{Reason: "warehouse is under maintenance (a backfill is reloading it); ask a node"}
	}
	return err
}

// GetLogs implements eth_getLogs with go-ethereum's semantics on the stored
// set, refusing (rather than partially answering) anything outside it.
func (a *API) GetLogs(ctx context.Context, crit FilterCriteria) ([]*types.Log, error) {
	if len(crit.Topics) > maxTopics {
		return nil, errExceedMaxTopics
	}
	if err := checkTopicScope(crit.Topics, crit.Addresses); err != nil {
		return nil, err
	}
	ctx, cancel := a.bounded(ctx)
	defer cancel()
	out := []*types.Log{}
	err := a.store.Read(ctx, func(v logstore.View) error {
		cov, ok, err := v.Coverage(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return &ScopeError{Reason: "warehouse is empty"}
		}
		q, empty, err := resolveRange(ctx, v, crit, cov)
		if err != nil || empty {
			return err
		}
		logs, err := v.FilterLogs(ctx, q, a.cfg.MaxResults)
		if errors.Is(err, logstore.ErrTooManyResults) {
			return fmt.Errorf("query returned more than %d results", a.cfg.MaxResults)
		}
		if err != nil {
			return err
		}
		out = make([]*types.Log, len(logs))
		for i := range logs {
			out[i] = &logs[i]
		}
		return nil
	})
	if err != nil {
		return nil, scopeIfMaintenance(err)
	}
	return out, nil
}

// checkTopicScope enforces the rules the warehouse adds: the filter must pin
// topic0 to warehouse signatures (otherwise the answer would silently omit
// every other event on the chain), and a CryptoPunks signature must be
// pinned to the CryptoPunks contract, because the same signatures from any
// other address are not stored (eventset.Keep). Topic position counts are
// not constrained: the vendor ignores trailing wildcards (see logstore.Query),
// so no position rule could change what a node returns.
func checkTopicScope(topics [][]common.Hash, addresses []common.Address) error {
	if len(topics) == 0 || len(topics[0]) == 0 {
		return &ScopeError{Reason: "filter must name at least one warehouse event signature in topics[0]"}
	}
	punks := false
	for _, sig := range topics[0] {
		if !eventset.IsWarehouseSignature(sig) {
			return &ScopeError{Reason: fmt.Sprintf("topic0 %s is not a warehouse event signature", sig.Hex())}
		}
		punks = punks || eventset.IsCryptoPunksSignature(sig)
	}
	if punks && !onlyCryptoPunks(addresses) {
		return &ScopeError{Reason: fmt.Sprintf("CryptoPunks signatures are stored only for %s; restrict address to it", eventset.CryptoPunksAddress.Hex())}
	}
	return nil
}

// onlyCryptoPunks reports whether addresses is non-empty and every entry is
// the CryptoPunks contract.
func onlyCryptoPunks(addresses []common.Address) bool {
	if len(addresses) == 0 {
		return false
	}
	for _, a := range addresses {
		if a != eventset.CryptoPunksAddress {
			return false
		}
	}
	return true
}

// resolveRange turns the criteria into a concrete block range, following
// geth's GetLogs/Filter.Logs order of checks, then bounds it by the covered
// interval. empty=true reproduces geth returning [] for begin > end after
// resolution.
func resolveRange(ctx context.Context, v logstore.View, crit FilterCriteria, cov logstore.Coverage) (logstore.Query, bool, error) {
	q := logstore.Query{Addresses: crit.Addresses, Topics: crit.Topics}
	if crit.BlockHash != nil {
		block, ok, err := v.BlockByHash(ctx, *crit.BlockHash)
		if err != nil {
			return q, false, err
		}
		if !ok {
			return q, false, errUnknownBlock
		}
		// A stored row is not proof of coverage: an interrupted backfill leaves
		// historical rows without a cursor, and a later tail publishes only
		// its own interval. Serve a hash only inside the published interval.
		if block.Number < cov.Start || block.Number > cov.Head {
			return q, false, &ScopeError{Reason: fmt.Sprintf("block %d (%s) is outside the warehouse coverage %d-%d", block.Number, block.Hash.Hex(), cov.Start, cov.Head)}
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
	from, err := resolveSpecial(begin, cov.Head)
	if err != nil {
		return q, false, err
	}
	to, err := resolveSpecial(end, cov.Head)
	if err != nil {
		return q, false, err
	}
	if from > to {
		return q, true, nil // geth answers [] for an inverted range, before any other consideration
	}
	if from > cov.Head || to > cov.Head {
		return q, false, &ScopeError{Reason: fmt.Sprintf("blocks %d-%d extend above the warehouse head %d", from, to, cov.Head)}
	}
	if from < cov.Start {
		return q, false, &ScopeError{Reason: fmt.Sprintf("blocks %d-%d extend below the warehouse coverage start %d", from, to, cov.Start)}
	}
	q.FromBlock, q.ToBlock = from, to
	return q, false, nil
}

// resolveSpecial maps geth's block tags onto the warehouse: latest is the
// head; earliest is 0; safe and finalized are refused, because the head is
// only confirmation_blocks deep — shallower than a node's safe/finalized
// boundary — and answering them from it would attach a finality guarantee
// the data does not have (the caller falls back to a node).
func resolveSpecial(number int64, head uint64) (uint64, error) {
	switch number {
	case rpc.LatestBlockNumber.Int64():
		return head, nil
	case rpc.SafeBlockNumber.Int64(), rpc.FinalizedBlockNumber.Int64():
		return 0, &ScopeError{Reason: "safe and finalized block tags are not served (the warehouse head is only confirmation_blocks deep); ask a node"}
	case rpc.EarliestBlockNumber.Int64():
		return 0, nil
	default:
		if number < 0 {
			return 0, errors.New("negative block number")
		}
		return uint64(number), nil
	}
}
