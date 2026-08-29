// Package chain wraps the Ethereum JSON-RPC client the tail ingestion uses.
//
// It is a port of ff-indexer-v2/internal/adapter/ethclient.go reduced to the
// four calls ingestion needs (newHeads subscription, eth_getLogs,
// eth_getBlockByNumber, eth_getBlockReceipts) plus eth_blockNumber. The retry
// policy and the retryable-error classification are copied unchanged so both
// services react identically to the same provider behavior.
package chain

import (
	"context"
	"errors"
	"fmt"
	"net"
	neturl "net/url"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/zap"

	"github.com/feral-file/ff-eth-logs/internal/logger"
)

// EthClient is the RPC surface tail ingestion depends on, kept minimal so it
// can be mocked in the subscriber tests.
//
//go:generate go run go.uber.org/mock/mockgen -source=client.go -destination=../mocks/chain.go -package=mocks -mock_names=EthClient=MockEthClient,Clock=MockClock
type EthClient interface {
	// SubscribeNewHead subscribes to eth_subscribe("newHeads"), delivering
	// the node-reported hash — see BlockHead for why not types.Header.
	SubscribeNewHead(ctx context.Context, ch chan<- *BlockHead) (ethereum.Subscription, error)
	// FilterLogs is eth_getLogs.
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
	// HeadByNumber returns the canonical head at a height with the
	// node-reported hash (eth_getBlockByNumber without transactions).
	HeadByNumber(ctx context.Context, number uint64) (*BlockHead, error)
	// BlockReceipts is eth_getBlockReceipts — the complete log source for a
	// block whose matching logs exceed the provider's eth_getLogs result cap.
	BlockReceipts(ctx context.Context, number uint64) ([]*types.Receipt, error)
	// BlockNumber is eth_blockNumber.
	BlockNumber(ctx context.Context) (uint64, error)
	// ChainID is eth_chainId.
	ChainID(ctx context.Context) (uint64, error)
	// Close closes the connection.
	Close()
}

// Clock abstracts time.Sleep so pagination tests do not wait for real
// halving back-offs.
type Clock interface {
	Sleep(d time.Duration)
}

// RealClock is the time package.
type RealClock struct{}

// Sleep sleeps for d.
func (RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// BlockHead is the subset of a newHeads notification ingestion needs.
//
// Reason: ethclient.SubscribeNewHead decodes notifications into types.Header,
// which drops the node-reported "hash". Recomputing it with Header.Hash() does
// not reproduce it for current mainnet headers — the decoded struct lacks
// fields newer than this go-ethereum version's RLP encoding (verified live in
// ff-indexer-v2: every local hash differed from eth_getBlockByNumber's while
// each head's parentHash matched the node hash of its predecessor).
// Parent-hash continuity therefore has to be checked against the wire hash,
// so ingestion subscribes through the raw RPC client into this type. The
// timestamp rides along because the warehouse stores it per block and the
// notification already carries it, saving one eth_getBlockByNumber per block
// in steady state.
type BlockHead struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
	Timestamp  hexutil.Uint64 `json:"timestamp"`
}

// Dial connects to a WebSocket (or HTTP) endpoint and wraps it with retries.
func Dial(ctx context.Context, rawurl string) (EthClient, error) {
	client, err := ethclient.DialContext(ctx, rawurl)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", endpointForLogs(rawurl), RedactURLs(err))
	}
	return NewRealEthClient(client, rawurl), nil
}

// RealEthClient wraps ethclient.Client with retry logic.
type RealEthClient struct {
	client *ethclient.Client
	url    string
}

// NewRealEthClient creates a RealEthClient. The stored URL is reduced to
// scheme and host: it exists only for log context, and provider URLs carry
// the API key in the path (Infura, Chainstack), which must never reach logs.
func NewRealEthClient(client *ethclient.Client, url string) *RealEthClient {
	return &RealEthClient{client: client, url: endpointForLogs(url)}
}

// endpointForLogs strips everything but scheme and host from an RPC URL.
func endpointForLogs(rawurl string) string {
	parsed, err := neturl.Parse(rawurl)
	if err != nil || parsed.Host == "" {
		return "<redacted>"
	}
	return parsed.Scheme + "://" + parsed.Host
}

// urlPattern matches ws/wss/http/https URLs inside free text, so a provider
// URL embedded in a transport error can be reduced to scheme and host.
var urlPattern = regexp.MustCompile(`(?i)\b(wss?|https?)://[^\s"'<>]+`)

// RedactURLs replaces every URL in err's message with its scheme and host.
//
// Reason: provider URLs carry the API key in the path (Infura, Chainstack),
// and transport errors from net/http and the websocket dialer quote the full
// URL. Those errors are logged (zap.Error), returned up to the fatal exit
// log, and forwarded to Sentry, so every error that can carry the endpoint
// is passed through here before it leaves this package. The wrapped error
// keeps its chain (errors.Is/As still work through redactedError).
func RedactURLs(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	clean := urlPattern.ReplaceAllStringFunc(msg, endpointForLogs)
	if clean == msg {
		return err
	}
	return &redactedError{msg: clean, cause: err}
}

// redactedError carries a sanitized message while preserving the cause for
// errors.Is / errors.As (the retry classifier looks through it).
type redactedError struct {
	msg   string
	cause error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.cause }

// retryableMessages are matched against the LOWERCASED error text, so every
// entry must be lowercase: as uppercase literals they could never match, which
// once silently made "unexpected EOF" a permanent error in the indexer.
var retryableMessages = []string{
	"connection refused", "connection reset", "connection timed out",
	"query cancelled", //nolint:misspell
	"i/o timeout", "broken pipe", "rate limit", "too many requests",
	"service unavailable", "bad gateway", "gateway timeout", "temporarily unavailable",
	"econnreset", "etimedout", "eof",
	// Provider-side 500s on a well-formed query (Infura returns a bare
	// "Internal error"). Retrying is bounded by the 5-minute budget.
	"internal error",
	"502", "503", "504",
}

// isRetryableEthError decides whether an RPC failure is worth retrying.
// Unknown errors are permanent by default to avoid infinite retries.
func isRetryableEthError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var urlErr *neturl.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && isTransientSyscall(opErr.Err) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.Temporary() || dnsErr.Timeout()
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range retryableMessages {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func isTransientSyscall(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH)
}

// executeWithRetry runs operation under exponential backoff (5 s initial,
// 30 s max, 5 min total, ×2, 50 % jitter) — the indexer's policy verbatim.
func (c *RealEthClient) executeWithRetry(ctx context.Context, operation func() error, operationName string) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 5 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 5 * time.Minute
	b.Multiplier = 2.0
	b.RandomizationFactor = 0.5

	retryOperation := func() error {
		err := RedactURLs(operation())
		if err == nil {
			return nil
		}
		if isRetryableEthError(err) {
			logger.WarnCtx(ctx, "retryable ethereum error encountered",
				zap.Error(err), zap.String("operation", operationName), zap.String("url", c.url))
			return fmt.Errorf("retryable error: %w", err)
		}
		logger.DebugCtx(ctx, "permanent ethereum error encountered",
			zap.Error(err), zap.String("operation", operationName), zap.String("url", c.url))
		return backoff.Permanent(fmt.Errorf("permanent error: %w", err))
	}
	if err := backoff.Retry(retryOperation, backoff.WithContext(b, ctx)); err != nil {
		return fmt.Errorf("ethereum operation %s failed after retries: %w", operationName, err)
	}
	return nil
}

// SubscribeNewHead subscribes to new block heads with retry logic. Only the
// subscribe call is retried; a subscription that later fails surfaces through
// its Err channel and is the caller's to re-establish.
func (c *RealEthClient) SubscribeNewHead(ctx context.Context, ch chan<- *BlockHead) (ethereum.Subscription, error) {
	var sub ethereum.Subscription
	err := c.executeWithRetry(ctx, func() error {
		var err error
		sub, err = c.client.Client().EthSubscribe(ctx, ch, "newHeads")
		return err
	}, "SubscribeNewHead")
	return sub, err
}

// FilterLogs is eth_getLogs with retry logic.
func (c *RealEthClient) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	var logs []types.Log
	err := c.executeWithRetry(ctx, func() error {
		var err error
		logs, err = c.client.FilterLogs(ctx, query)
		return err
	}, "FilterLogs")
	return logs, err
}

// HeadByNumber fetches the canonical head at a height. A nil result (the
// node does not have the block) is permanent: ethereum.NotFound.
func (c *RealEthClient) HeadByNumber(ctx context.Context, number uint64) (*BlockHead, error) {
	var head *BlockHead
	err := c.executeWithRetry(ctx, func() error {
		var result *BlockHead
		if err := c.client.Client().CallContext(ctx, &result, "eth_getBlockByNumber", hexutil.EncodeUint64(number), false); err != nil {
			return err
		}
		if result == nil {
			return backoff.Permanent(fmt.Errorf("block %d: %w", number, ethereum.NotFound))
		}
		head = result
		return nil
	}, "HeadByNumber")
	return head, err
}

// BlockReceipts is eth_getBlockReceipts with retry logic.
func (c *RealEthClient) BlockReceipts(ctx context.Context, number uint64) ([]*types.Receipt, error) {
	var receipts []*types.Receipt
	err := c.executeWithRetry(ctx, func() error {
		var err error
		receipts, err = c.client.BlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number))) //nolint:gosec // block numbers fit int64
		return err
	}, "BlockReceipts")
	return receipts, err
}

// BlockNumber is eth_blockNumber with retry logic.
func (c *RealEthClient) BlockNumber(ctx context.Context) (uint64, error) {
	var n uint64
	err := c.executeWithRetry(ctx, func() error {
		var err error
		n, err = c.client.BlockNumber(ctx)
		return err
	}, "BlockNumber")
	return n, err
}

// ChainID is eth_chainId with retry logic.
func (c *RealEthClient) ChainID(ctx context.Context) (uint64, error) {
	var id uint64
	err := c.executeWithRetry(ctx, func() error {
		n, err := c.client.ChainID(ctx)
		if err != nil {
			return err
		}
		if !n.IsUint64() {
			return backoff.Permanent(fmt.Errorf("chain id %s does not fit uint64", n))
		}
		id = n.Uint64()
		return nil
	}, "ChainID")
	return id, err
}

// Close closes the underlying connection.
func (c *RealEthClient) Close() { c.client.Close() }
