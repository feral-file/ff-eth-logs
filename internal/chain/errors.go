package chain

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/rpc"
)

// containsAny reports whether the lowercased error text contains any needle.
// Provider messages are matched case-insensitively: vendors capitalize
// differently ("Block range limit exceeded" vs "block range is too wide") and
// a case slip must not turn a recoverable limit into a fatal walk.
func containsAny(err error, needles ...string) bool {
	msg := strings.ToLower(err.Error())
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// IsBlockRangeCapError reports whether a log query failed because the provider
// caps the queried block span rather than the result count. A span cap is a
// fixed provider property: once a span is rejected, every span that size or
// larger will be too, so pagination must not probe above an accepted span.
//
// Phrasings, each observed from a real provider (ff-indexer-v2 helpers/errors.go):
//   - Infura:     "range 9999999 exceeds limit of 10000"
//   - Chainstack: "Block range limit exceeded. See more details at
//     https://docs.chainstack.com/docs/limits#evm-range-limits" (-32602,
//     rejected above toBlock-fromBlock = 10100). Any -32602 whose message
//     mentions a range or limit is treated as a span cap as well.
//   - others:     "query exceeds max block range 100000",
//     "eth_getLogs is limited to 1024 block range", "block range is too wide".
func IsBlockRangeCapError(err error) bool {
	if err == nil {
		return false
	}
	if isInvalidParams(err) && containsAny(err, "range", "limit") {
		return true
	}
	return containsAny(err,
		"exceeds limit of",
		"block range limit",
		"exceeds max block range",
		"block range is too wide",
		"range too large",
		"range is too large",
	) || (containsAny(err, "limited to") && containsAny(err, "block range"))
}

// isInvalidParams reports whether err carries JSON-RPC code -32602.
func isInvalidParams(err error) bool {
	var rpcErr rpc.Error
	return errors.As(err, &rpcErr) && rpcErr.ErrorCode() == -32602
}

// IsTooManyResultsError reports whether a log query failed due to provider
// result or block-range limits. Both mean the same thing to callers: the
// queried window is too big and must be split, not treated as fatal.
//
// Result-cap phrasings observed: Infura "query returned more than 10000
// results", drpc "query returns too many logs, narrow your filter: 20000",
// "query exceeds max results 20000".
func IsTooManyResultsError(err error) bool {
	if err == nil {
		return false
	}
	return containsAny(err,
		"query returned more than",
		"too many results",
		"too many logs",
		"exceeds max results",
	) || IsBlockRangeCapError(err)
}
