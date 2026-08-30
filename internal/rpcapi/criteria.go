// Package rpcapi serves the warehouse over JSON-RPC with go-ethereum's own
// rpc server, so request framing, batching, parameter decoding and error
// codes are geth's rather than an imitation. The `eth` namespace exposes
// eth_getLogs, eth_blockNumber and eth_chainId; every other method gets
// geth's standard -32601 "the method X does not exist/is not available".
package rpcapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

// Limits copied from go-ethereum eth/filters/api.go.
const (
	maxTopics    = 4
	maxSubTopics = 1000
)

// Error strings copied verbatim from go-ethereum eth/filters/api.go so a
// client sees the same message it would from a node.
var (
	errInvalidTopic           = errors.New("invalid topic(s)")
	errInvalidBlockRange      = errors.New("invalid block range params")
	errUnknownBlock           = errors.New("unknown block")
	errPendingLogsUnsupported = errors.New("pending logs are not supported")
	errExceedMaxTopics        = errors.New("exceed max topics")
)

// FilterCriteria is the eth_getLogs parameter object. It is a copy of
// go-ethereum's filters.FilterCriteria with its UnmarshalJSON, kept local
// because importing eth/filters drags the whole node into the binary.
type FilterCriteria struct {
	BlockHash *common.Hash
	FromBlock *big.Int
	ToBlock   *big.Int
	Addresses []common.Address
	Topics    [][]common.Hash
}

// UnmarshalJSON decodes the filter exactly as geth does, including the
// error messages for every malformed shape.
func (args *FilterCriteria) UnmarshalJSON(data []byte) error {
	type input struct {
		BlockHash *common.Hash     `json:"blockHash"`
		FromBlock *rpc.BlockNumber `json:"fromBlock"`
		ToBlock   *rpc.BlockNumber `json:"toBlock"`
		Addresses interface{}      `json:"address"`
		Topics    []interface{}    `json:"topics"`
	}
	var raw input
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.BlockHash != nil {
		if raw.FromBlock != nil || raw.ToBlock != nil {
			return errors.New("cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other")
		}
		args.BlockHash = raw.BlockHash
	} else {
		if raw.FromBlock != nil {
			args.FromBlock = big.NewInt(raw.FromBlock.Int64())
		}
		if raw.ToBlock != nil {
			args.ToBlock = big.NewInt(raw.ToBlock.Int64())
		}
	}
	addrs, err := decodeAddresses(raw.Addresses)
	if err != nil {
		return err
	}
	args.Addresses = addrs
	if len(raw.Topics) > maxTopics {
		return errExceedMaxTopics
	}
	if len(raw.Topics) > 0 {
		args.Topics = make([][]common.Hash, len(raw.Topics))
		for i, t := range raw.Topics {
			sub, err := decodeTopicPosition(t)
			if err != nil {
				return err
			}
			args.Topics[i] = sub
		}
	}
	return nil
}

// decodeAddresses accepts a single address string or an array of them.
func decodeAddresses(raw interface{}) ([]common.Address, error) {
	out := []common.Address{}
	switch rawAddr := raw.(type) {
	case nil:
	case []interface{}:
		for i, addr := range rawAddr {
			strAddr, ok := addr.(string)
			if !ok {
				return nil, fmt.Errorf("non-string address at index %d", i)
			}
			a, err := decodeAddress(strAddr)
			if err != nil {
				return nil, fmt.Errorf("invalid address at index %d: %w", i, err)
			}
			out = append(out, a)
		}
	case string:
		a, err := decodeAddress(rawAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid address: %w", err)
		}
		out = append(out, a)
	default:
		return nil, errors.New("invalid addresses in query")
	}
	return out, nil
}

// decodeTopicPosition accepts null (wildcard), a string, or an array of
// strings/nulls (a null component makes the whole position a wildcard).
func decodeTopicPosition(t interface{}) ([]common.Hash, error) {
	switch topic := t.(type) {
	case nil:
		return nil, nil
	case string:
		top, err := decodeTopic(topic)
		if err != nil {
			return nil, err
		}
		return []common.Hash{top}, nil
	case []interface{}:
		if len(topic) > maxSubTopics {
			return nil, errExceedMaxTopics
		}
		var out []common.Hash
		for _, rawTopic := range topic {
			if rawTopic == nil {
				return nil, nil
			}
			s, ok := rawTopic.(string)
			if !ok {
				return nil, errInvalidTopic
			}
			parsed, err := decodeTopic(s)
			if err != nil {
				return nil, err
			}
			out = append(out, parsed)
		}
		return out, nil
	default:
		return nil, errInvalidTopic
	}
}

func decodeAddress(s string) (common.Address, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != common.AddressLength {
		err = fmt.Errorf("hex has invalid length %d after decoding; expected %d for address", len(b), common.AddressLength)
	}
	return common.BytesToAddress(b), err
}

func decodeTopic(s string) (common.Hash, error) {
	b, err := hexutil.Decode(s)
	if err == nil && len(b) != common.HashLength {
		err = fmt.Errorf("hex has invalid length %d after decoding; expected %d for topic", len(b), common.HashLength)
	}
	return common.BytesToHash(b), err
}
