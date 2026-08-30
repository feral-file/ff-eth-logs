package rpcapi

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	sigTransfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	addrPunks   = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
)

// TestFilterCriteriaUnmarshal pins the parse behavior and the exact error
// strings a client would get from go-ethereum eth/filters.
func TestFilterCriteriaUnmarshal(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantErr string
		check   func(t *testing.T, c FilterCriteria)
	}{
		{name: "range and topics", in: `{"fromBlock":"0x10","toBlock":"0x20","address":"` + addrPunks + `","topics":["` + sigTransfer + `",null,["` + sigTransfer + `","` + sigTransfer + `"]]}`,
			check: func(t *testing.T, c FilterCriteria) {
				assert.Equal(t, big.NewInt(16), c.FromBlock)
				assert.Equal(t, big.NewInt(32), c.ToBlock)
				assert.Equal(t, []common.Address{common.HexToAddress(addrPunks)}, c.Addresses)
				require.Len(t, c.Topics, 3)
				assert.Len(t, c.Topics[0], 1)
				assert.Nil(t, c.Topics[1])
				assert.Len(t, c.Topics[2], 2)
			}},
		{name: "tags", in: `{"fromBlock":"earliest","toBlock":"latest"}`, check: func(t *testing.T, c FilterCriteria) {
			assert.Equal(t, rpc.EarliestBlockNumber.Int64(), c.FromBlock.Int64())
			assert.Equal(t, rpc.LatestBlockNumber.Int64(), c.ToBlock.Int64())
		}},
		{name: "address array", in: `{"address":["` + addrPunks + `","` + addrPunks + `"]}`, check: func(t *testing.T, c FilterCriteria) {
			assert.Len(t, c.Addresses, 2)
			assert.Empty(t, c.Topics)
		}},
		{name: "null in sub array is wildcard", in: `{"topics":[["` + sigTransfer + `",null]]}`, check: func(t *testing.T, c FilterCriteria) {
			assert.Nil(t, c.Topics[0])
		}},
		{name: "blockHash", in: `{"blockHash":"0x` + repeat("ab", 32) + `"}`, check: func(t *testing.T, c FilterCriteria) {
			require.NotNil(t, c.BlockHash)
			assert.Nil(t, c.FromBlock)
		}},
		{name: "blockHash with range", in: `{"blockHash":"0x` + repeat("ab", 32) + `","fromBlock":"0x1"}`,
			wantErr: "cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other"},
		{name: "too many topics", in: `{"topics":[null,null,null,null,null]}`, wantErr: "exceed max topics"},
		{name: "non-string topic", in: `{"topics":[1]}`, wantErr: "invalid topic(s)"},
		{name: "non-string topic in array", in: `{"topics":[[1]]}`, wantErr: "invalid topic(s)"},
		{name: "short topic", in: `{"topics":["0x01"]}`, wantErr: "hex has invalid length 1 after decoding; expected 32 for topic"},
		{name: "short address", in: `{"address":"0x01"}`, wantErr: "invalid address: hex has invalid length 1 after decoding; expected 20 for address"},
		{name: "bad address in array", in: `{"address":["0x01"]}`, wantErr: "invalid address at index 0: hex has invalid length 1 after decoding; expected 20 for address"},
		{name: "non-string address in array", in: `{"address":[1]}`, wantErr: "non-string address at index 0"},
		// geth (eth/filters/api.go) rejects a null address element the same
		// way; only topics treat null as a wildcard.
		{name: "null address in array", in: `{"address":[null,"` + addrPunks + `"]}`, wantErr: "non-string address at index 0"},
		{name: "address object", in: `{"address":{}}`, wantErr: "invalid addresses in query"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c FilterCriteria
			err := json.Unmarshal([]byte(tc.in), &c)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			tc.check(t, c)
		})
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
