package eventset

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSignatures pins the hex values the BigQuery extract used, so a change
// to a signature string here cannot silently diverge from the backfilled data.
func TestSignatures(t *testing.T) {
	want := map[string]common.Hash{
		"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": Transfer,
		"0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62": TransferSingle,
		"0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb": TransferBatch,
		"0xf8e1a15aba9398e019f0b49df1a4fde98ee17ae345cb5f6b5e2c27f5033e8ce7": MetadataUpdate,
		"0x6bd5c950a8d8df17f772f5af37cb3655737899cbf903264b9795592da439661c": BatchMetadataUpdate,
		"0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b": URI,
		"0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8": PunkTransfer,
		"0x8a0e37b73a0d9c82e205d4d1a3ff3d0b57ce5f4d7bccf6bac03336dc101cb7ba": PunkAssign,
		"0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3": PunkBought,
	}
	for hex, got := range want {
		assert.Equal(t, common.HexToHash(hex), got, hex)
	}
	require.Len(t, Topics(), 9)
	for _, topic := range Topics() {
		assert.True(t, IsWarehouseSignature(topic))
	}
	assert.False(t, IsWarehouseSignature(common.HexToHash("0x01")))
	assert.True(t, IsCryptoPunksSignature(PunkBought))
	assert.Contains(t, OmittedShapes(Transfer), "ERC-20")
	assert.Empty(t, OmittedShapes(PunkBought))
	assert.False(t, IsCryptoPunksSignature(Transfer))
}

func TestKeep(t *testing.T) {
	other := common.HexToAddress("0x1")
	h := common.HexToHash
	cases := []struct {
		name    string
		topics  []common.Hash
		address common.Address
		keep    bool
	}{
		{"erc721 transfer", []common.Hash{Transfer, h("0x1"), h("0x2"), h("0x3")}, other, true},
		{"erc20 transfer (3 topics)", []common.Hash{Transfer, h("0x1"), h("0x2")}, other, false},
		{"cryptokitties transfer (1 topic)", []common.Hash{Transfer}, other, false},
		{"transfer single", []common.Hash{TransferSingle, h("0x1"), h("0x2"), h("0x3")}, other, true},
		{"transfer single short", []common.Hash{TransferSingle, h("0x1")}, other, false},
		{"transfer batch", []common.Hash{TransferBatch, h("0x1"), h("0x2"), h("0x3")}, other, true},
		{"metadata update", []common.Hash{MetadataUpdate}, other, true},
		{"metadata update indexed", []common.Hash{MetadataUpdate, h("0x1")}, other, false},
		{"batch metadata update", []common.Hash{BatchMetadataUpdate}, other, true},
		{"uri", []common.Hash{URI, h("0x1")}, other, true},
		{"uri anonymous", []common.Hash{URI}, other, false},
		{"punk transfer from punks", []common.Hash{PunkTransfer, h("0x1"), h("0x2")}, CryptoPunksAddress, true},
		{"punk transfer elsewhere", []common.Hash{PunkTransfer, h("0x1"), h("0x2")}, other, false},
		{"punk assign", []common.Hash{PunkAssign, h("0x1")}, CryptoPunksAddress, true},
		{"punk bought", []common.Hash{PunkBought}, CryptoPunksAddress, true},
		{"unknown signature", []common.Hash{h("0xabc")}, other, false},
		{"anonymous", nil, other, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.keep, Keep(tc.topics, tc.address))
		})
	}
}
