// Package eventset defines the set of Ethereum logs the warehouse stores.
//
// The set is the intersection of two rules that both mirror ff-indexer-v2:
//
//   - topic0 is one of the signatures chain ingestion subscribes to (the six
//     standard ERC-721 / ERC-1155 / EIP-4906 events plus the three CryptoPunks
//     events from the contract registry), and
//   - the log has the *shape* the indexer's parsers accept. ERC-20 Transfer
//     shares the ERC-721 signature but carries three topics instead of four;
//     the indexer skips it at parse time (ff-indexer-v2 commit 60eba9b), so
//     the warehouse never stores it. Same for a one-topic MetadataUpdate or a
//     two-topic URI.
//
// The BigQuery backfill (docs/probe_2026-08.md) applied exactly these rules,
// so tail ingestion must too — otherwise the served set would depend on how
// a block reached the warehouse.
package eventset

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// Event signatures, keccak256 of the canonical ABI signature. Computed at
// init so a typo in a hex literal cannot silently drop an event family.
var (
	// Transfer is ERC-721 Transfer(address,address,uint256) — and, with three
	// topics, ERC-20 Transfer, which Keep rejects.
	Transfer = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	// TransferSingle is ERC-1155 TransferSingle(address,address,address,uint256,uint256).
	TransferSingle = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))
	// TransferBatch is ERC-1155 TransferBatch(address,address,address,uint256[],uint256[]).
	TransferBatch = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))
	// MetadataUpdate is EIP-4906 MetadataUpdate(uint256).
	MetadataUpdate = crypto.Keccak256Hash([]byte("MetadataUpdate(uint256)"))
	// BatchMetadataUpdate is EIP-4906 BatchMetadataUpdate(uint256,uint256).
	BatchMetadataUpdate = crypto.Keccak256Hash([]byte("BatchMetadataUpdate(uint256,uint256)"))
	// URI is ERC-1155 URI(string,uint256).
	URI = crypto.Keccak256Hash([]byte("URI(string,uint256)"))

	// PunkTransfer is CryptoPunks PunkTransfer(address,address,uint256).
	PunkTransfer = crypto.Keccak256Hash([]byte("PunkTransfer(address,address,uint256)"))
	// PunkAssign is CryptoPunks Assign(address,uint256).
	PunkAssign = crypto.Keccak256Hash([]byte("Assign(address,uint256)"))
	// PunkBought is CryptoPunks PunkBought(uint256,uint256,address,address).
	PunkBought = crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	// CryptoPunksAddress is the only contract whose custom signatures are
	// accepted; the same three signatures from any other address are ignored,
	// mirroring the indexer's registry (ErrUnconfiguredContract → skip).
	CryptoPunksAddress = common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb")
)

// topicCount is the number of topics each standard signature must carry to be
// parseable by the indexer; a mismatch is skipped, never stored.
var topicCount = map[common.Hash]int{
	Transfer:            4,
	TransferSingle:      4,
	TransferBatch:       4,
	MetadataUpdate:      1,
	BatchMetadataUpdate: 1,
	URI:                 2,
}

// punkSignatures are accepted only from CryptoPunksAddress, in any shape.
var punkSignatures = map[common.Hash]struct{}{
	PunkTransfer: {},
	PunkAssign:   {},
	PunkBought:   {},
}

// Topics returns the topic0 filter chain ingestion subscribes with: every
// signature in the set, in a stable order. It is the exact filter the indexer
// sends in FetchIngestionLogs (standard signatures + registry custom ones).
func Topics() []common.Hash {
	return []common.Hash{
		Transfer, TransferSingle, TransferBatch, MetadataUpdate, BatchMetadataUpdate, URI,
		PunkTransfer, PunkAssign, PunkBought,
	}
}

// IsWarehouseSignature reports whether topic0 belongs to the set. It answers
// the serving-side question "can the warehouse answer a filter on this
// signature at all"; Keep answers the ingestion-side question for one log.
func IsWarehouseSignature(topic0 common.Hash) bool {
	if _, ok := topicCount[topic0]; ok {
		return true
	}
	_, ok := punkSignatures[topic0]
	return ok
}

// IsCryptoPunksSignature reports whether topic0 is one of the three
// address-scoped CryptoPunks signatures.
func IsCryptoPunksSignature(topic0 common.Hash) bool {
	_, ok := punkSignatures[topic0]
	return ok
}

// Keep reports whether a log fetched by the ingestion filter is stored.
//
// Reason: the ingestion filter is topic0-only, so it also returns ERC-20
// Transfers (3 topics), pre-standard NFT Transfers (CryptoKitties: 1 topic)
// and CryptoPunks-signature events from unrelated contracts. None of those is
// consumable by the indexer, and the backfill excluded them, so they are
// dropped here. Constraints: keep this in lock-step with the BigQuery extract
// WHERE clause in docs/probe_2026-08.md; any drift changes served results.
//
// One deliberate difference from the indexer: 4-topic TransferBatch logs are
// stored although the indexer's ERC-1155 adapter skips them at parse time —
// the extract kept them (2.0 M logs) and a consumer that wants batch
// transfers should not need a re-export.
func Keep(topics []common.Hash, address common.Address) bool {
	if len(topics) == 0 {
		return false
	}
	if want, ok := topicCount[topics[0]]; ok {
		return len(topics) == want
	}
	if _, ok := punkSignatures[topics[0]]; ok {
		return address == CryptoPunksAddress
	}
	return false
}
