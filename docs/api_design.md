# API Design Rules

Rules for the FF Eth Logs JSON-RPC surface. The normative contract is the code in `internal/rpcapi` (parameter decoding copied from go-ethereum `eth/filters`, served through go-ethereum's `rpc.Server`) and the filter semantics in `internal/logstore/reader.go`. This document states the contract a client can rely on and the rules for evolving it.

## 1. Transport

- **`POST /`** — JSON-RPC 2.0 over HTTP. Requests are handled by go-ethereum's `rpc.Server` (v1.16.5), so framing, batch requests (a JSON array of requests), parameter decoding and the standard error codes are geth's own: `-32700` parse error, `-32600` invalid request, `-32601` `the method <name> does not exist/is not available`, `-32602` `invalid argument <i>: <reason>` for a parameter that fails to decode.
- **`GET /health`** — JSON `{"status":"ok","head":<uint64>,"coverage_start":<uint64>,"empty":<bool>,"chain_id":<uint64>}`; `503` with `{"status":"error","error":"..."}` when the database does not answer. `head` and `coverage_start` are the covered interval, `empty` is true before the first write or backfill `finish`.
- No authentication, no TLS: private network only (see [constraints](constraints.md)).
- Timeouts: `server.read_timeout` 30 s, `server.write_timeout` 120 s, `rpc.query_timeout` 60 s per database query.

## 2. Methods

| Method | Params | Result | Notes |
| --- | --- | --- | --- |
| `eth_getLogs` | `[filter]` | `Log[]` | Section 3 |
| `eth_blockNumber` | `[]` | hex quantity | The **warehouse head** — the last block whose logs are fully stored — not the chain tip. `-32000 out of warehouse scope: warehouse is empty` before the first write |
| `eth_chainId` | `[]` | hex quantity | `ethereum.chain_id` (`0x1`) |

Every other method (including `eth_getBlockByNumber`, `eth_call`, `eth_getTransactionReceipt`, `eth_subscribe`) returns `-32601 the method <name> does not exist/is not available`.

## 3. `eth_getLogs` contract

### 3.1 Parameter parsing (identical to geth)

The single parameter is decoded by a copy of `filters.FilterCriteria.UnmarshalJSON`; a decoding failure is a `-32602 invalid argument 0: <message>` with these messages:

| Input | Rule | Message |
| --- | --- | --- |
| `blockHash` with `fromBlock` or `toBlock` | mutually exclusive | `cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other` |
| `address` | string or array of strings | `invalid addresses in query`; `non-string address at index N`; `invalid address at index N: <hex error>`; `invalid address: <hex error>`; `hex has invalid length N after decoding; expected 20 for address` |
| `topics` | at most 4 positions | `exceed max topics` |
| `topics[i]` | `null`, a string, or an array of strings/nulls; at most 1000 per position | `exceed max topics` (over 1000); `invalid topic(s)` (wrong type); `hex has invalid length N after decoding; expected 32 for topic` |
| `topics[i]` containing `null` | the whole position becomes a wildcard | — |
| `fromBlock` / `toBlock` | hex quantity or a tag (`latest`, `earliest`, `pending`, `safe`, `finalized`) | geth's `rpc.BlockNumber` errors |

### 3.2 Block tags and range rules

Resolution follows geth's `GetLogs` / `Filter.Logs` order of checks, then bounds by the head:

| Case | Result |
| --- | --- |
| `blockHash` known and inside `[coverage_start, head]` | that block only |
| `blockHash` known but outside the covered interval (rows left by an interrupted backfill, or above a rewound head) | `-32000 out of warehouse scope: block N (0x…) is outside the warehouse coverage S-H` |
| `blockHash` unknown | `-32000 unknown block` |
| `fromBlock` / `toBlock` omitted | `latest` |
| `latest` | the warehouse head `H` |
| `safe`, `finalized` | `-32000 out of warehouse scope: safe and finalized block tags are not served (the warehouse head is only confirmation_blocks deep); ask a node` — the head is ~2 blocks deep, a node's safe/finalized boundary is deeper, so the tags would promise a finality the data does not have |
| `earliest` | block 0 |
| `pending` in either bound | `-32000 pending logs are not supported` |
| both bounds explicit numbers and `fromBlock > toBlock` | `-32000 invalid block range params` |
| any other negative number | `-32000 negative block number` |
| `fromBlock > toBlock` after tag resolution (e.g. `fromBlock: "latest"` with an explicit lower `toBlock`) | `[]` |
| either bound above the head | `-32000 out of warehouse scope: blocks A-B extend above the warehouse head H` |
| `fromBlock` below the coverage start (a warehouse that holds only a tail, or a partially loaded one, never answers history it did not load) | `-32000 out of warehouse scope: blocks A-B extend below the warehouse coverage start S` |

### 3.3 Scope rule

Two rules the warehouse adds to geth's. First, **`topics[0]` must be present, non-empty, and every entry must be a warehouse signature.** Otherwise the answer would silently omit every other event on the chain, so the request is refused:

- `-32000 out of warehouse scope: filter must name at least one warehouse event signature in topics[0]`
- `-32000 out of warehouse scope: topic0 0x… is not a warehouse event signature`

Second, **what an in-scope answer is.** On the logs it stores, the warehouse applies the vendor's matching semantics exactly: addresses OR'd, a valued topic position must exist and match, an empty position (`null`, `[]`, or absent) imposes nothing — not even that the topic exists. That last point was measured, not assumed: on Infura (Geth v1.17.5, 2026-08-29, block 25,700,000) `[[Transfer]]`, `[[Transfer],null,null,null]`, `[[Transfer],[],[],[]]` and `[[Transfer],null]` all returned the same 814 logs (804 of them 3-topic ERC-20), for range and `blockHash` queries alike; go-ethereum's older `filterLogs` rule "N positions need ≥ N topics" is not what the vendor serves. Position count is therefore not a scope rule here.

What differs from the vendor is only the **stored set**: per signature, the warehouse never holds the shapes the indexer's parsers reject, so a vendor answer minus those shapes equals the warehouse answer (verified: the vendor's 25-block `[[Transfer],null,null,null]` answer filtered to 4-topic logs was identical, 65 = 65, to the warehouse's). The omitted shapes (`eventset.OmittedShapes`):

| Signature | Never stored (a node returns them, the warehouse does not) |
| --- | --- |
| `Transfer` | logs with fewer than 4 topics — ERC-20 `Transfer`, pre-standard NFT transfers (CryptoKitties) |
| `TransferSingle`, `TransferBatch` | logs with fewer than 4 topics (malformed emitters) |
| `MetadataUpdate`, `BatchMetadataUpdate` | logs with more than 1 topic (nonstandard emitters with indexed arguments) |
| `URI` | logs with a topic count other than 2 |
| CryptoPunks signatures | the same signatures emitted by any contract other than `0xb47e…3bbb` (and such filters are refused, see below) |

A client that needs any of those must ask a node; the indexer does not, because its parsers discard exactly them.

Third, **a CryptoPunks signature (`PunkTransfer`, `Assign`, `PunkBought`) is servable only when `address` is present and every entry is `0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb`.** The same signatures emitted by other contracts are not stored (`eventset.Keep`), so an unpinned or mixed-address filter would return a subset a node would not:

- `-32000 out of warehouse scope: CryptoPunks signatures are stored only for 0xb47E3cd837dDF8e4c57F05d70Ab865de6e193BBB; restrict address to it`

Scope checks run before the head is read, so they are reported even on an empty warehouse. `ScopeError` messages deliberately avoid the words `range`, `limit` and `too many` so a range-cap classifier never mistakes them for a window to halve.

### 3.4 Matching semantics

go-ethereum's `filterLogs` rules, reproduced in SQL:

- `address` values are OR'd; empty means any.
- `topics[i]` values are OR'd within a position and AND'd across positions; an empty position is a wildcard.
- A wildcard position (`null`, `[]`, or absent) imposes **nothing** — the log need not have a topic there — exactly as the vendor does (see the measurement above); `[[Transfer],[],[],[]]` and `[[Transfer]]` are the same filter. A valued position must exist and match.

### 3.5 Result shape

An array of go-ethereum `types.Log` objects, `[]` (never `null`) when nothing matches, ordered by `(blockNumber, logIndex)`:

```json
{
  "address": "0x…", "topics": ["0x…"], "data": "0x",
  "blockNumber": "0x…", "transactionHash": "0x…", "transactionIndex": "0x…",
  "blockHash": "0x…", "blockTimestamp": "0x…", "logIndex": "0x…", "removed": false
}
```

- `blockHash` and `blockTimestamp` come from `eth_blocks`; `blockTimestamp` is always present, so a client needs no `eth_getBlockByNumber` for timestamps.
- `removed` is always `false`: only confirmed blocks are stored.
- `data` is `0x` for ERC-721 `Transfer`.

### 3.6 Result cap

Exceeding `rpc.max_results` (default 100,000) returns `-32000 query returned more than 100000 results` — the Infura phrasing, so a client with the indexer's pagination halves its window instead of failing. The reply is an error, never a truncated array: a silently partial `eth_getLogs` answer is the one thing a client cannot detect.

### 3.7 The event set and the ERC-20 caveat

| Event | `topics[0]` | Stored shape |
| --- | --- | --- |
| ERC-721 `Transfer(address,address,uint256)` | `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef` | exactly 4 topics |
| ERC-1155 `TransferSingle(address,address,address,uint256,uint256)` | `0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62` | 4 topics |
| ERC-1155 `TransferBatch(address,address,address,uint256[],uint256[])` | `0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb` | 4 topics |
| EIP-4906 `MetadataUpdate(uint256)` | `0xf8e1a15aba9398e019f0b49df1a4fde98ee17ae345cb5f6b5e2c27f5033e8ce7` | 1 topic |
| EIP-4906 `BatchMetadataUpdate(uint256,uint256)` | `0x6bd5c950a8d8df17f772f5af37cb3655737899cbf903264b9795592da439661c` | 1 topic |
| ERC-1155 `URI(string,uint256)` | `0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b` | 2 topics |
| CryptoPunks `PunkTransfer(address,address,uint256)` | `0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8` | any shape, address `0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb` only |
| CryptoPunks `Assign(address,uint256)` | `0x8a0e37b73a0d9c82e205d4d1a3ff3d0b57ce5f4d7bccf6bac03336dc101cb7ba` | same |
| CryptoPunks `PunkBought(uint256,uint256,address,address)` | `0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3` | same |

The stored shapes are the ones the indexer's parsers accept; a node holds more (3-topic ERC-20 and 1-topic pre-standard `Transfer`s, nonstandard `MetadataUpdate`/`URI` shapes, CryptoPunks-signature events from other contracts). The warehouse never answers a filter those extra shapes could match — that is what the position-count and address rules above enforce — so an in-scope answer is exact and everything else is a scope error for the vendor.

## 4. What a routing client should do

1. Call `eth_blockNumber` on the warehouse to learn the head `H`.
2. Send every `eth_getLogs` whose `topics[0]` is within the set (CryptoPunks signatures pinned to the contract) and whose range lies inside `[coverage_start, H]` (`GET /health` reports both) here, unpaginated (no span cap applies), with the filter unchanged — position count does not matter to either side.
3. Send the residual range `(H, tip]`, anything below `coverage_start`, anything with an empty or foreign `topics[0]`, and anything that needs the omitted shapes (ERC-20, nonstandard emitters) to the vendor; that range is ≤ a few blocks behind the tip, bills one request and needs no pagination.
4. Treat `-32000 out of warehouse scope: …` as fall-through to the vendor, never as a retry or a window split.
5. Treat `query returned more than N results` as the existing too-many-results signal and halve the window.
6. Merge the two legs in `(blockNumber, logIndex)` order.

## 5. Compatibility expectations

- **Byte-compatible with `eth_getLogs`** for in-scope filters: the log JSON, ordering, empty-array result and error strings match go-ethereum v1.16.5. Upgrading go-ethereum is a compatibility event if it changes `types.Log` JSON or the filter error messages.
- **Additive only** — a new method or a new signature in the set is additive; removing a signature, changing a shape rule, changing the meaning of `eth_blockNumber`, or changing the scope-error code or wording is a breaking change for the routing client and needs a coordinated indexer release.
- **Errors stay distinguishable** — scope errors must never contain `range`, `limit` or `too many`; the result-cap error must keep the `query returned more than` prefix.
- **`GET /health`** keeps the four fields; add fields, do not rename them.

## 6. Checklist for adding a method

1. Add the method as an exported method on `rpcapi.API` — geth's naming maps `GetX` to `eth_getX`; keep it read-only over `rpcapi.Warehouse`.
2. Copy parameter decoding and error strings from go-ethereum rather than paraphrasing them, and say in a comment which file they came from.
3. Decide the out-of-scope behaviour explicitly: refuse with `ScopeError` rather than answer partially.
4. Add the differential/integration test in `internal/rpcapi` beside the code (coverage is attributed per package).
5. Update the methods table above, `README.md`, and `docs/architecture.md`; if the method touches the event set, update `docs/schema.md` and `internal/eventset` together.
