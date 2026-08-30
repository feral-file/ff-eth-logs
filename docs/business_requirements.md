# Business Requirements

Business-level requirements for **FF Eth Logs**, the Ethereum NFT log warehouse.

## 1. Problem statement

The Feral File indexer reconstructs NFT ownership and provenance from Ethereum event logs. Every historical question it asks — "which tokens did this wallet ever receive", "every transfer of this token since mint", "replay this ERC-1155 balance" — is an `eth_getLogs` walk from genesis to the head against a vendor RPC. That traffic is immutable data fetched over and over, and the vendor pricing model punishes exactly that shape:

- **Span cap** — providers reject `eth_getLogs` above ~10,000 blocks per call (Infura 10,000; Chainstack 10,100), so a full-history walk is ~2,600 sequential windows per filter and an owner scan is three of them.
- **Per-call billing** — each window is a billed request (2 RU on Chainstack for archive ranges, 255 credits on Infura), so cost scales with history length, not with the number of matching logs, which is usually a handful.
- **Suppressed demand** — to stay inside the budget the indexer throttles address scans, paces scan sessions, guards `eth_getLogs` behind a call budget, and runs with **full provenance disabled**. The work the product wants is not being done; the bill only looks acceptable because of it.

The historical half of that traffic does not need a node at all. The warehouse holds the complete, shape-filtered NFT event set in PostgreSQL and answers the same `eth_getLogs` calls in one query.

## 2. Primary users and consumers

- **The indexer's routing `EthClient`** — wraps the vendor client, sends every `eth_getLogs` at or below the warehouse head here, and keeps the residual tip range, `eth_call`, receipts and `newHeads` on the vendor. Existing call sites (owner scans, ERC-721/1155 provenance, ERC-1155 replays, CryptoPunks provenance) work unchanged.
- **Internal services** that need NFT logs — anything that can speak JSON-RPC `eth_getLogs` on the private network, without a vendor key.

## 3. Core jobs to be done

1. **Serve `eth_getLogs` for the NFT event set over full history** — the vendor's matching semantics on the stored logs (the answer is the vendor's minus the documented never-stored shapes), geth's wire shapes and error strings, results ordered by `(blockNumber, logIndex)`, plus `blockTimestamp` on every log so provenance never pays for block headers.
2. **Stay current** — follow the chain head and write each block once it is `confirmation_blocks` (2) deep: the served head trails the tip by about two blocks plus one fetch round-trip.
3. **Be rebuildable** — the entire history loads from the GCS Parquet export (`gs://eth-logs/eth-log-warehouse/v1`) with the `backfill` command; the tail refills from the cursor. There is no state that only exists in the warehouse.
4. **Refuse rather than approximate** — a filter the warehouse cannot answer exactly (no `topics[0]`, a signature outside the set, a range above the head) is rejected with an out-of-scope error the client routes to the vendor.

## 4. In-scope capabilities

- **Chain**: Ethereum mainnet (`eth_chainId` = 1).
- **Events**: ERC-721 `Transfer` (4 topics), ERC-1155 `TransferSingle` / `TransferBatch` (4 topics) and `URI` (2 topics), EIP-4906 `MetadataUpdate` / `BatchMetadataUpdate` (1 topic), and CryptoPunks `PunkTransfer` / `Assign` / `PunkBought` from `0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb` only — the set and shapes the indexer's parsers accept (`internal/eventset`).
- **Methods**: `eth_getLogs`, `eth_blockNumber` (warehouse head), `eth_chainId`; `GET /health`.
- **Full history**: genesis to the head (first stored log at block 937,821).
- **Tail ingestion** with confirmation lag, bounded catch-up, dense-block receipts fallback, and shape rules identical to the backfill.
- **One-off backfill** from the BigQuery export; **rewind** for operator-driven recovery.

## 5. Out-of-scope capabilities

- **ERC-20** — 3-topic `Transfer` logs share the ERC-721 signature and are not stored: every `Transfer` filter is answered from the 4-topic logs only, which is the vendor's answer minus the ERC-20 and pre-standard transfers (the vendor ignores topic position count, so no filter shape changes this — [api design](api_design.md)). Consumers that need ERC-20 transfers ask a node.
- **Non-NFT events** and any `topics[0]` outside the set — refused, not partially answered.
- **State calls** — `eth_call`, `eth_getCode`, `eth_getTransactionReceipt`, `eth_getBlockByNumber` and every other method return `-32601`.
- **Tip data** — nothing above the warehouse head; only `latest` resolves to the head, while `safe`, `finalized` and `pending` are refused (the head is only `confirmation_blocks` deep, so those tags fall through to a node — [api design](api_design.md)).
- **Other chains** — mainnet only; one warehouse per deployment.
- **Authentication, TLS, rate limiting** — the endpoint binds on a private network; see [constraints](constraints.md).

## 6. Success criteria

- **Exactness on the stored set** — for an in-scope filter inside coverage, the response equals the vendor's response minus the documented never-stored shapes (ERC-20 / pre-standard `Transfer`s, nonstandard emitters); verified live on 2026-08-29 against Infura for owner scans, contract provenance, ERC-1155 and CryptoPunks filters (identical) and for a broad `Transfer` window (identical after removing the vendor's <4-topic logs), on backfilled and on tail-written blocks alike.
- **One query per walk** — a full-history owner scan or token provenance is a single `eth_getLogs` call served from indexed columns, not ~2,600 windows.
- **Measured size** (probe of 2026-08-28, [docs/probe_2026-08.md](probe_2026-08.md)): **402,266,375** logs to block 25,842,829, modelled at **≈ 205 GB** in PostgreSQL (510 B/log all-in), growing **≈ 1.5 GB/month** (12-month average 2.9 M logs/month).
- **Currency** — head within ~2 blocks + fetch latency of the tip in steady state; a restart resumes from the durable cursor without gaps.
- **Rebuild** — a fresh instance reaches the export head from the GCS copy with one `backfill` run and catches up the remainder over RPC.

## 7. Operational priorities

- **Predictable over fast** — ingestion failures are fatal and visible (process exit, supervisor restart, Sentry on deep reorgs), never silently skipped ranges.
- **Cursor never ahead of data** — blocks, logs and the cursor commit in one transaction, so the head is always a fully served block.
- **Cheap to run** — a single process and one PostgreSQL instance of ~250 GB; the export in GCS is the disaster-recovery source, so the database needs no backups.
- **Shared vendor budget** — the tail uses the same Chainstack endpoint as the indexer; its steady-state cost is one `newHeads` push and one `eth_getLogs` per block.

## 8. Known risks

- **Deep reorgs** — a reorg deeper than `confirmation_blocks` is repaired automatically by rewinding to the verified common ancestor and re-fetching, and reported at error level ([operations](operations.md)); a fork deeper than 1,024 blocks or below the covered interval stops ingestion for an operator. Post-merge mainnet reorgs are almost always one block deep.
- **Export freshness** — the BigQuery public dataset lags mainnet by ~2 days; the RPC catch-up after a backfill must be sized (`max_catchup_blocks`) for the age of the export at first start.
- **Event-set drift** — if the indexer's parsers accept a new signature or shape, the warehouse is incomplete for it until the extract and `eventset.Keep` change together and history is re-exported.
- **Provider behaviour** — result caps, span caps and transient errors are handled by the ported classifier and retry policy; a new provider phrasing must be added to `internal/chain/errors.go` before cut-over.
