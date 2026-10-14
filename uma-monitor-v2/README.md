# UMA Monitor V2 (read-only)

One program that watches both UMA Optimistic Oracle V2 contracts on Polygon and tracks
each oracle request through its lifecycle, plus the wallets competing to answer it.

| Contract | Address |
|---|---|
| Standard OOV2 | `0xeE3Afe347D5C74317041E2618C49534dAf887c24` |
| Managed OOV2 | `0x2C0367a9DB231dDeBd88a94b4f6461a6e47C58B1` |

**Read-only.** No private key, no transactions, no bonds at risk.

## Requirements

- Node.js **22.13+** (uses the built-in `node:sqlite`; on older Node run `npm install better-sqlite3` and it is used instead)
- `ethers` v6 (`npm install ethers` if your project doesn't already have it)
- An Alchemy key in `.env` (`ALCHEMY_API_KEY=...`, or `POLYGON_HTTP_URL` / `POLYGON_WS_URL`). See `.env.example`.
- Optional, for the competition tracker: `ETHERSCAN_API_KEY=...` (free key from etherscan.io; one key covers Polygon through the V2 API)

## Setup in your existing project

1. Copy `v2.mjs` into your existing project folder, next to your `.env`. Don't replace or delete the old monitor scripts.
2. Import your existing data (old files are read, never modified; duplicate event IDs are ignored):
   ```
   node v2.mjs import
   ```
   With no arguments it scans `*.jsonl` / `*.ndjson` in the project folder and under `data/`, `logs/`, `output/`.
   You can also pass files or folders: `node v2.mjs import data/history.jsonl data/live.jsonl`.
3. Check the import: `node v2.mjs report`
4. Stop the two old monitors (Ctrl+C in each terminal), then run:
   ```
   node v2.mjs live
   ```
5. In a second terminal: `node v2.mjs report`, or `node v2.mjs dashboard` and open http://127.0.0.1:8787

## Commands

| Command | What it does |
|---|---|
| `import [paths...]` | Import old JSONL data into `data/v2/monitor.sqlite` |
| `live` | Confirmed polling + WebSocket fast path, whitelist snapshots. Ctrl+C stops cleanly after the current batch |
| `sync` | Catch up to the confirmed head once and exit |
| `backfill --from N [--to M]` | Rescan a range (fills gaps between old historical and live files; doesn't move checkpoints) |
| `whitelist [--limit N]` | Snapshot the default proposer whitelist and check open managed requests |
| `attempts [--from N] [--to M] [--reasons N]` | Competition tracker: every direct `proposePrice` transaction, including reverted ones (needs `ETHERSCAN_API_KEY`). `--reasons N` replays up to N failed ones to read the revert reason (eth_call on Alchemy) |
| `report` | Event counts, request statuses, proposer rankings, disputers, open opportunities, whitelist, checkpoints, RPC usage |
| `dashboard [--port 8787]` | Local read-only web dashboard |
| `label <address> <label>` | Name a wallet |
| `import-labels <file.json>` | Bulk labels: `{"0xabc…": "name"}` or `[{"address": "0x…", "label": "…"}]` |

## How it works

- **One RPC queue.** Every HTTP call goes through one serial queue with a compute-unit token bucket
  (default 240 CU/s) and a minimum 400 ms spacing. Estimated costs: `eth_getLogs` 60, `eth_call` 26,
  `eth_blockNumber` 10. One `eth_getLogs` covers both contracts. These are estimates; check real usage in the Alchemy dashboard.
- **Block ranges.** `eth_getLogs` uses 10-block chunks by default (Alchemy free-plan limit) and halves the chunk automatically if a range is rejected.
- **Reorg protection.** Events are only *confirmed* once they are `V2_CONFIRMATIONS` (32) blocks deep.
  WebSocket events are stored immediately as unconfirmed. When the confirmed scan covers their block,
  any unconfirmed event the chain no longer contains is deleted and its request is rebuilt. Logs with `removed: true` are deleted immediately.
- **Checkpoints.** Each batch of events and its checkpoint commit in one SQLite transaction. A crash never skips blocks.
- **WebSocket.** Reconnects with exponential backoff (1 s → 60 s), sends a heartbeat every 30 s and forces a reconnect after 2 silent minutes.
  While it is down, the program polls the unconfirmed chain tip over HTTP.
- **Lifecycle matching.** The request ID is `keccak256(requester, identifier, timestamp, ancillaryData)`, the same as the contract.
  A request's status and proposal outcomes are always recomputed from its stored events, so the result doesn't depend on the order events arrive in.
- **Whitelists.** It reads `defaultProposerWhitelist()` → `getWhitelist()` on the Managed OOV2 at startup and every 60 min, and saves a new snapshot only when the list changes.
  For new and open managed requests it calls `getProposerWhitelistWithEnforcementStatus(requester, identifier, ancillaryData)`.
  This returns the list that actually applies to that request (custom or default) and whether it is enforced.
  Each proposal records whether its proposer was on that list (`whitelist_basis`: `request`, `default` or `public`).

## Competition tracker

Oracle events only ever show the one accepted proposal per request. The contract rejects every later attempt.
To see who else tried, `attempts` pulls every transaction sent to each oracle from Etherscan, including reverted ones.
It decodes `proposePrice`, `proposePriceFor` and `multicall` calls, matches each one to its request, and labels it:

| Label | Meaning |
|---|---|
| `won` | The accepted proposal |
| `lost_same_block` | Reverted in the same block as the winner (a true race) |
| `lost_late` | Reverted in a later block (the request was already proposed) |
| `failed_before_winner` | Reverted earlier in the winner's block, so for some other reason |
| `not_whitelisted` | Managed OOV2 request, proposer not on the whitelist |
| `out_of_gas` | Used ≥97% of its gas limit |
| `failed_other` / `unknown_request` | Anything else / no matching request in the DB |

`report` and the dashboard's **Competition** tab show:
- wallets per request (median, max, and a distribution)
- each wallet's attempts, wins, lost races and win rate
- the most contested requests
- whether same-block winners paid a higher gas price

In `live` mode this runs every 5 minutes (`V2_ATTEMPTS_INTERVAL_MIN`) when the key is set.

**Lower bound.** Only *direct* calls to the oracle are visible. Bots that propose through their own contract, and transactions that never reached the chain, are missed.
The report prints the **direct-call coverage**: the share of accepted proposals that arrived as direct calls. If it's 60%, roughly 40% of winners, and probably a similar share of losers, use contracts.
An accepted proposer who used a contract is still counted as one of the wallets on that request.

## Tables (`data/v2/monitor.sqlite`)

| Table | Contents |
|---|---|
| `oracle_events` | Every request, proposal, dispute, settlement (plus undecoded contract events as `Unknown:<topic>`) |
| `requests` | One row per oracle request: status, title/market_id from ancillary data, reward, proposer, dispute, settlement, whitelist status |
| `proposals` | Accepted proposals: proposer, price, latency in blocks, outcome (`pending`, `settled_accepted`, `disputed_pending`, `disputed_won`, `disputed_lost`) |
| `whitelist_snapshots` | Default and per-request whitelist reads |
| `sync_checkpoints` | Last confirmed block per contract |
| `rpc_metrics` | Per-minute, per-method call counts, errors, latency and estimated CUs |
| `transaction_attempts` | Every direct propose/dispute transaction (winning or reverted): wallet, block, position, gas price, classification, revert reason |
| `wallet_labels`, `import_log` | Labels and import history |

You can query it directly, e.g. `sqlite3 data/v2/monitor.sqlite "select * from proposals limit 10"`.

## Known limits (V2.1 work)

- Competition counts are a lower bound (see above). Attempts through bot contracts would need transaction tracing, which Alchemy's free plan doesn't include.
- Whitelist reads show the *current* state, not the state at the historical block. Imported or backfilled history is compared against the latest default snapshot (`whitelist_basis = default`).
- Legacy JSONL rows without `ancillaryData` can't be matched exactly to a request. They're grouped under `partial:` request keys, and `import` reports how many there are.
- If old files have no contract address and their file name contains neither `managed` nor `standard`, those events are stored with contract `unknown`.
