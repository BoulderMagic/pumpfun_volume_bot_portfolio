#!/usr/bin/env node
// UMA Monitor V2 — read-only competition monitor for UMA Optimistic Oracle V2 on Polygon.
//
// Tracks the Standard OOV2 and the Managed OOV2 through one shared RPC queue / CU limiter,
// matches requests -> proposals -> disputes -> settlements, snapshots proposer whitelists,
// and stores everything in one SQLite database (data/v2/monitor.sqlite).
//
// It never needs a private key and never sends a transaction.
//
// Commands:
//   node v2.mjs import [files or dirs...]   import old JSONL monitor data (dedupes by event id)
//   node v2.mjs live                        confirmed polling + WebSocket fast path (Ctrl+C to stop)
//   node v2.mjs sync                        catch up to the confirmed head once, then exit
//   node v2.mjs backfill --from N [--to M]  rescan a block range (fills gaps, does not move checkpoints)
//   node v2.mjs whitelist [--limit N]       snapshot default whitelist + check open managed requests
//   node v2.mjs attempts [--from N] [--reasons N]  competition tracker: every direct proposePrice tx, incl. reverted (Etherscan)
//   node v2.mjs report                      print statistics
//   node v2.mjs dashboard [--port 8787]     local web dashboard (http://127.0.0.1:8787)
//   node v2.mjs label <address> <label>     attach a label to a wallet
//   node v2.mjs import-labels <file.json>   bulk labels: {"0xabc": "label"} or [{address,label}]

import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';
import { Interface, keccak256, solidityPacked, toUtf8String, Utf8ErrorFuncs } from 'ethers';

// ───────────────────────────────────────────────────────────── config

if (typeof process.loadEnvFile === 'function' && fs.existsSync('.env')) process.loadEnvFile('.env');

const env = (...names) => {
  for (const n of names) if (process.env[n] && process.env[n].trim()) return process.env[n].trim();
  return undefined;
};
const num = (v, d) => (v === undefined || v === '' || Number.isNaN(Number(v)) ? d : Number(v));

function resolveUrls() {
  const key = env('ALCHEMY_API_KEY', 'ALCHEMY_KEY');
  let httpUrl = env('POLYGON_HTTP_URL', 'ALCHEMY_HTTP_URL', 'ALCHEMY_POLYGON_URL', 'ALCHEMY_URL', 'POLYGON_RPC_URL', 'RPC_URL', 'HTTP_RPC_URL');
  let wsUrl = env('POLYGON_WS_URL', 'POLYGON_WSS_URL', 'ALCHEMY_WS_URL', 'ALCHEMY_POLYGON_WS', 'WS_URL', 'WSS_URL', 'WS_RPC_URL');
  if (!httpUrl && key) httpUrl = `https://polygon-mainnet.g.alchemy.com/v2/${key}`;
  if (httpUrl && httpUrl.startsWith('ws')) { wsUrl ??= httpUrl; httpUrl = httpUrl.replace(/^ws/, 'http'); }
  if (!wsUrl && key) wsUrl = `wss://polygon-mainnet.g.alchemy.com/v2/${key}`;
  if (!wsUrl && httpUrl && /alchemy\.com/.test(httpUrl)) wsUrl = httpUrl.replace(/^https?/, 'wss');
  if (wsUrl && /^https?:/.test(wsUrl)) wsUrl = wsUrl.replace(/^http/, 'ws'); // e.g. POLYGON_WSS_URL=https://...alchemy...
  return { httpUrl, wsUrl };
}

const { httpUrl, wsUrl } = resolveUrls();
const CFG = {
  httpUrl,
  wsUrl: env('V2_DISABLE_WS') === '1' ? undefined : wsUrl,
  dbFile: env('V2_DB') ?? path.join('data', 'v2', 'monitor.sqlite'),
  cuPerSec: num(env('V2_CU_PER_SEC'), 240),
  minIntervalMs: num(env('V2_MIN_INTERVAL_MS'), 400),
  logChunk: num(env('V2_LOG_CHUNK'), 10), // Alchemy free plan: eth_getLogs limited to small block ranges
  confirmations: num(env('V2_CONFIRMATIONS'), 32),
  pollMs: num(env('V2_POLL_MS'), 15000),
  startBlock: env('V2_START_BLOCK') ? Number(env('V2_START_BLOCK')) : undefined,
  defaultLookback: num(env('V2_DEFAULT_LOOKBACK'), 2000),
  whitelistRefreshMin: num(env('V2_WHITELIST_REFRESH_MIN'), 60),
  whitelistChecksPerCycle: num(env('V2_WHITELIST_CHECKS_PER_CYCLE'), 5),
  dashboardPort: num(env('V2_DASHBOARD_PORT'), 8787),
  attemptsIntervalMin: num(env('V2_ATTEMPTS_INTERVAL_MIN'), 5),
};

// Estimated Alchemy compute units per method. eth_getLogs uses our working estimate of 60.
const CU_COST = {
  eth_getLogs: 60, eth_call: 26, eth_blockNumber: 10, eth_chainId: 0, eth_getBlockByNumber: 16,
  eth_getTransactionReceipt: 20, eth_getTransactionByHash: 17, eth_subscribe: 10,
};
const cuFor = (m) => CU_COST[m] ?? 26;

const CONTRACTS = [
  { key: 'standard', name: 'Standard OOV2', address: (env('V2_STANDARD_ADDRESS') ?? '0xee3afe347d5c74317041e2618c49534daf887c24').toLowerCase() },
  { key: 'managed', name: 'Managed OOV2', address: (env('V2_MANAGED_ADDRESS') ?? '0x2c0367a9db231ddebd88a94b4f6461a6e47c58b1').toLowerCase() },
];
const BY_ADDRESS = new Map(CONTRACTS.map((c) => [c.address, c]));
const BY_KEY = new Map(CONTRACTS.map((c) => [c.key, c]));
const MANAGED = BY_KEY.get('managed');

// ───────────────────────────────────────────────────────────── ABIs

const OO_EVENTS = new Interface([
  'event RequestPrice(address indexed requester, bytes32 identifier, uint256 timestamp, bytes ancillaryData, address currency, uint256 reward, uint256 finalFee)',
  'event ProposePrice(address indexed requester, address indexed proposer, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 proposedPrice, uint256 expirationTimestamp, address currency)',
  'event DisputePrice(address indexed requester, address indexed proposer, address indexed disputer, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 proposedPrice)',
  'event Settle(address indexed requester, address indexed proposer, address indexed disputer, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 price, uint256 payout)',
]);
const MANAGED_FNS = new Interface([
  'function defaultProposerWhitelist() view returns (address)',
  'function getProposerWhitelistWithEnforcementStatus(address requester, bytes32 identifier, bytes ancillaryData) view returns (bool isEnforced, address[] allowedProposers)',
]);
const WHITELIST_FNS = new Interface(['function getWhitelist() view returns (address[])']);
const LIFECYCLE = new Set(['RequestPrice', 'ProposePrice', 'DisputePrice', 'Settle']);

// ───────────────────────────────────────────────────────────── helpers

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowSec = () => Math.floor(Date.now() / 1000);
const lc = (v) => (typeof v === 'string' ? v.toLowerCase() : v);
const toInt = (v) => {
  if (v === undefined || v === null || v === '') return null;
  if (typeof v === 'number') return v;
  if (typeof v === 'bigint') return Number(v);
  if (typeof v === 'object' && v.hex) return Number(BigInt(v.hex));
  const s = String(v);
  return s.startsWith('0x') ? Number(BigInt(s)) : Number(s);
};
const toBigStr = (v) => {
  if (v === undefined || v === null || v === '') return null;
  if (typeof v === 'bigint') return v.toString();
  if (typeof v === 'object' && (v.hex || v._hex)) return BigInt(v.hex ?? v._hex).toString();
  if (typeof v === 'object' && v.type === 'BigInt' && v.value !== undefined) return String(v.value);
  const s = String(v);
  try { return BigInt(s).toString(); } catch { return s; }
};
const short = (a) => (a && a.length > 12 ? `${a.slice(0, 6)}…${a.slice(-4)}` : a ?? '');
const ts = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const log = (...a) => console.log(`[${ts()}]`, ...a);
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');

function bytes32ToString(hex) {
  if (!hex || !hex.startsWith('0x')) return hex ?? null;
  const clean = hex.slice(2).replace(/(00)+$/, '');
  if (!clean) return '';
  try { return toUtf8String('0x' + clean, Utf8ErrorFuncs.replace); } catch { return hex; }
}
function ancillaryToText(hex) {
  if (!hex || hex === '0x') return '';
  try { return toUtf8String(hex, Utf8ErrorFuncs.replace); } catch { return ''; }
}
function parseAncillary(text) {
  if (!text) return { title: null, marketId: null };
  const title = text.match(/title:\s*(.*?)(?:,\s*description:|$)/s)?.[1]?.trim() ?? null;
  const marketId = text.match(/market_id:\s*(\d+)/)?.[1] ?? null;
  return { title: title ? title.slice(0, 300) : null, marketId };
}
function requestIdOf(requester, identifier, timestamp, ancillaryData) {
  return keccak256(solidityPacked(['address', 'bytes32', 'uint256', 'bytes'], [requester, identifier, BigInt(timestamp), ancillaryData]));
}
function argsToPlain(fragment, args) {
  const out = {};
  fragment.inputs.forEach((inp, i) => {
    const v = args[i];
    out[inp.name] = typeof v === 'bigint' ? v.toString() : typeof v === 'string' ? (inp.type === 'address' ? v.toLowerCase() : v) : v;
  });
  return out;
}

// ───────────────────────────────────────────────────────────── database

async function openDatabase(file) {
  fs.mkdirSync(path.dirname(path.resolve(file)), { recursive: true });
  const origEmit = process.emitWarning;
  process.emitWarning = (w, ...rest) => { if (String(w?.message ?? w).includes('SQLite')) return; return origEmit.call(process, w, ...rest); };
  let db;
  try {
    const { DatabaseSync } = await import('node:sqlite');
    db = new DatabaseSync(file);
  } catch (e) {
    try {
      const Better = (await import('better-sqlite3')).default;
      db = new Better(file);
    } catch {
      throw new Error(`No SQLite driver available. Use Node.js 22.13+ (built-in node:sqlite) or run "npm install better-sqlite3". (${e.message})`);
    }
  }
  db.exec('PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 10000; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = OFF;');
  db.exec(SCHEMA);
  return db;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS oracle_events (
  event_id TEXT PRIMARY KEY,            -- txHash:logIndex (or legacy:<sha256> for rows without them)
  contract TEXT NOT NULL,               -- standard | managed | unknown
  event_name TEXT NOT NULL,
  block_number INTEGER,
  block_hash TEXT,
  tx_hash TEXT,
  log_index INTEGER,
  request_key TEXT,                     -- contract:requestId
  requester TEXT, proposer TEXT, disputer TEXT,
  identifier TEXT,
  request_timestamp INTEGER,
  price TEXT,                           -- proposedPrice / settled price
  amount TEXT,                          -- reward (RequestPrice) / payout (Settle)
  args_json TEXT,                       -- decoded args without ancillaryData (stored once in requests)
  confirmed INTEGER NOT NULL DEFAULT 1, -- 0 = seen via WebSocket/tip poll, not yet past confirmation depth
  source TEXT,
  inserted_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events_request ON oracle_events(request_key);
CREATE INDEX IF NOT EXISTS idx_events_block ON oracle_events(block_number);
CREATE INDEX IF NOT EXISTS idx_events_name ON oracle_events(event_name, contract);
CREATE INDEX IF NOT EXISTS idx_events_proposer ON oracle_events(proposer);

CREATE TABLE IF NOT EXISTS requests (
  request_key TEXT PRIMARY KEY,
  contract TEXT NOT NULL,
  request_id TEXT,
  requester TEXT, identifier TEXT, request_timestamp INTEGER,
  ancillary_hex TEXT, ancillary_text TEXT, title TEXT, market_id TEXT,
  currency TEXT, reward TEXT, final_fee TEXT,
  status TEXT,                          -- Requested | Proposed | Disputed | Settled | Unknown
  requested_block INTEGER, requested_tx TEXT,
  proposal_count INTEGER DEFAULT 0, first_proposal_block INTEGER, first_proposer TEXT,
  latest_proposer TEXT, proposed_price TEXT, expiration_timestamp INTEGER,
  dispute_count INTEGER DEFAULT 0, last_disputer TEXT,
  settled_block INTEGER, settled_price TEXT, payout TEXT,
  whitelist_enforced INTEGER, whitelist_size INTEGER, whitelist_checked_at INTEGER,
  has_unconfirmed INTEGER DEFAULT 0,
  updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(contract, status);

CREATE TABLE IF NOT EXISTS proposals (
  event_id TEXT PRIMARY KEY,
  request_key TEXT NOT NULL,
  contract TEXT NOT NULL,
  proposer TEXT, proposed_price TEXT, expiration_timestamp INTEGER,
  block_number INTEGER, tx_hash TEXT,
  latency_blocks INTEGER,               -- proposal block - request block (when the request was seen)
  outcome TEXT,                         -- pending | settled_accepted | disputed_pending | disputed_won | disputed_lost
  disputer TEXT,
  proposer_on_whitelist INTEGER,        -- 1/0 when a snapshot exists, NULL otherwise
  whitelist_basis TEXT                  -- request | default | public
);
CREATE INDEX IF NOT EXISTS idx_proposals_proposer ON proposals(proposer);
CREATE INDEX IF NOT EXISTS idx_proposals_request ON proposals(request_key);

CREATE TABLE IF NOT EXISTS whitelist_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  taken_at INTEGER NOT NULL,
  block_number INTEGER,
  contract TEXT NOT NULL,
  scope TEXT NOT NULL,                  -- default | request
  request_key TEXT,
  whitelist_address TEXT,
  enforced INTEGER,
  address_count INTEGER,
  addresses_hash TEXT,
  addresses_json TEXT,                  -- NULL when identical to the latest default snapshot
  error TEXT
);
CREATE INDEX IF NOT EXISTS idx_wl_scope ON whitelist_snapshots(scope, request_key, id);

CREATE TABLE IF NOT EXISTS sync_checkpoints (
  contract TEXT PRIMARY KEY,
  last_block INTEGER NOT NULL,
  updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS rpc_metrics (
  minute INTEGER NOT NULL,              -- unix seconds, floored to the minute
  method TEXT NOT NULL,
  calls INTEGER DEFAULT 0, errors INTEGER DEFAULT 0,
  total_ms INTEGER DEFAULT 0, max_ms INTEGER DEFAULT 0,
  est_cu INTEGER DEFAULT 0,
  last_error TEXT,
  PRIMARY KEY (minute, method)
);

-- Populated by the V2.1 transaction-analysis component (failed / reverted competing proposals).
CREATE TABLE IF NOT EXISTS transaction_attempts (
  tx_hash TEXT PRIMARY KEY,
  request_key TEXT,
  contract TEXT,
  from_address TEXT,
  block_number INTEGER,
  status TEXT,                          -- success | reverted | dropped
  gas_used TEXT,
  revert_reason TEXT,
  detected_at INTEGER,
  source TEXT
);

CREATE TABLE IF NOT EXISTS wallet_labels (
  address TEXT PRIMARY KEY,
  label TEXT,
  source TEXT,
  updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS import_log (
  file TEXT PRIMARY KEY,
  size INTEGER, mtime INTEGER,
  lines INTEGER, inserted INTEGER, duplicates INTEGER, skipped INTEGER,
  imported_at INTEGER
);
`;

function tx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { try { db.exec('ROLLBACK'); } catch {} throw e; }
}

class Store {
  constructor(db) {
    this.db = db;
    this.q = {};
    const p = (k, sql) => (this.q[k] = db.prepare(sql));
    p('eventExists', 'SELECT confirmed FROM oracle_events WHERE event_id = ?');
    p('insertEvent', `INSERT INTO oracle_events (event_id, contract, event_name, block_number, block_hash, tx_hash, log_index, request_key, requester, proposer, disputer, identifier, request_timestamp, price, amount, args_json, confirmed, source, inserted_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    p('confirmEvent', 'UPDATE oracle_events SET confirmed = 1, block_number = ?, block_hash = ? WHERE event_id = ?');
    p('deleteEvent', 'DELETE FROM oracle_events WHERE event_id = ?');
    p('eventKey', 'SELECT request_key FROM oracle_events WHERE event_id = ?');
    p('unconfirmedInRange', 'SELECT event_id, request_key FROM oracle_events WHERE confirmed = 0 AND block_number BETWEEN ? AND ?');
    p('insertRequest', `INSERT OR IGNORE INTO requests (request_key, contract, request_id, requester, identifier, request_timestamp, ancillary_hex, ancillary_text, title, market_id, status, updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?, 'Unknown', ?)`);
    p('eventsForRequest', 'SELECT * FROM oracle_events WHERE request_key = ? ORDER BY block_number, log_index');
    p('deleteRequest', 'DELETE FROM requests WHERE request_key = ?');
    p('deleteProposals', 'DELETE FROM proposals WHERE request_key = ?');
    p('insertProposal', `INSERT OR REPLACE INTO proposals (event_id, request_key, contract, proposer, proposed_price, expiration_timestamp, block_number, tx_hash, latency_blocks, outcome, disputer, proposer_on_whitelist, whitelist_basis)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    p('updateRequest', `UPDATE requests SET status=?, requested_block=?, requested_tx=?, currency=?, reward=?, final_fee=?, proposal_count=?, first_proposal_block=?, first_proposer=?,
      latest_proposer=?, proposed_price=?, expiration_timestamp=?, dispute_count=?, last_disputer=?, settled_block=?, settled_price=?, payout=?, has_unconfirmed=?, updated_at=? WHERE request_key=?`);
    p('getCheckpoint', 'SELECT last_block FROM sync_checkpoints WHERE contract = ?');
    p('setCheckpoint', `INSERT INTO sync_checkpoints (contract, last_block, updated_at) VALUES (?,?,?)
      ON CONFLICT(contract) DO UPDATE SET last_block = MAX(last_block, excluded.last_block), updated_at = excluded.updated_at`);
    p('metric', `INSERT INTO rpc_metrics (minute, method, calls, errors, total_ms, max_ms, est_cu, last_error) VALUES (?,?,1,?,?,?,?,?)
      ON CONFLICT(minute, method) DO UPDATE SET calls = calls + 1, errors = errors + excluded.errors, total_ms = total_ms + excluded.total_ms,
      max_ms = MAX(max_ms, excluded.max_ms), est_cu = est_cu + excluded.est_cu, last_error = COALESCE(excluded.last_error, last_error)`);
    p('latestDefaultWl', "SELECT * FROM whitelist_snapshots WHERE scope = 'default' AND error IS NULL ORDER BY id DESC LIMIT 1");
    p('latestRequestWl', "SELECT * FROM whitelist_snapshots WHERE scope = 'request' AND request_key = ? AND error IS NULL ORDER BY id DESC LIMIT 1");
    p('insertWl', `INSERT INTO whitelist_snapshots (taken_at, block_number, contract, scope, request_key, whitelist_address, enforced, address_count, addresses_hash, addresses_json, error)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
    p('setRequestWl', 'UPDATE requests SET whitelist_enforced = ?, whitelist_size = ?, whitelist_checked_at = ? WHERE request_key = ?');
    p('upsertLabel', `INSERT INTO wallet_labels (address, label, source, updated_at) VALUES (?,?,?,?)
      ON CONFLICT(address) DO UPDATE SET label = excluded.label, source = excluded.source, updated_at = excluded.updated_at`);
    this.defaultWl = null; // { addresses:Set, hash, id }
  }

  checkpoint(contract) { return this.q.getCheckpoint.get(contract)?.last_block ?? null; }
  setCheckpoint(contract, block) { this.q.setCheckpoint.run(contract, block, nowSec()); }

  recordMetric(method, ms, ok, error, cu) {
    try {
      this.q.metric.run(Math.floor(Date.now() / 60000) * 60, method, ok ? 0 : 1, Math.round(ms), Math.round(ms), cu, error ? String(error).slice(0, 300) : null);
    } catch { /* metrics must never break the monitor */ }
  }

  loadDefaultWhitelist() {
    const row = this.q.latestDefaultWl.get();
    this.defaultWl = row ? { id: row.id, hash: row.addresses_hash, address: row.whitelist_address, addresses: new Set(JSON.parse(row.addresses_json ?? '[]')) } : null;
    return this.defaultWl;
  }

  // Write one decoded event. Returns 'new' | 'confirmed' | 'dup'.
  putEvent(ev, confirmed, source) {
    const existing = this.q.eventExists.get(ev.event_id);
    if (existing) {
      if (confirmed && !existing.confirmed) { this.q.confirmEvent.run(ev.block_number, ev.block_hash ?? null, ev.event_id); return 'confirmed'; }
      return 'dup';
    }
    this.q.insertEvent.run(ev.event_id, ev.contract, ev.event_name, ev.block_number ?? null, ev.block_hash ?? null, ev.tx_hash ?? null, ev.log_index ?? null,
      ev.request_key ?? null, ev.requester ?? null, ev.proposer ?? null, ev.disputer ?? null, ev.identifier ?? null, ev.request_timestamp ?? null,
      ev.price ?? null, ev.amount ?? null, JSON.stringify(ev.args ?? {}), confirmed ? 1 : 0, source, nowSec());
    if (ev.request_key) {
      const text = ancillaryToText(ev.ancillary_hex);
      const { title, marketId } = parseAncillary(text);
      this.q.insertRequest.run(ev.request_key, ev.contract, ev.request_id ?? null, ev.requester ?? null, ev.identifier ?? null, ev.request_timestamp ?? null,
        ev.ancillary_hex ?? null, text || null, title, marketId, nowSec());
    }
    return 'new';
  }

  removeEvent(eventId) {
    const key = this.q.eventKey.get(eventId)?.request_key;
    this.q.deleteEvent.run(eventId);
    return key;
  }

  // Recompute a request's lifecycle + its proposals from its events (order-independent, reorg-safe).
  rebuildRequest(key) {
    const evs = this.q.eventsForRequest.all(key);
    this.q.deleteProposals.run(key);
    if (!evs.length) { this.q.deleteRequest.run(key); return; }
    const contract = evs[0].contract;
    const r = {
      status: 'Unknown', requested_block: null, requested_tx: null, currency: null, reward: null, final_fee: null,
      proposal_count: 0, first_proposal_block: null, first_proposer: null, latest_proposer: null, proposed_price: null, expiration_timestamp: null,
      dispute_count: 0, last_disputer: null, settled_block: null, settled_price: null, payout: null, has_unconfirmed: 0,
    };
    const proposals = [];
    for (const e of evs) {
      const a = JSON.parse(e.args_json || '{}');
      if (!e.confirmed) r.has_unconfirmed = 1;
      switch (e.event_name) {
        case 'RequestPrice':
          r.requested_block = e.block_number; r.requested_tx = e.tx_hash;
          r.currency = a.currency ?? null; r.reward = toBigStr(a.reward); r.final_fee = toBigStr(a.finalFee);
          if (r.status === 'Unknown') r.status = 'Requested';
          break;
        case 'ProposePrice':
          r.proposal_count++;
          r.first_proposal_block ??= e.block_number; r.first_proposer ??= e.proposer;
          r.latest_proposer = e.proposer; r.proposed_price = e.price; r.expiration_timestamp = toInt(a.expirationTimestamp);
          if (r.status !== 'Settled') r.status = 'Proposed';
          proposals.push({ e, a, outcome: 'pending', disputer: null });
          break;
        case 'DisputePrice': {
          r.dispute_count++; r.last_disputer = e.disputer;
          if (r.status !== 'Settled') r.status = 'Disputed';
          const p = [...proposals].reverse().find((x) => x.outcome === 'pending' && x.e.proposer === e.proposer) ?? proposals.at(-1);
          if (p) { p.outcome = 'disputed_pending'; p.disputer = e.disputer; }
          break;
        }
        case 'Settle': {
          r.status = 'Settled'; r.settled_block = e.block_number; r.settled_price = e.price; r.payout = e.amount;
          for (const p of proposals) {
            if (p.outcome === 'pending') p.outcome = 'settled_accepted';
            else if (p.outcome === 'disputed_pending') p.outcome = p.e.price === e.price ? 'disputed_won' : 'disputed_lost';
          }
          break;
        }
      }
    }
    this.q.updateRequest.run(r.status, r.requested_block, r.requested_tx, r.currency, r.reward, r.final_fee, r.proposal_count, r.first_proposal_block, r.first_proposer,
      r.latest_proposer, r.proposed_price, r.expiration_timestamp, r.dispute_count, r.last_disputer, r.settled_block, r.settled_price, r.payout, r.has_unconfirmed, nowSec(), key);

    const reqWl = contract === 'managed' ? this.q.latestRequestWl.get(key) : null;
    let wlSet = null, basis = contract === 'standard' ? 'public' : null;
    if (reqWl) {
      basis = 'request';
      wlSet = reqWl.enforced ? new Set(reqWl.addresses_json ? JSON.parse(reqWl.addresses_json) : this.defaultWl?.hash === reqWl.addresses_hash ? this.defaultWl.addresses : []) : null;
      if (!reqWl.enforced) basis = 'public';
    } else if (contract === 'managed' && this.defaultWl) {
      basis = 'default'; wlSet = this.defaultWl.addresses;
    }
    for (const p of proposals) {
      const onWl = wlSet ? (wlSet.has(p.e.proposer) ? 1 : 0) : null;
      this.q.insertProposal.run(p.e.event_id, key, contract, p.e.proposer, p.e.price, toInt(p.a.expirationTimestamp), p.e.block_number, p.e.tx_hash,
        r.requested_block != null && p.e.block_number != null ? p.e.block_number - r.requested_block : null, p.outcome, p.disputer, onWl, basis);
    }
  }
}

// ───────────────────────────────────────────────────────────── log decoding

// Turns a raw eth_getLogs / eth_subscribe log into a normalized event record.
function decodeLog(rawLog) {
  const address = lc(rawLog.address);
  const contract = BY_ADDRESS.get(address)?.key ?? 'unknown';
  const base = {
    event_id: `${lc(rawLog.transactionHash)}:${toInt(rawLog.logIndex)}`,
    contract, block_number: toInt(rawLog.blockNumber), block_hash: lc(rawLog.blockHash), tx_hash: lc(rawLog.transactionHash), log_index: toInt(rawLog.logIndex),
  };
  let parsed = null;
  try { parsed = OO_EVENTS.parseLog({ topics: rawLog.topics, data: rawLog.data }); } catch { /* not a lifecycle event */ }
  if (!parsed) {
    return { ...base, event_name: `Unknown:${(rawLog.topics?.[0] ?? '0x').slice(0, 10)}`, args: { topics: rawLog.topics, data: (rawLog.data ?? '').slice(0, 4000) } };
  }
  return fromArgs(base, parsed.name, argsToPlain(parsed.fragment, parsed.args));
}

// Builds the normalized record from named args (shared by live decoding and legacy import).
function fromArgs(base, name, a) {
  const requester = lc(a.requester), identifierHex = a.identifier, anc = a.ancillaryData;
  const ev = {
    ...base, event_name: name, requester, proposer: lc(a.proposer) ?? null, disputer: lc(a.disputer) ?? null,
    identifier: bytes32ToString(identifierHex), request_timestamp: toInt(a.timestamp),
  };
  if (name === 'ProposePrice' || name === 'DisputePrice') ev.price = toBigStr(a.proposedPrice);
  if (name === 'Settle') { ev.price = toBigStr(a.price); ev.amount = toBigStr(a.payout); }
  if (name === 'RequestPrice') ev.amount = toBigStr(a.reward);
  if (requester && identifierHex?.startsWith?.('0x') && identifierHex.length === 66 && ev.request_timestamp != null && typeof anc === 'string' && anc.startsWith('0x')) {
    ev.request_id = requestIdOf(requester, identifierHex, ev.request_timestamp, anc);
    ev.request_key = `${base.contract}:${ev.request_id}`;
    ev.ancillary_hex = anc;
  } else if (requester && ev.request_timestamp != null) {
    // Legacy rows without ancillary data cannot be matched exactly; keep them grouped separately.
    ev.request_key = `partial:${base.contract}:${requester}:${ev.identifier}:${ev.request_timestamp}`;
  }
  const args = { ...a };
  delete args.ancillaryData;
  for (const k of Object.keys(args)) if (typeof args[k] === 'bigint') args[k] = args[k].toString();
  ev.args = args;
  return ev;
}

// Writes a batch of raw logs in one transaction; optionally prunes reorged unconfirmed rows and advances checkpoints.
function writeLogBatch(store, logs, { confirmed, range, advanceCheckpoint, source }) {
  return tx(store.db, () => {
    const keys = new Set();
    const stats = { new: 0, confirmed: 0, dup: 0, removed: 0, newManagedRequests: [] };
    const seen = new Set();
    for (const raw of logs) {
      if (raw.removed) {
        const id = `${lc(raw.transactionHash)}:${toInt(raw.logIndex)}`;
        const k = store.removeEvent(id); if (k) keys.add(k); stats.removed++;
        continue;
      }
      const ev = decodeLog(raw);
      seen.add(ev.event_id);
      const res = store.putEvent(ev, confirmed, source);
      stats[res]++;
      if (ev.request_key) keys.add(ev.request_key);
      if (res === 'new' && ev.contract === 'managed' && ev.event_name === 'RequestPrice') stats.newManagedRequests.push(ev.request_key);
    }
    if (confirmed && range) {
      for (const row of store.q.unconfirmedInRange.all(range[0], range[1])) {
        if (!seen.has(row.event_id)) { store.q.deleteEvent.run(row.event_id); if (row.request_key) keys.add(row.request_key); stats.removed++; }
      }
    }
    for (const k of keys) store.rebuildRequest(k);
    if (advanceCheckpoint && range) for (const c of CONTRACTS) store.setCheckpoint(c.key, range[1]);
    return stats;
  });
}

// ───────────────────────────────────────────────────────────── RPC (shared queue + CU limiter)

class RpcError extends Error { constructor(msg, code) { super(msg); this.code = code; } }

class RpcClient {
  constructor(url, store) {
    if (!url) throw new Error('No RPC URL configured. Set ALCHEMY_API_KEY or POLYGON_HTTP_URL in .env');
    this.url = url; this.store = store;
    this.capacity = CFG.cuPerSec; this.tokens = CFG.cuPerSec; this.refillAt = Date.now(); this.lastStart = 0;
    this.chain = Promise.resolve(); this.id = 0;
  }
  call(method, params = []) {
    const p = this.chain.then(() => this.#exec(method, params));
    this.chain = p.catch(() => {});
    return p;
  }
  async #acquire(cost) {
    for (;;) {
      const now = Date.now();
      this.tokens = Math.min(this.capacity, this.tokens + ((now - this.refillAt) / 1000) * CFG.cuPerSec);
      this.refillAt = now;
      const spacing = CFG.minIntervalMs - (now - this.lastStart);
      const budget = cost > this.tokens ? ((cost - this.tokens) / CFG.cuPerSec) * 1000 : 0;
      const wait = Math.max(spacing, budget);
      if (wait <= 0) { this.tokens -= cost; this.lastStart = now; return; }
      await sleep(Math.ceil(wait));
    }
  }
  async #exec(method, params) {
    const cost = cuFor(method);
    let lastErr;
    for (let attempt = 0; attempt < 6; attempt++) {
      await this.#acquire(cost);
      const t0 = performance.now();
      try {
        const res = await fetch(this.url, {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ jsonrpc: '2.0', id: ++this.id, method, params }), signal: AbortSignal.timeout(30000),
        });
        const ms = performance.now() - t0;
        if (res.status === 429) { this.store?.recordMetric(method, ms, false, 'HTTP 429', cost); lastErr = new RpcError('HTTP 429 rate limited', 429); await sleep(1000 * 2 ** attempt); continue; }
        const text = await res.text();
        let body; try { body = JSON.parse(text); } catch { throw new RpcError(`HTTP ${res.status}: ${text.slice(0, 200)}`, res.status); }
        if (body.error) {
          const code = body.error.code, msg = body.error.message ?? JSON.stringify(body.error);
          this.store?.recordMetric(method, ms, false, `${code}: ${msg}`, cost);
          if (code === 429 || /rate limit|exceeded its compute units|throughput/i.test(msg)) { lastErr = new RpcError(msg, 429); await sleep(1000 * 2 ** attempt); continue; }
          throw new RpcError(msg, code);
        }
        this.store?.recordMetric(method, ms, true, null, cost);
        return body.result;
      } catch (e) {
        if (e instanceof RpcError) throw e;
        this.store?.recordMetric(method, performance.now() - t0, false, e.message, cost);
        lastErr = e;
        await sleep(1000 * 2 ** attempt);
      }
    }
    throw lastErr;
  }
  async blockNumber() { return Number(BigInt(await this.call('eth_blockNumber'))); }
  getLogs(from, to) {
    return this.call('eth_getLogs', [{ address: CONTRACTS.map((c) => c.address), fromBlock: '0x' + from.toString(16), toBlock: '0x' + to.toString(16) }]);
  }
  ethCall(to, data, block = 'latest') { return this.call('eth_call', [{ to, data }, block]); }
}

// ───────────────────────────────────────────────────────────── sync engine

class Syncer {
  constructor(store, rpc) { this.store = store; this.rpc = rpc; this.chunk = CFG.logChunk; this.stopped = false; this.pendingWl = new Set(); }

  // Fetch [from, to] in chunks, shrinking the chunk if the provider rejects the range.
  async scan(from, to, { confirmed, advanceCheckpoint, label }) {
    const totals = { new: 0, confirmed: 0, dup: 0, removed: 0, calls: 0 };
    let cur = from, lastLog = Date.now();
    while (cur <= to && !this.stopped) {
      const end = Math.min(cur + this.chunk - 1, to);
      let logs;
      try {
        logs = await this.rpc.getLogs(cur, end);
      } catch (e) {
        if (this.chunk > 1 && /range|block range|limit|too many|response size|10 block|query returned more/i.test(e.message)) {
          this.chunk = Math.max(1, Math.floor(this.chunk / 2));
          log(`eth_getLogs range rejected; reducing chunk to ${this.chunk} blocks (${e.message.slice(0, 120)})`);
          continue;
        }
        throw e;
      }
      totals.calls++;
      const s = writeLogBatch(this.store, logs, { confirmed, range: [cur, end], advanceCheckpoint, source: confirmed ? 'rpc' : 'rpc-tip' });
      for (const k of ['new', 'confirmed', 'dup', 'removed']) totals[k] += s[k];
      for (const k of s.newManagedRequests) this.pendingWl.add(k);
      if (s.new || s.removed) log(`${label} ${cur}-${end}: +${s.new} new, ${s.confirmed} confirmed, ${s.removed} removed`);
      if (Date.now() - lastLog > 30000 && to - end > 0) { log(`${label} progress: block ${end} / ${to} (${to - end} remaining)`); lastLog = Date.now(); }
      cur = end + 1;
    }
    return totals;
  }

  async startBlock(head) {
    const cps = CONTRACTS.map((c) => this.store.checkpoint(c.key)).filter((v) => v != null);
    if (cps.length === CONTRACTS.length) return Math.min(...cps) + 1;
    const start = CFG.startBlock ?? Math.max(0, head - CFG.confirmations - CFG.defaultLookback);
    log(`No checkpoint for every contract; starting at block ${start}${CFG.startBlock == null ? ` (head - ${CFG.defaultLookback}; set V2_START_BLOCK to change)` : ''}`);
    for (const c of CONTRACTS) if (this.store.checkpoint(c.key) == null) this.store.setCheckpoint(c.key, start - 1);
    return Math.min(...CONTRACTS.map((c) => this.store.checkpoint(c.key))) + 1;
  }

  async syncConfirmed() {
    const head = await this.rpc.blockNumber();
    const target = head - CFG.confirmations;
    const from = await this.startBlock(head);
    if (from > target) return { head, target, totals: null };
    const totals = await this.scan(from, target, { confirmed: true, advanceCheckpoint: true, label: 'sync' });
    return { head, target, totals };
  }

  // Without WebSocket, poll the unconfirmed tip so new requests show up quickly.
  async syncTip(head) {
    const from = head - CFG.confirmations + 1;
    if (from > head) return;
    await this.scan(Math.max(from, 0), head, { confirmed: false, advanceCheckpoint: false, label: 'tip' });
  }
}

// ───────────────────────────────────────────────────────────── whitelist snapshots

class WhitelistTracker {
  constructor(store, rpc) { this.store = store; this.rpc = rpc; this.lastDefault = 0; this.perRequestDisabled = false; }

  async snapshotDefault() {
    const store = this.store;
    let wlAddr = null;
    try {
      const block = await this.rpc.blockNumber();
      wlAddr = lc(MANAGED_FNS.decodeFunctionResult('defaultProposerWhitelist', await this.rpc.ethCall(MANAGED.address, MANAGED_FNS.encodeFunctionData('defaultProposerWhitelist')))[0]);
      const list = WHITELIST_FNS.decodeFunctionResult('getWhitelist', await this.rpc.ethCall(wlAddr, WHITELIST_FNS.encodeFunctionData('getWhitelist')))[0].map(lc).sort();
      const hash = sha(list.join(','));
      const prev = store.loadDefaultWhitelist();
      this.lastDefault = Date.now();
      if (prev && prev.hash === hash && prev.address === wlAddr) return { changed: false, address: wlAddr, count: list.length };
      store.q.insertWl.run(nowSec(), block, 'managed', 'default', null, wlAddr, 1, list.length, hash, JSON.stringify(list), null);
      store.loadDefaultWhitelist();
      log(`Default proposer whitelist ${short(wlAddr)}: ${list.length} addresses${prev ? ' (CHANGED)' : ''}`);
      return { changed: true, address: wlAddr, count: list.length };
    } catch (e) {
      this.lastDefault = Date.now();
      store.q.insertWl.run(nowSec(), null, 'managed', 'default', null, wlAddr, null, null, null, null, e.message.slice(0, 300));
      log(`Default whitelist snapshot failed: ${e.message.slice(0, 160)}`);
      return { error: e.message };
    }
  }

  // Reads the whitelist that actually applies to one managed request (custom or default) at the current block.
  async checkRequest(key) {
    if (this.perRequestDisabled) return null;
    const store = this.store;
    const req = store.db.prepare('SELECT requester, identifier, ancillary_hex, request_timestamp FROM requests WHERE request_key = ?').get(key);
    if (!req || !req.ancillary_hex || !req.requester) return null;
    const identifierHex = await this.identifierHex(key, req);
    if (!identifierHex) return null;
    try {
      const block = await this.rpc.blockNumber();
      const data = MANAGED_FNS.encodeFunctionData('getProposerWhitelistWithEnforcementStatus', [req.requester, identifierHex, req.ancillary_hex]);
      const [enforced, allowed] = MANAGED_FNS.decodeFunctionResult('getProposerWhitelistWithEnforcementStatus', await this.rpc.ethCall(MANAGED.address, data));
      const list = allowed.map(lc).sort();
      const hash = sha(list.join(','));
      const sameAsDefault = store.defaultWl && store.defaultWl.hash === hash;
      tx(store.db, () => {
        store.q.insertWl.run(nowSec(), block, 'managed', 'request', key, null, enforced ? 1 : 0, list.length, hash, sameAsDefault ? null : JSON.stringify(list), null);
        store.q.setRequestWl.run(enforced ? 1 : 0, list.length, nowSec(), key);
        store.rebuildRequest(key);
      });
      return { enforced, count: list.length, custom: !sameAsDefault };
    } catch (e) {
      if (/execution reverted|function selector|invalid opcode|could not decode|BAD_DATA/i.test(e.message)) {
        this.perRequestDisabled = true;
        log(`Per-request whitelist lookup not supported by this contract (${e.message.slice(0, 100)}); disabling for this run.`);
      } else log(`Whitelist check failed for ${short(key)}: ${e.message.slice(0, 120)}`);
      return null;
    }
  }

  // The bytes32 identifier is in the RequestPrice/ProposePrice event args.
  async identifierHex(key) {
    const row = this.store.db.prepare('SELECT args_json FROM oracle_events WHERE request_key = ? LIMIT 1').get(key);
    const id = row ? JSON.parse(row.args_json).identifier : null;
    return typeof id === 'string' && id.length === 66 ? id : null;
  }

  // Open managed requests (no settlement yet) that haven't been checked.
  async checkOpenRequests(limit) {
    if (this.perRequestDisabled) return 0;
    const rows = this.store.db.prepare(`SELECT request_key FROM requests WHERE contract = 'managed' AND status IN ('Requested','Proposed')
      AND whitelist_checked_at IS NULL AND ancillary_hex IS NOT NULL ORDER BY requested_block DESC LIMIT ?`).all(limit);
    let n = 0;
    for (const r of rows) { if (await this.checkRequest(r.request_key)) n++; if (this.perRequestDisabled) break; }
    return n;
  }
}

// ───────────────────────────────────────────────────────────── WebSocket fast path

class LogStream {
  constructor(url, onLog, store) { this.url = url; this.onLog = onLog; this.store = store; this.backoff = 1000; this.stopped = false; this.lastMsg = Date.now(); this.reqId = 1000; }
  async start() {
    this.WS = globalThis.WebSocket ?? (await import('ws')).default;
    this.connect();
    this.hb = setInterval(() => this.heartbeat(), 30000);
  }
  connect() {
    if (this.stopped) return;
    let ws;
    try { ws = new this.WS(this.url); } catch (e) { log(`WebSocket create failed: ${e.message}`); return this.reconnect(); }
    this.ws = ws; this.subscribed = false;
    ws.onopen = () => {
      this.lastMsg = Date.now();
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_subscribe', params: ['logs', { address: CONTRACTS.map((c) => c.address) }] }));
      this.store.recordMetric('ws:eth_subscribe', 0, true, null, cuFor('eth_subscribe'));
    };
    ws.onmessage = (ev) => {
      this.lastMsg = Date.now();
      let msg; try { msg = JSON.parse(typeof ev.data === 'string' ? ev.data : ev.data.toString()); } catch { return; }
      if (msg.id === 1) {
        if (msg.result) { this.subscribed = true; this.backoff = 1000; log('WebSocket subscribed to oracle logs'); }
        else if (msg.error) { log(`WebSocket subscribe error: ${msg.error.message}`); try { ws.close(); } catch {} }
        return;
      }
      if (msg.method === 'eth_subscription' && msg.params?.result) {
        this.store.recordMetric('ws:log', 0, true, null, 0);
        try { this.onLog(msg.params.result); } catch (e) { log(`WebSocket log handling failed: ${e.message}`); }
      }
    };
    ws.onerror = (e) => { this.store.recordMetric('ws:error', 0, false, e?.message ?? 'error', 0); };
    ws.onclose = () => { if (this.ws === ws) { this.subscribed = false; this.reconnect(); } };
  }
  reconnect() {
    if (this.stopped) return;
    const wait = this.backoff;
    this.backoff = Math.min(this.backoff * 2, 60000);
    log(`WebSocket disconnected; reconnecting in ${Math.round(wait / 1000)}s (polling continues meanwhile)`);
    setTimeout(() => this.connect(), wait);
  }
  heartbeat() {
    const ws = this.ws;
    if (!ws || ws.readyState !== 1) return;
    if (Date.now() - this.lastMsg > 120000) { log('WebSocket silent for 2 minutes; forcing reconnect'); try { ws.close(); } catch {} return; }
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: ++this.reqId, method: 'eth_blockNumber', params: [] }));
    this.store.recordMetric('ws:eth_blockNumber', 0, true, null, cuFor('eth_blockNumber'));
  }
  stop() { this.stopped = true; clearInterval(this.hb); try { this.ws?.close(); } catch {} }
}

// ───────────────────────────────────────────────────────────── legacy import

function findJsonl(targets) {
  const out = [];
  const walk = (p, depth) => {
    let st; try { st = fs.statSync(p); } catch { return; }
    if (st.isFile()) { if (/\.(jsonl|ndjson)$/i.test(p)) out.push(p); return; }
    if (!st.isDirectory() || depth > 6) return;
    const base = path.basename(p);
    if (['node_modules', '.git', 'v2'].includes(base) && depth > 0) return;
    for (const f of fs.readdirSync(p)) walk(path.join(p, f), depth + 1);
  };
  if (targets.length) for (const t of targets) walk(t, 0);
  else {
    for (const f of fs.readdirSync('.')) if (/\.(jsonl|ndjson)$/i.test(f)) out.push(f);
    for (const d of ['data', 'logs', 'output']) if (fs.existsSync(d)) walk(d, 0);
  }
  return [...new Set(out.map((f) => path.resolve(f)))].filter((f) => !f.startsWith(path.resolve(path.dirname(CFG.dbFile))));
}

function guessContractFromFile(file) {
  // file name first, then its folder (e.g. data/managed-live/2026-10-01.jsonl, data/public-live/...)
  for (const part of [path.basename(file), path.basename(path.dirname(file))].map((x) => x.toLowerCase())) {
    if (/managed|moo/.test(part)) return 'managed';
    if (/standard|std|public/.test(part)) return 'standard';
  }
  return 'unknown';
}

// Normalizes one legacy JSON line (raw log, ethers event, or flat object) into an event record.
function normalizeLegacy(obj, file, line) {
  const src = obj.log ?? obj.raw ?? obj.event?.log ?? obj;
  const pick = (...ks) => { for (const k of ks) { for (const o of [obj, src, obj.event]) if (o && o[k] !== undefined && o[k] !== null) return o[k]; } return undefined; };
  const txHash = lc(pick('transactionHash', 'txHash', 'tx_hash', 'hash', 'tx'));
  const logIndex = toInt(pick('logIndex', 'log_index', 'index'));
  const address = lc(pick('address', 'contractAddress', 'contract_address', 'oracle', 'oracleAddress'));
  const contract = BY_ADDRESS.get(address)?.key ?? (BY_KEY.has(lc(pick('contract', 'monitor', 'source'))) ? lc(pick('contract', 'monitor', 'source')) : guessContractFromFile(file));
  const base = {
    event_id: txHash && logIndex != null ? `${txHash}:${logIndex}` : `legacy:${sha(line)}`,
    contract, block_number: toInt(pick('blockNumber', 'block_number', 'block')), block_hash: lc(pick('blockHash', 'block_hash')) ?? null,
    tx_hash: txHash ?? null, log_index: logIndex,
  };
  const topics = src.topics ?? obj.topics;
  const data = src.data ?? obj.data;
  if (Array.isArray(topics) && typeof data === 'string') {
    try {
      const parsed = OO_EVENTS.parseLog({ topics, data });
      if (parsed) return fromArgs(base, parsed.name, argsToPlain(parsed.fragment, parsed.args));
    } catch { /* fall through to named args */ }
  }
  let name = pick('eventName', 'event', 'name', 'type', 'kind');
  if (name && typeof name === 'object') name = name.name ?? name.eventName;
  name = typeof name === 'string' ? name : null;
  const norm = { requestprice: 'RequestPrice', request: 'RequestPrice', proposeprice: 'ProposePrice', propose: 'ProposePrice', proposal: 'ProposePrice',
    disputeprice: 'DisputePrice', dispute: 'DisputePrice', settle: 'Settle', settlement: 'Settle', settled: 'Settle' }[(name ?? '').toLowerCase().replace(/[^a-z]/g, '')] ?? name;
  let args = obj.args ?? obj.decoded ?? obj.decodedArgs ?? obj.params ?? obj.values ?? src.args;
  if (Array.isArray(args) && LIFECYCLE.has(norm)) {
    const frag = OO_EVENTS.getEvent(norm);
    args = Object.fromEntries(frag.inputs.map((inp, i) => [inp.name, args[i]]));
  }
  if (!args || typeof args !== 'object') args = obj;
  const a = {};
  for (const [k, v] of Object.entries(args)) a[k] = typeof v === 'object' && v !== null && (v.hex || v._hex || v.type === 'BigNumber') ? toBigStr(v) : v;
  if (!norm) return null;
  if (!LIFECYCLE.has(norm)) return { ...base, event_name: norm, args: a };
  for (const k of ['requester', 'proposer', 'disputer', 'currency']) if (typeof a[k] === 'string') a[k] = a[k].toLowerCase();
  if (typeof a.identifier === 'string' && !a.identifier.startsWith('0x')) {
    a.identifier = '0x' + Buffer.from(a.identifier, 'utf8').toString('hex').padEnd(64, '0').slice(0, 64);
  }
  if (typeof a.ancillaryData === 'string' && !a.ancillaryData.startsWith('0x')) a.ancillaryData = '0x' + Buffer.from(a.ancillaryData, 'utf8').toString('hex');
  if (a.ancillaryData === undefined && typeof a.ancillary === 'string') a.ancillaryData = a.ancillary.startsWith('0x') ? a.ancillary : '0x' + Buffer.from(a.ancillary, 'utf8').toString('hex');
  return fromArgs(base, norm, a);
}

async function cmdImport(store, targets) {
  const files = findJsonl(targets);
  if (!files.length) {
    console.log('No .jsonl files found. Pass paths explicitly: node v2.mjs import path/to/history.jsonl path/to/live.jsonl');
    return;
  }
  console.log(`Importing into ${CFG.dbFile}\n`);
  const summary = [];
  const maxBlock = {};
  for (const file of files) {
    const st = fs.statSync(file);
    const text = fs.readFileSync(file, 'utf8');
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    const stats = { lines: lines.length, new: 0, dup: 0, skipped: 0, byContract: {} };
    const BATCH = 2000;
    for (let i = 0; i < lines.length; i += BATCH) {
      tx(store.db, () => {
        const keys = new Set();
        for (const line of lines.slice(i, i + BATCH)) {
          let obj; try { obj = JSON.parse(line); } catch { stats.skipped++; continue; }
          let ev; try { ev = normalizeLegacy(obj, file, line); } catch { ev = null; }
          if (!ev) { stats.skipped++; continue; }
          const res = store.putEvent(ev, true, `import:${path.basename(file)}`);
          if (res === 'dup') stats.dup++; else stats.new++;
          stats.byContract[ev.contract] = (stats.byContract[ev.contract] ?? 0) + 1;
          if (ev.request_key) keys.add(ev.request_key);
          if (ev.block_number != null && ev.contract !== 'unknown') maxBlock[ev.contract] = Math.max(maxBlock[ev.contract] ?? 0, ev.block_number);
        }
        for (const k of keys) store.rebuildRequest(k);
      });
    }
    store.db.prepare(`INSERT OR REPLACE INTO import_log (file, size, mtime, lines, inserted, duplicates, skipped, imported_at) VALUES (?,?,?,?,?,?,?,?)`)
      .run(file, st.size, Math.floor(st.mtimeMs / 1000), stats.lines, stats.new, stats.dup, stats.skipped, nowSec());
    summary.push({ file: path.relative('.', file), lines: stats.lines, inserted: stats.new, duplicates: stats.dup, skipped: stats.skipped,
      contracts: Object.entries(stats.byContract).map(([k, v]) => `${k}:${v}`).join(' ') });
  }
  console.table(summary);
  for (const [c, b] of Object.entries(maxBlock)) {
    if (store.checkpoint(c) == null) { store.setCheckpoint(c, b); console.log(`Checkpoint for ${c} set to imported max block ${b}.`); }
  }
  const unknown = store.db.prepare("SELECT COUNT(*) n FROM oracle_events WHERE contract = 'unknown'").get().n;
  const partial = store.db.prepare("SELECT COUNT(*) n FROM requests WHERE request_key LIKE 'partial:%'").get().n;
  if (unknown) console.log(`\nNote: ${unknown} events could not be assigned to a contract (no address field and no 'managed'/'standard' in the file name).`);
  if (partial) console.log(`Note: ${partial} requests came from rows without ancillaryData and can't be matched exactly to new events.`);
  console.log('\nOriginal files were not modified. Run "node v2.mjs report" to check the totals.');
}

// ───────────────────────────────────────────────────────────── commands

async function cmdSync(store, { live }) {
  const rpc = new RpcClient(CFG.httpUrl, store);
  const syncer = new Syncer(store, rpc);
  const wl = new WhitelistTracker(store, rpc);
  store.loadDefaultWhitelist();

  let stream = null;
  let attempts = null, lastAttempts = 0;
  if (live && env('ETHERSCAN_API_KEY', 'POLYGONSCAN_API_KEY')) {
    attempts = new AttemptTracker(store, new EtherscanClient(store), rpc);
    log(`Competition tracker on: Etherscan txlist every ${CFG.attemptsIntervalMin} min`);
  } else if (live) log('Competition tracker off (set ETHERSCAN_API_KEY to track failed proposal transactions).');
  const stop = () => { if (syncer.stopped) process.exit(1); syncer.stopped = true; stream?.stop(); log('Stopping after the current batch… (Ctrl+C again to force)'); };
  process.on('SIGINT', stop); process.on('SIGTERM', stop);

  log(`UMA Monitor V2 (read-only) — DB ${CFG.dbFile}`);
  log(`Limiter: ${CFG.cuPerSec} CU/s, ≥${CFG.minIntervalMs}ms between HTTP calls, getLogs chunk ${CFG.logChunk} blocks, ${CFG.confirmations} confirmations`);
  for (const c of CONTRACTS) log(`  ${c.name.padEnd(14)} ${c.address}  checkpoint ${store.checkpoint(c.key) ?? '(none)'}`);

  if (live) {
    if (!store.defaultWl || Date.now() / 1000 - (store.q.latestDefaultWl.get()?.taken_at ?? 0) > CFG.whitelistRefreshMin * 60) await wl.snapshotDefault();
    else wl.lastDefault = Date.now();
    if (CFG.wsUrl) {
      stream = new LogStream(CFG.wsUrl, (raw) => {
        const s = writeLogBatch(store, [raw], { confirmed: false, source: 'ws' });
        for (const k of s.newManagedRequests) syncer.pendingWl.add(k);
        if (s.new) {
          const ev = decodeLog(raw);
          log(`live ${ev.contract} ${ev.event_name} block ${ev.block_number}${ev.proposer ? ` proposer ${short(ev.proposer)}` : ''} (unconfirmed)`);
        }
      }, store);
      await stream.start();
    } else log('No WebSocket URL; polling the chain tip instead.');
  }

  do {
    try {
      const { head, target, totals } = await syncer.syncConfirmed();
      if (!live) { log(`Caught up to confirmed block ${target} (head ${head}).`, totals ? `${totals.new} new events, ${totals.calls} log queries.` : ''); break; }
      if (!stream?.subscribed && !syncer.stopped) await syncer.syncTip(head);
      if (Date.now() - wl.lastDefault > CFG.whitelistRefreshMin * 60000) {
        const r = await wl.snapshotDefault();
        if (r.changed) for (const row of store.db.prepare("SELECT request_key FROM requests WHERE contract='managed' AND status IN ('Requested','Proposed')").all()) tx(store.db, () => store.rebuildRequest(row.request_key));
      }
      let budget = CFG.whitelistChecksPerCycle;
      for (const k of [...syncer.pendingWl]) {
        if (budget-- <= 0 || syncer.stopped) break;
        syncer.pendingWl.delete(k);
        const r = await wl.checkRequest(k);
        if (r) log(`whitelist ${short(k.split(':')[1])}: ${r.enforced ? `enforced, ${r.count} proposers${r.custom ? ' (custom list)' : ''}` : 'NOT enforced (public)'}`);
      }
      if (budget > 0 && !syncer.stopped) await wl.checkOpenRequests(budget);
      if (attempts && !syncer.stopped && Date.now() - lastAttempts > CFG.attemptsIntervalMin * 60000) {
        lastAttempts = Date.now();
        try { await attempts.sync({ label: 'attempts' }); } catch (e) { log(`Attempt tracking error: ${e.message}`); }
      }
    } catch (e) {
      log(`Cycle error: ${e.message}`);
    }
    if (live && !syncer.stopped) {
      const until = Date.now() + CFG.pollMs;
      while (Date.now() < until && !syncer.stopped) await sleep(250);
    }
  } while (live && !syncer.stopped);

  stream?.stop();
  log('Stopped.');
}

async function cmdBackfill(store, args) {
  const from = Number(argVal(args, '--from'));
  if (!Number.isFinite(from)) throw new Error('Usage: node v2.mjs backfill --from <block> [--to <block>]');
  const rpc = new RpcClient(CFG.httpUrl, store);
  const syncer = new Syncer(store, rpc);
  process.on('SIGINT', () => { syncer.stopped = true; log('Stopping after the current batch…'); });
  const head = await rpc.blockNumber();
  const to = Math.min(Number(argVal(args, '--to') ?? head - CFG.confirmations), head - CFG.confirmations);
  log(`Backfilling ${from}-${to} (${to - from + 1} blocks, ~${Math.ceil((to - from + 1) / CFG.logChunk)} log queries)`);
  const t = await syncer.scan(from, to, { confirmed: true, advanceCheckpoint: false, label: 'backfill' });
  log(`Backfill done: ${t.new} new, ${t.dup} already stored, ${t.calls} queries.`);
}

async function cmdWhitelist(store, args) {
  const rpc = new RpcClient(CFG.httpUrl, store);
  const wl = new WhitelistTracker(store, rpc);
  const r = await wl.snapshotDefault();
  if (!r.error) console.log(`Default whitelist ${r.address}: ${r.count} addresses${r.changed ? ' (new snapshot saved)' : ' (unchanged)'}`);
  tx(store.db, () => { for (const row of store.db.prepare("SELECT request_key FROM requests WHERE contract='managed'").all()) store.rebuildRequest(row.request_key); });
  const n = await wl.checkOpenRequests(Number(argVal(args, '--limit') ?? 25));
  console.log(`Checked ${n} open managed requests.`);
}

function argVal(args, name) { const i = args.indexOf(name); return i >= 0 ? args[i + 1] : undefined; }

// ───────────────────────────────────────────────────────────── competition tracker (failed + winning proposal transactions)
//
// Event logs only show the winning proposal. Etherscan's txlist for each oracle address also returns
// reverted transactions, so every *direct* proposePrice call (winner or loser) can be matched to its request.
// Attempts routed through a bot's own contract are not visible this way; the report shows how many winning
// proposals arrived as direct calls ("direct-call coverage") so the blind spot is measured, not guessed.

const OO_FNS = new Interface([
  'function proposePrice(address requester, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 proposedPrice) returns (uint256)',
  'function proposePriceFor(address proposer, address requester, bytes32 identifier, uint256 timestamp, bytes ancillaryData, int256 proposedPrice) returns (uint256)',
  'function disputePrice(address requester, bytes32 identifier, uint256 timestamp, bytes ancillaryData) returns (uint256)',
  'function disputePriceFor(address disputer, address requester, bytes32 identifier, uint256 timestamp, bytes ancillaryData) returns (uint256)',
  'function multicall(bytes[] data) returns (bytes[])',
]);

const ATTEMPT_COLUMNS = {
  function_name: 'TEXT', kind: 'TEXT', proposer: 'TEXT', tx_index: 'INTEGER', block_time: 'INTEGER',
  gas_limit: 'TEXT', gas_price: 'TEXT', proposed_price: 'TEXT', classification: 'TEXT',
  winner_tx: 'TEXT', blocks_after_winner: 'INTEGER',
};

function migrateAttempts(db) {
  const have = new Set(db.prepare('PRAGMA table_info(transaction_attempts)').all().map((c) => c.name));
  for (const [col, type] of Object.entries(ATTEMPT_COLUMNS)) if (!have.has(col)) db.exec(`ALTER TABLE transaction_attempts ADD COLUMN ${col} ${type}`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_attempts_request ON transaction_attempts(request_key);
    CREATE INDEX IF NOT EXISTS idx_attempts_from ON transaction_attempts(from_address);`);
}

class EtherscanClient {
  constructor(store) {
    this.key = env('ETHERSCAN_API_KEY', 'POLYGONSCAN_API_KEY');
    if (!this.key) throw new Error('Set ETHERSCAN_API_KEY in .env (free key from etherscan.io; one key covers Polygon via the V2 API).');
    this.url = env('V2_ETHERSCAN_URL') ?? 'https://api.etherscan.io/v2/api';
    this.chainId = env('V2_CHAIN_ID') ?? '137';
    this.minGap = 1000 / num(env('V2_ETHERSCAN_RPS'), 4);
    this.last = 0; this.chain = Promise.resolve(); this.store = store;
  }
  call(params) {
    const p = this.chain.then(() => this.#exec(params));
    this.chain = p.catch(() => {});
    return p;
  }
  async #exec(params) {
    const qs = new URLSearchParams({ chainid: this.chainId, ...params, apikey: this.key });
    const method = `etherscan:${params.action}`;
    for (let attempt = 0; attempt < 6; attempt++) {
      const wait = this.last + this.minGap - Date.now();
      if (wait > 0) await sleep(wait);
      this.last = Date.now();
      const t0 = performance.now();
      try {
        const res = await fetch(`${this.url}?${qs}`, { signal: AbortSignal.timeout(30000) });
        const body = await res.json();
        const ms = performance.now() - t0;
        if (body.status === '1') { this.store.recordMetric(method, ms, true, null, 0); return body.result; }
        if (/no transactions found|no records found/i.test(body.message ?? '') || (Array.isArray(body.result) && body.result.length === 0)) { this.store.recordMetric(method, ms, true, null, 0); return []; }
        const msg = typeof body.result === 'string' ? body.result : body.message;
        this.store.recordMetric(method, ms, false, msg, 0);
        if (/rate limit/i.test(msg ?? '')) { await sleep(1000 * 2 ** attempt); continue; }
        throw new Error(`Etherscan: ${msg}`);
      } catch (e) {
        if (e.message.startsWith('Etherscan:')) throw e;
        this.store.recordMetric(method, performance.now() - t0, false, e.message, 0);
        if (attempt === 5) throw e;
        await sleep(1000 * 2 ** attempt);
      }
    }
    throw new Error('Etherscan: rate limited too many times');
  }
  // All transactions *to* an address in [from, to], ascending. Pages by advancing startblock (the API caps page*offset at 10,000).
  async *txlist(address, from, to) {
    const PAGE = 1000;
    let start = from;
    const seen = new Set();
    for (;;) {
      const rows = await this.call({ module: 'account', action: 'txlist', address, startblock: String(start), endblock: String(to), page: '1', offset: String(PAGE), sort: 'asc' });
      const fresh = rows.filter((r) => !seen.has(r.hash));
      for (const r of fresh) seen.add(r.hash);
      if (fresh.length) yield fresh;
      if (rows.length < PAGE) return;
      const lastBlock = Number(rows.at(-1).blockNumber);
      if (lastBlock === start && !fresh.length) { log(`Etherscan: more than ${PAGE} txs in block ${start}; some may be skipped`); start++; }
      else start = lastBlock; // re-read the last block (dedupe by hash) so nothing in it is lost
      if (seen.size > 50000) seen.clear();
    }
  }
}

// Decodes a direct oracle call into zero or more attempt records (multicall can bundle several).
function decodeAttemptCalls(contractKey, input) {
  let parsed;
  try { parsed = OO_FNS.parseTransaction({ data: input }); } catch { return []; }
  if (!parsed) return [];
  if (parsed.name === 'multicall') return parsed.args[0].flatMap((d) => decodeAttemptCalls(contractKey, d));
  const a = argsToPlain(parsed.fragment, parsed.args);
  const kind = parsed.name.startsWith('propose') ? 'propose' : 'dispute';
  const requestId = requestIdOf(a.requester, a.identifier, a.timestamp, a.ancillaryData);
  return [{
    function_name: parsed.name, kind, request_key: `${contractKey}:${requestId}`,
    proposer: a.proposer ?? null, proposed_price: a.proposedPrice != null ? String(a.proposedPrice) : null,
    requester: a.requester, identifier: a.identifier, timestamp: a.timestamp, ancillary: a.ancillaryData,
  }];
}

class AttemptTracker {
  constructor(store, scan, rpc) {
    this.store = store; this.scan = scan; this.rpc = rpc;
    migrateAttempts(store.db);
    const db = store.db;
    this.q = {
      upsert: db.prepare(`INSERT INTO transaction_attempts (tx_hash, request_key, contract, from_address, block_number, status, gas_used, revert_reason, detected_at, source,
          function_name, kind, proposer, tx_index, block_time, gas_limit, gas_price, proposed_price)
        VALUES (?,?,?,?,?,?,?,NULL,?,'etherscan',?,?,?,?,?,?,?,?)
        ON CONFLICT(tx_hash) DO UPDATE SET status = excluded.status, gas_used = excluded.gas_used, block_number = excluded.block_number, tx_index = excluded.tx_index`),
      forRequest: db.prepare('SELECT * FROM transaction_attempts WHERE request_key = ? ORDER BY block_number, tx_index'),
      winner: db.prepare('SELECT event_id, tx_hash, block_number FROM proposals WHERE request_key = ? ORDER BY block_number LIMIT 1'),
      setClass: db.prepare('UPDATE transaction_attempts SET classification = ?, winner_tx = ?, blocks_after_winner = ? WHERE tx_hash = ?'),
      setReason: db.prepare('UPDATE transaction_attempts SET revert_reason = ? WHERE tx_hash = ?'),
    };
  }

  // Ingest one page of Etherscan rows for a contract.
  ingest(contract, rows) {
    return tx(this.store.db, () => {
      const keys = new Set();
      let n = 0;
      for (const r of rows) {
        if (lc(r.to) !== contract.address) continue;
        const calls = decodeAttemptCalls(contract.key, r.input);
        if (!calls.length) continue;
        const status = r.isError === '1' || r.txreceipt_status === '0' ? 'reverted' : 'success';
        // A multicall with several proposals shares one tx hash; store the first, it is the one that decides the race.
        const c = calls[0];
        this.q.upsert.run(lc(r.hash), c.request_key, contract.key, lc(r.from), Number(r.blockNumber), status, r.gasUsed ?? null, nowSec(),
          c.function_name, c.kind, c.proposer ? lc(c.proposer) : lc(r.from), Number(r.transactionIndex), Number(r.timeStamp), r.gas ?? null, r.gasPrice ?? null, c.proposed_price);
        keys.add(c.request_key); n++;
      }
      for (const k of keys) this.classify(k);
      return { attempts: n, requests: keys.size };
    });
  }

  // Labels each attempt on a request relative to the accepted proposal (from the ProposePrice event).
  classify(key) {
    const attempts = this.q.forRequest.all(key);
    const win = this.q.winner.get(key);
    const winTx = win?.tx_hash ?? attempts.find((a) => a.kind === 'propose' && a.status === 'success')?.tx_hash ?? null;
    const winAttempt = attempts.find((a) => a.tx_hash === winTx);
    const winBlock = win?.block_number ?? winAttempt?.block_number ?? null;
    const req = this.store.db.prepare('SELECT contract, status FROM requests WHERE request_key = ?').get(key);
    const wl = req?.contract === 'managed' ? (this.store.q.latestRequestWl.get(key) ?? null) : null;
    const wlSet = wl ? (wl.enforced ? new Set(wl.addresses_json ? JSON.parse(wl.addresses_json) : [...(this.store.defaultWl?.addresses ?? [])]) : null)
      : req?.contract === 'managed' && this.store.defaultWl ? this.store.defaultWl.addresses : null;
    for (const a of attempts) {
      let cls;
      const after = winBlock != null && a.block_number != null ? a.block_number - winBlock : null;
      if (a.kind === 'dispute') cls = a.status === 'success' ? 'dispute' : 'dispute_failed';
      else if (a.tx_hash === winTx) cls = 'won';
      else if (a.status === 'success') cls = 'success_unmatched'; // success without a matching event (e.g. not yet synced)
      else if (a.gas_used && a.gas_limit && Number(a.gas_used) >= 0.97 * Number(a.gas_limit)) cls = 'out_of_gas';
      else if (winBlock != null && after > 0) cls = 'lost_late';
      else if (winBlock != null && after === 0) cls = winAttempt?.tx_index != null && a.tx_index < winAttempt.tx_index ? 'failed_before_winner' : 'lost_same_block';
      else if (wlSet && !wlSet.has(a.proposer)) cls = 'not_whitelisted';
      else if (!req) cls = 'unknown_request';
      else cls = 'failed_other';
      this.q.setClass.run(cls, winTx, after, a.tx_hash);
    }
  }

  async sync({ from, to, label = 'attempts' } = {}) {
    const head = await this.rpc.blockNumber();
    const end = Math.min(to ?? head - CFG.confirmations, head - CFG.confirmations);
    const total = { attempts: 0, requests: 0 };
    for (const c of CONTRACTS) {
      const cpKey = `attempts:${c.key}`;
      let start = from;
      if (start == null) {
        const cp = this.store.checkpoint(cpKey);
        start = cp != null ? cp + 1
          : this.store.db.prepare('SELECT MIN(block_number) AS b FROM oracle_events WHERE contract = ? AND block_number IS NOT NULL').get(c.key).b
            ?? CFG.startBlock ?? head - CFG.defaultLookback;
      }
      if (start > end) continue;
      log(`${label} ${c.name}: blocks ${start}-${end} via Etherscan`);
      for await (const rows of this.scan.txlist(c.address, start, end)) {
        const s = this.ingest(c, rows);
        total.attempts += s.attempts; total.requests += s.requests;
        if (s.attempts) log(`${label} ${c.key}: +${s.attempts} proposal/dispute txs up to block ${rows.at(-1).blockNumber}`);
      }
      if (from == null || to == null) this.store.setCheckpoint(cpKey, end);
    }
    return total;
  }

  // Re-classify everything (e.g. after new ProposePrice events or whitelist snapshots arrive).
  reclassifyAll() {
    const keys = this.store.db.prepare('SELECT DISTINCT request_key FROM transaction_attempts').all();
    tx(this.store.db, () => { for (const r of keys) this.classify(r.request_key); });
    return keys.length;
  }

  // Optional: replay failed attempts with eth_call at the previous block to read the revert reason.
  // If the replay succeeds, the call was valid before the block, so it most likely lost to a tx in the same block.
  async reasons(limit) {
    const rows = this.store.db.prepare(`SELECT a.tx_hash, a.from_address, a.block_number, a.contract FROM transaction_attempts a
      WHERE a.status = 'reverted' AND a.revert_reason IS NULL AND a.classification IN ('failed_other','lost_same_block','failed_before_winner','not_whitelisted','unknown_request')
      ORDER BY a.block_number DESC LIMIT ?`).all(limit);
    let n = 0;
    for (const r of rows) {
      const t = await this.rpc.call('eth_getTransactionByHash', [r.tx_hash]);
      if (!t) continue;
      let reason;
      try {
        await this.rpc.call('eth_call', [{ from: r.from_address, to: t.to, data: t.input, gas: t.gas }, '0x' + (r.block_number - 1).toString(16)]);
        reason = 'replay OK at previous block (lost to a same-block tx)';
      } catch (e) { reason = e.message.replace(/^execution reverted:?\s*/i, '').slice(0, 200) || 'reverted (no reason)'; }
      this.q.setReason.run(reason, r.tx_hash); n++;
    }
    return n;
  }
}

async function cmdAttempts(store, args) {
  const rpc = new RpcClient(CFG.httpUrl, store);
  const tracker = new AttemptTracker(store, new EtherscanClient(store), rpc);
  const from = argVal(args, '--from'), to = argVal(args, '--to');
  const t = await tracker.sync({ from: from != null ? Number(from) : undefined, to: to != null ? Number(to) : undefined });
  log(`Done: ${t.attempts} proposal/dispute transactions across ${t.requests} requests.`);
  log(`Reclassified ${tracker.reclassifyAll()} requests.`);
  const r = argVal(args, '--reasons');
  if (r) log(`Read revert reasons for ${await tracker.reasons(Number(r))} failed attempts.`);
}

function competitionAnalysis(db) {
  migrateAttempts(db);
  const all = (sql, ...p) => db.prepare(sql).all(...p);
  const get = (sql, ...p) => db.prepare(sql).get(...p);
  // wallets = distinct proposers seen in direct txs, plus the accepted proposer when it arrived via a contract (not in txlist)
  const perRequest = `SELECT a.request_key, r.contract, r.title, COUNT(*) AS txs,
      COUNT(DISTINCT a.proposer) + (SELECT COUNT(*) FROM proposals p WHERE p.request_key = a.request_key AND p.block_number = (SELECT MIN(block_number) FROM proposals p2 WHERE p2.request_key = a.request_key)
        AND NOT EXISTS (SELECT 1 FROM transaction_attempts x WHERE x.request_key = a.request_key AND x.proposer = p.proposer)) AS wallets,
      SUM(a.status = 'reverted') AS failed, SUM(a.classification = 'lost_same_block') AS same_block,
      MIN(a.block_number) AS first_block, MAX(a.classification = 'won') AS has_winner
    FROM transaction_attempts a LEFT JOIN requests r ON r.request_key = a.request_key
    WHERE a.kind = 'propose' GROUP BY a.request_key`;
  return {
    summary: () => {
      const s = get(`SELECT COUNT(*) AS requests, SUM(wallets) AS wallet_slots, ROUND(AVG(wallets), 2) AS avg_wallets, MAX(wallets) AS max_wallets,
          SUM(wallets > 1) AS contested, SUM(failed) AS failed_txs, SUM(same_block > 0) AS same_block_races FROM (${perRequest})`);
      const cov = get(`SELECT COUNT(*) AS proposals, SUM(EXISTS (SELECT 1 FROM transaction_attempts a WHERE a.tx_hash = p.tx_hash)) AS direct
        FROM proposals p WHERE p.block_number >= COALESCE((SELECT MIN(block_number) FROM transaction_attempts), 1e18)`);
      const classes = all(`SELECT classification, COUNT(*) AS txs, COUNT(DISTINCT proposer) AS wallets FROM transaction_attempts GROUP BY classification ORDER BY txs DESC`);
      const ws = all(`SELECT wallets, COUNT(*) AS requests FROM (${perRequest}) GROUP BY wallets ORDER BY wallets`);
      const median = (() => { const v = ws.flatMap((r) => Array(r.requests).fill(r.wallets)); return v.length ? v[Math.floor((v.length - 1) / 2)] : null; })();
      const buckets = [['1', 1, 1], ['2', 2, 2], ['3-5', 3, 5], ['6-10', 6, 10], ['11-20', 11, 20], ['21+', 21, 1e9]]
        .map(([b, lo, hi]) => ({ wallets_per_request: b, requests: ws.filter((r) => r.wallets >= lo && r.wallets <= hi).reduce((x, r) => x + r.requests, 0) }));
      return { ...s, median_wallets: median, coverage: cov, classes, buckets };
    },
    wallets: (limit = 50) => all(`SELECT a.proposer AS wallet, COALESCE(l.label, '') AS label, COUNT(*) AS attempts, COUNT(DISTINCT a.request_key) AS requests,
        SUM(a.classification = 'won') AS won, SUM(a.classification IN ('lost_same_block','lost_late')) AS lost_race,
        SUM(a.classification = 'lost_same_block') AS lost_same_block, SUM(a.classification = 'not_whitelisted') AS not_whitelisted,
        SUM(a.classification IN ('failed_other','out_of_gas','failed_before_winner','unknown_request')) AS other_failed,
        ROUND(100.0 * SUM(a.classification = 'won') / COUNT(DISTINCT a.request_key), 1) AS win_pct,
        ROUND(AVG(CAST(a.gas_price AS REAL)) / 1e9, 1) AS avg_gwei, COUNT(DISTINCT a.from_address) AS senders, MAX(a.block_number) AS last_block
      FROM transaction_attempts a LEFT JOIN wallet_labels l ON l.address = a.proposer
      WHERE a.kind = 'propose' GROUP BY a.proposer ORDER BY attempts DESC LIMIT ?`, limit),
    contested: (limit = 25) => all(`SELECT * FROM (${perRequest}) WHERE wallets > 1 ORDER BY wallets DESC, first_block DESC LIMIT ?`, limit),
    gas: () => get(`SELECT ROUND(AVG(CASE WHEN w.gas_price IS NOT NULL THEN CAST(w.gas_price AS REAL) / CAST(l.gas_price AS REAL) END), 2) AS winner_vs_loser_gas_ratio,
        SUM(CAST(w.gas_price AS REAL) > CAST(l.gas_price AS REAL)) AS winner_paid_more, SUM(CAST(w.gas_price AS REAL) <= CAST(l.gas_price AS REAL)) AS winner_paid_same_or_less
      FROM transaction_attempts l JOIN transaction_attempts w ON w.tx_hash = l.winner_tx
      WHERE l.classification = 'lost_same_block'`),
  };
}

function printCompetition(db) {
  const c = competitionAnalysis(db);
  const s = c.summary();
  console.log('\n=== Competition (direct proposePrice transactions, including reverted) ===');
  if (!s.requests) { console.log('No attempt data yet. Run: node v2.mjs attempts   (needs ETHERSCAN_API_KEY)'); return; }
  console.log(`Requests with attempts: ${s.requests} | contested (2+ wallets): ${s.contested} | wallets per request: median ${s.median_wallets}, avg ${s.avg_wallets}, max ${s.max_wallets}`);
  console.log(`Failed proposal txs: ${s.failed_txs} | requests with a same-block race: ${s.same_block_races}`);
  const cov = s.coverage;
  if (cov?.proposals) console.log(`Direct-call coverage: ${cov.direct}/${cov.proposals} accepted proposals (${Math.round((100 * cov.direct) / cov.proposals)}%) were direct oracle calls. Bots using their own contracts are invisible here, so counts are a LOWER BOUND.`);
  console.table(s.buckets);
  console.table(s.classes);
  console.log('Wallets by attempts:');
  console.table(c.wallets(25));
  console.log('Most contested requests:');
  console.table(c.contested(10).map((r) => ({ contract: r.contract, block: r.first_block, wallets: r.wallets, txs: r.txs, failed: r.failed, title: (r.title ?? r.request_key).slice(0, 60) })));
  const g = c.gas();
  if (g?.winner_vs_loser_gas_ratio != null) console.log(`Same-block races: winner/loser gas price ratio ${g.winner_vs_loser_gas_ratio}; winner paid more in ${g.winner_paid_more}, same or less in ${g.winner_paid_same_or_less}.`);
}

// ───────────────────────────────────────────────────────────── analysis queries (report + dashboard)

const fmtPrice = (p) => {
  if (p == null) return '';
  try {
    const b = BigInt(p);
    if (b === 10n ** 18n) return '1 (YES/p2)';
    if (b === 0n) return '0 (NO/p1)';
    if (b === 5n * 10n ** 17n) return '0.5 (50-50/p3)';
    if (b === -(2n ** 255n) || b < -(10n ** 30n)) return 'TOO_EARLY';
    return (Number(b) / 1e18).toString();
  } catch { return p; }
};
const fmtToken = (v, dec = 6) => {
  if (v == null) return '';
  try { return (Number(BigInt(v)) / 10 ** dec).toLocaleString(undefined, { maximumFractionDigits: 2 }); } catch { return String(v); }
};

function analysis(db) {
  const all = (sql, ...p) => db.prepare(sql).all(...p);
  const get = (sql, ...p) => db.prepare(sql).get(...p);
  const now = nowSec();
  return {
    events: () => all(`SELECT contract, event_name, COUNT(*) AS events, SUM(confirmed = 0) AS unconfirmed, MIN(block_number) AS first_block, MAX(block_number) AS last_block
      FROM oracle_events GROUP BY contract, event_name ORDER BY contract, events DESC`),
    statuses: () => all(`SELECT contract,
        CASE WHEN status = 'Proposed' AND expiration_timestamp < ${now} THEN 'Proposed (liveness over)' ELSE status END AS status, COUNT(*) AS requests
      FROM requests GROUP BY 1, 2 ORDER BY 1, 3 DESC`),
    proposers: (limit = 25) => all(`SELECT p.proposer, COALESCE(l.label, '') AS label,
        COUNT(*) AS proposals, COUNT(DISTINCT p.request_key) AS requests,
        SUM(p.contract = 'standard') AS standard, SUM(p.contract = 'managed') AS managed,
        SUM(p.outcome = 'settled_accepted') AS accepted, SUM(p.outcome = 'disputed_won') AS disputes_won,
        SUM(p.outcome = 'disputed_lost') AS disputes_lost, SUM(p.outcome IN ('pending','disputed_pending')) AS open,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM proposals), 1) AS share_pct,
        ROUND(AVG(p.latency_blocks), 0) AS avg_latency_blocks,
        MAX(p.proposer_on_whitelist) AS on_whitelist, MAX(p.block_number) AS last_block
      FROM proposals p LEFT JOIN wallet_labels l ON l.address = p.proposer
      GROUP BY p.proposer ORDER BY proposals DESC LIMIT ?`, limit),
    disputers: (limit = 10) => all(`SELECT e.disputer, COALESCE(l.label, '') AS label, COUNT(*) AS disputes, COUNT(DISTINCT e.proposer) AS proposers_disputed, MAX(e.block_number) AS last_block
      FROM oracle_events e LEFT JOIN wallet_labels l ON l.address = e.disputer
      WHERE e.event_name = 'DisputePrice' GROUP BY e.disputer ORDER BY disputes DESC LIMIT ?`, limit),
    opportunities: (limit = 25) => all(`SELECT r.contract, r.request_key, r.title, r.identifier, r.reward, r.final_fee, r.requested_block,
        CASE WHEN r.contract = 'standard' THEN 'public' WHEN r.whitelist_enforced = 0 THEN 'public' WHEN r.whitelist_enforced = 1 THEN 'whitelisted (' || r.whitelist_size || ')' ELSE 'unchecked' END AS access,
        COALESCE(l.label, r.requester) AS requester
      FROM requests r LEFT JOIN wallet_labels l ON l.address = r.requester
      WHERE r.status = 'Requested' ORDER BY (r.contract = 'standard' OR r.whitelist_enforced = 0) DESC, r.requested_block DESC LIMIT ?`, limit),
    recentProposals: (limit = 25) => all(`SELECT p.block_number, p.contract, COALESCE(l.label, p.proposer) AS proposer, p.proposed_price, p.outcome, p.latency_blocks,
        p.proposer_on_whitelist, p.whitelist_basis, r.title
      FROM proposals p LEFT JOIN requests r ON r.request_key = p.request_key LEFT JOIN wallet_labels l ON l.address = p.proposer
      ORDER BY p.block_number DESC LIMIT ?`, limit),
    whitelist: () => {
      const d = get("SELECT * FROM whitelist_snapshots WHERE scope = 'default' AND error IS NULL ORDER BY id DESC LIMIT 1");
      const err = get("SELECT taken_at, error FROM whitelist_snapshots WHERE scope = 'default' ORDER BY id DESC LIMIT 1");
      const members = d ? JSON.parse(d.addresses_json).map((a) => {
        const s = get(`SELECT COUNT(*) AS proposals, MAX(block_number) AS last_block FROM proposals WHERE proposer = ?`, a);
        return { address: a, label: get('SELECT label FROM wallet_labels WHERE address = ?', a)?.label ?? '', proposals: s.proposals, last_block: s.last_block };
      }).sort((x, y) => y.proposals - x.proposals) : [];
      const perRequest = get(`SELECT COUNT(*) AS checked, SUM(enforced = 1) AS enforced, SUM(enforced = 0) AS public, SUM(addresses_json IS NOT NULL) AS custom
        FROM (SELECT request_key, enforced, addresses_json FROM whitelist_snapshots WHERE scope = 'request' AND error IS NULL GROUP BY request_key HAVING id = MAX(id))`);
      const outsiders = all(`SELECT proposer, COUNT(*) AS proposals FROM proposals WHERE contract = 'managed' AND proposer_on_whitelist = 0 GROUP BY proposer ORDER BY proposals DESC LIMIT 10`);
      return { snapshot: d ? { address: d.whitelist_address, count: d.address_count, taken_at: d.taken_at, block: d.block_number } : null, lastError: err?.error ? err : null, members, perRequest, outsiders };
    },
    checkpoints: () => all('SELECT contract, last_block, updated_at FROM sync_checkpoints ORDER BY contract'),
    rpc: (minutes = 60) => all(`SELECT method, SUM(calls) AS calls, SUM(errors) AS errors, ROUND(1.0 * SUM(total_ms) / SUM(calls), 0) AS avg_ms, MAX(max_ms) AS max_ms,
        SUM(est_cu) AS est_cu, ROUND(SUM(est_cu) / (${minutes} * 60.0), 1) AS avg_cu_per_s, MAX(last_error) AS last_error
      FROM rpc_metrics WHERE minute >= ? GROUP BY method ORDER BY est_cu DESC`, now - minutes * 60),
    attempts: () => get('SELECT COUNT(*) AS n FROM transaction_attempts').n,
    labels: () => get('SELECT COUNT(*) AS n FROM wallet_labels').n,
  };
}

function cmdReport(store) {
  const a = analysis(store.db);
  const h = (t) => console.log(`\n=== ${t} ===`);
  console.log(`UMA Monitor V2 report — ${CFG.dbFile} — ${ts()}`);

  h('Events');
  const ev = a.events();
  ev.length ? console.table(ev) : console.log('(no events yet — run "node v2.mjs import" or "node v2.mjs live")');

  h('Requests by status');
  console.table(a.statuses());

  h('Most active proposer wallets');
  console.table(a.proposers(20).map((r) => ({ ...r, proposer: r.proposer, on_whitelist: r.on_whitelist == null ? (r.managed ? '?' : 'n/a') : r.on_whitelist ? 'yes' : 'no' })));

  h('Disputers');
  console.table(a.disputers());

  h('Open requests with no proposal yet (opportunities)');
  console.table(a.opportunities(15).map((r) => ({ contract: r.contract, block: r.requested_block, access: r.access, reward: fmtToken(r.reward), title: (r.title ?? r.identifier ?? '').slice(0, 70) })));

  h('Whitelist');
  const w = a.whitelist();
  if (w.snapshot) {
    console.log(`Default proposer whitelist ${w.snapshot.address}: ${w.snapshot.count} addresses (snapshot ${new Date(w.snapshot.taken_at * 1000).toISOString()})`);
    console.table(w.members.slice(0, 30));
  } else console.log('No whitelist snapshot yet (runs automatically in live mode, or: node v2.mjs whitelist).');
  if (w.lastError) console.log(`Last snapshot error: ${w.lastError.error}`);
  console.log(`Per-request checks: ${w.perRequest?.checked ?? 0} checked, ${w.perRequest?.enforced ?? 0} enforced, ${w.perRequest?.public ?? 0} public, ${w.perRequest?.custom ?? 0} custom lists`);
  if (w.outsiders.length) { console.log('Managed-oracle proposers NOT on the whitelist used for comparison:'); console.table(w.outsiders); }
  console.log(`Wallet labels stored: ${a.labels()}`);

  h('Scan checkpoints');
  console.table(a.checkpoints().map((c) => ({ ...c, updated_at: c.updated_at ? new Date(c.updated_at * 1000).toISOString() : '' })));

  h('RPC usage (last 60 minutes, estimated CUs)');
  const rpc = a.rpc(60);
  rpc.length ? console.table(rpc) : console.log('(no RPC calls recorded in the last hour)');
  printCompetition(store.db);
}

// ───────────────────────────────────────────────────────────── dashboard

function cmdDashboard(store, args) {
  const port = Number(argVal(args, '--port') ?? CFG.dashboardPort);
  const a = analysis(store.db);
  const routes = {
    '/api/summary': () => ({ events: a.events(), statuses: a.statuses(), checkpoints: a.checkpoints(), attempts: a.attempts(), generated_at: nowSec() }),
    '/api/proposers': () => a.proposers(100),
    '/api/disputers': () => a.disputers(25),
    '/api/opportunities': () => a.opportunities(100).map((r) => ({ ...r, reward: fmtToken(r.reward), final_fee: fmtToken(r.final_fee) })),
    '/api/proposals': () => a.recentProposals(100).map((r) => ({ ...r, proposed_price: fmtPrice(r.proposed_price) })),
    '/api/whitelist': () => a.whitelist(),
    '/api/rpc': () => a.rpc(60),
    '/api/competition': () => { const c = competitionAnalysis(store.db); return { summary: c.summary(), wallets: c.wallets(100), contested: c.contested(50), gas: c.gas() }; },
  };
  http.createServer((req, res) => {
    const url = new URL(req.url, 'http://x');
    try {
      if (routes[url.pathname]) { res.writeHead(200, { 'content-type': 'application/json' }); return res.end(JSON.stringify(routes[url.pathname](), (k, v) => (typeof v === 'bigint' ? v.toString() : v))); }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); res.end(DASHBOARD_HTML);
    } catch (e) { res.writeHead(500); res.end(e.message); }
  }).listen(port, '127.0.0.1', () => console.log(`Dashboard: http://127.0.0.1:${port}  (read-only; Ctrl+C to stop)`));
}

const DASHBOARD_HTML = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>UMA Competition Monitor</title>
<style>
:root{--bg:#f7f7f5;--card:#fff;--ink:#1d1d1b;--mute:#6b6b66;--line:#e4e4df;--acc:#2f5bd3;--ok:#1d7a46;--bad:#b4362c}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--card:#1e1e1c;--ink:#ecebe6;--mute:#9a9992;--line:#33332f;--acc:#7d9bff;--ok:#5cc98a;--bad:#ff7d70}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,sans-serif}
header{padding:16px 20px;border-bottom:1px solid var(--line);display:flex;gap:12px;align-items:baseline;flex-wrap:wrap}
h1{font-size:18px;margin:0}small{color:var(--mute)}nav{display:flex;gap:6px;flex-wrap:wrap;padding:12px 20px}
nav button{border:1px solid var(--line);background:var(--card);color:var(--ink);padding:6px 12px;border-radius:6px;cursor:pointer}
nav button.on{border-color:var(--acc);color:var(--acc)}main{padding:0 20px 40px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin:10px 0 18px}
.tile{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px 12px}.tile b{display:block;font-size:22px}
.wrap{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:8px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{color:var(--mute);font-weight:600;font-size:12px}td.t{white-space:normal;min-width:260px}.yes{color:var(--ok)}.no{color:var(--bad)}code{font-size:12px}
h2{font-size:15px;margin:18px 0 8px}
</style></head><body>
<header><h1>UMA Oracle Competition Monitor</h1><small id="gen">loading…</small><small>read-only · Polygon · Standard + Managed OOV2</small></header>
<nav id="tabs"></nav><main id="view"></main>
<script>
const tabs={overview:'Overview',opportunities:'Public opportunities',proposers:'Wallet rankings',proposals:'Accepted proposals',competition:'Competition',whitelist:'Whitelist',rpc:'RPC usage'};
let cur=new URLSearchParams(location.hash.slice(1)).get('t')||'overview';
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const addr=a=>a&&/^0x[0-9a-f]{40}$/i.test(a)?'<code title="'+a+'">'+a.slice(0,6)+'…'+a.slice(-4)+'</code>':esc(a);
const yn=v=>v==null?'?':v?'<span class="yes">yes</span>':'<span class="no">no</span>';
function table(rows,cols){if(!rows||!rows.length)return '<p><small>No data yet.</small></p>';cols=cols||Object.keys(rows[0]).map(k=>[k,k]);
 return '<div class="wrap"><table><tr>'+cols.map(c=>'<th>'+esc(c[1])+'</th>').join('')+'</tr>'+rows.map(r=>'<tr>'+cols.map(c=>{const v=r[c[0]];const f=c[2];return '<td'+(c[3]?' class="t"':'')+'>'+(f?f(v,r):esc(v))+'</td>'}).join('')+'</tr>').join('')+'</table></div>'}
const j=p=>fetch(p).then(r=>r.json());
async function render(){
 document.getElementById('tabs').innerHTML=Object.entries(tabs).map(([k,v])=>'<button class="'+(k===cur?'on':'')+'" onclick="go(\\''+k+'\\')">'+v+'</button>').join('');
 const v=document.getElementById('view');let h='';
 if(cur==='overview'){const s=await j('/api/summary');const tot=(n)=>s.events.filter(e=>e.event_name===n).reduce((a,e)=>a+e.events,0);
  h+='<div class="tiles">'+[['Requests',tot('RequestPrice')],['Proposals',tot('ProposePrice')],['Disputes',tot('DisputePrice')],['Settlements',tot('Settle')],['Unconfirmed',s.events.reduce((a,e)=>a+(e.unconfirmed||0),0)]].map(t=>'<div class="tile"><small>'+t[0]+'</small><b>'+t[1].toLocaleString()+'</b></div>').join('')+'</div>';
  h+='<h2>Requests by status</h2>'+table(s.statuses)+'<h2>Events</h2>'+table(s.events)+'<h2>Checkpoints</h2>'+table(s.checkpoints.map(c=>({...c,updated_at:new Date(c.updated_at*1000).toLocaleString()})));
  document.getElementById('gen').textContent='updated '+new Date(s.generated_at*1000).toLocaleTimeString();}
 if(cur==='opportunities')h+='<h2>Open requests with no proposal yet</h2>'+table(await j('/api/opportunities'),[['contract','Oracle'],['access','Access'],['requested_block','Block'],['reward','Reward'],['final_fee','Final fee'],['requester','Requester',addr],['title','Market',(v,r)=>esc(v||r.identifier),1]]);
 if(cur==='proposers')h+='<h2>Accepted proposers (by on-chain ProposePrice events)</h2>'+table(await j('/api/proposers'),[['proposer','Wallet',addr],['label','Label'],['proposals','Proposals'],['share_pct','Share %'],['requests','Requests'],['standard','Standard'],['managed','Managed'],['accepted','Settled OK'],['disputes_won','Disp. won'],['disputes_lost','Disp. lost'],['open','Open'],['avg_latency_blocks','Avg latency (blocks)'],['on_whitelist','Whitelisted',yn],['last_block','Last block']])+'<h2>Disputers</h2>'+table(await j('/api/disputers'),[['disputer','Wallet',addr],['label','Label'],['disputes','Disputes'],['proposers_disputed','Proposers disputed'],['last_block','Last block']]);
 if(cur==='proposals')h+='<h2>Most recent proposals</h2>'+table(await j('/api/proposals'),[['block_number','Block'],['contract','Oracle'],['proposer','Proposer',addr],['proposed_price','Price'],['outcome','Outcome'],['latency_blocks','Latency'],['proposer_on_whitelist','Whitelisted',yn],['whitelist_basis','Basis'],['title','Market',esc,1]])+'<p><small>Failed / reverted competing attempts are not visible in events; they will come from the transaction_attempts table (V2.1).</small></p>';
 if(cur==='whitelist'){const w=await j('/api/whitelist');h+=w.snapshot?'<h2>Default proposer whitelist '+addr(w.snapshot.address)+' — '+w.snapshot.count+' addresses</h2>'+table(w.members,[['address','Wallet',addr],['label','Label'],['proposals','Proposals'],['last_block','Last block']]):'<p>No whitelist snapshot yet.</p>';
  h+='<h2>Per-request checks</h2>'+table([w.perRequest||{}]);if(w.outsiders.length)h+='<h2>Managed proposers not on the comparison whitelist</h2>'+table(w.outsiders,[['proposer','Wallet',addr],['proposals','Proposals']]);if(w.lastError)h+='<p class="no">Last snapshot error: '+esc(w.lastError.error)+'</p>';}
 if(cur==='competition'){const c=await j('/api/competition');const s=c.summary;
  if(!s.requests)h+='<p>No attempt data yet. Run <code>node v2.mjs attempts</code> (needs ETHERSCAN_API_KEY).</p>';else{
  h+='<div class="tiles">'+[['Requests tracked',s.requests],['Contested (2+ wallets)',s.contested],['Median wallets / request',s.median_wallets],['Max wallets',s.max_wallets],['Failed proposal txs',s.failed_txs],['Same-block races',s.same_block_races]].map(t=>'<div class="tile"><small>'+t[0]+'</small><b>'+(t[1]??0).toLocaleString()+'</b></div>').join('')+'</div>';
  if(s.coverage&&s.coverage.proposals)h+='<p><small>Direct-call coverage: '+s.coverage.direct+' of '+s.coverage.proposals+' accepted proposals were direct oracle calls. Bots that propose through their own contracts are not visible, so these counts are a lower bound.</small></p>';
  h+='<h2>Wallets per request</h2>'+table(s.buckets)+'<h2>Outcomes</h2>'+table(s.classes);
  h+='<h2>Competing wallets</h2>'+table(c.wallets,[['wallet','Wallet',addr],['label','Label'],['attempts','Attempts'],['requests','Requests'],['won','Won'],['win_pct','Win %'],['lost_race','Lost race'],['lost_same_block','Same block'],['not_whitelisted','Not WL'],['other_failed','Other fail'],['avg_gwei','Avg gwei'],['senders','Senders'],['last_block','Last block']]);
  h+='<h2>Most contested requests</h2>'+table(c.contested,[['contract','Oracle'],['first_block','Block'],['wallets','Wallets'],['txs','Txs'],['failed','Failed'],['same_block','Same block'],['title','Market',(v,r)=>esc(v||r.request_key),1]]);
  if(c.gas&&c.gas.winner_vs_loser_gas_ratio!=null)h+='<p><small>Same-block races: winner/loser gas price ratio '+c.gas.winner_vs_loser_gas_ratio+' (winner paid more in '+c.gas.winner_paid_more+', same or less in '+c.gas.winner_paid_same_or_less+').</small></p>';}}
 if(cur==='rpc')h+='<h2>RPC usage — last 60 minutes (estimated CUs)</h2>'+table(await j('/api/rpc'));
 v.innerHTML=h;}
function go(t){cur=t;location.hash='t='+t;render()}
render();setInterval(render,15000);
</script></body></html>`;

// ───────────────────────────────────────────────────────────── labels

function cmdLabel(store, args) {
  const [address, ...rest] = args;
  if (!/^0x[0-9a-fA-F]{40}$/.test(address ?? '') || !rest.length) throw new Error('Usage: node v2.mjs label <0xaddress> <label text>');
  store.q.upsertLabel.run(address.toLowerCase(), rest.join(' '), 'manual', nowSec());
  console.log(`Labelled ${address.toLowerCase()} as "${rest.join(' ')}"`);
}

function cmdImportLabels(store, args) {
  const file = args[0];
  if (!file) throw new Error('Usage: node v2.mjs import-labels <file.json>');
  const data = JSON.parse(fs.readFileSync(file, 'utf8'));
  const entries = Array.isArray(data) ? data.map((r) => [r.address ?? r.wallet, r.label ?? r.name]) : Object.entries(data).map(([k, v]) => [k, typeof v === 'string' ? v : v?.label ?? v?.name]);
  let n = 0;
  tx(store.db, () => { for (const [a, l] of entries) if (/^0x[0-9a-fA-F]{40}$/.test(a ?? '') && l) { store.q.upsertLabel.run(a.toLowerCase(), String(l), `file:${path.basename(file)}`, nowSec()); n++; } });
  console.log(`Imported ${n} labels from ${file}`);
}

// ───────────────────────────────────────────────────────────── main

const HELP = `UMA Monitor V2 (read-only)

  node v2.mjs import [paths...]        import old JSONL data into ${CFG.dbFile}
  node v2.mjs live                     run the monitor (Ctrl+C to stop)
  node v2.mjs sync                     catch up once and exit
  node v2.mjs backfill --from N [--to M]
  node v2.mjs whitelist [--limit N]
  node v2.mjs attempts [--from N] [--to M] [--reasons N]   competition tracker (needs ETHERSCAN_API_KEY)
  node v2.mjs report
  node v2.mjs dashboard [--port 8787]
  node v2.mjs label <address> <label>
  node v2.mjs import-labels <file.json>
`;

async function main() {
  const [cmd, ...args] = process.argv.slice(2);
  if (!cmd || cmd === 'help' || cmd === '--help') { console.log(HELP); return; }
  const db = await openDatabase(CFG.dbFile);
  const store = new Store(db);
  store.loadDefaultWhitelist();
  try {
    switch (cmd) {
      case 'import': await cmdImport(store, args); break;
      case 'live': await cmdSync(store, { live: true }); break;
      case 'sync': await cmdSync(store, { live: false }); break;
      case 'backfill': await cmdBackfill(store, args); break;
      case 'whitelist': await cmdWhitelist(store, args); break;
      case 'attempts': await cmdAttempts(store, args); break;
      case 'report': cmdReport(store); break;
      case 'dashboard': cmdDashboard(store, args); return; // keep DB open while serving
      case 'label': cmdLabel(store, args); break;
      case 'import-labels': cmdImportLabels(store, args); break;
      default: console.log(HELP); process.exitCode = 1;
    }
  } finally {
    if (cmd !== 'dashboard') db.close();
  }
}

main().catch((e) => { console.error(`Error: ${e.message}`); process.exit(1); });
