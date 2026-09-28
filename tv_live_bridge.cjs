// TV LIVE BRIDGE — one script, two outputs
// ========================================
// Every 5s during the EGX session (Sun-Thu 09:50 -> 14:36 Cairo, machine tz):
//   1) fetch the TradingView EGX scanner (whole market, one POST)
//   2) write CHANGED prices into tv_ticks (separate table, source='tv')
//   3) every 3rd sample append a JSONL snapshot (same format as
//      tv_lag_snap.cjs) so tv_lag_analyze.cjs works unchanged.
//
// Why a separate table: tv data freshness is NOT yet measured. Until the
// measurement verdict, tv prices must NOT masquerade as live prices in
// live_ticks. After the verdict the site can read tv_ticks knowingly.
//
// Backoff on fetch errors; single-instance lock; file log (bt/tv_bridge.log);
// prunes tv_ticks older than 3 days at session start.
//
// Usage:
//   node tv_live_bridge.cjs          (wait for session, run, exit 14:36)
//   node tv_live_bridge.cjs --once   (single sample now: DB + JSONL, for testing)
//
// ASCII ONLY.
const fs = require("fs");
const path = require("path");

const LOCK = path.join(__dirname, "tv_bridge.lock");
const LOG = path.join(__dirname, "tv_bridge.log");
const SCANNER_URL = "https://scanner.tradingview.com/egypt/scan?label-product=screener-stock";
const SAMPLE_MS = 5000;
const JSONL_EVERY = 3;            // snapshot file every 15s
const INTERVAL_BACKOFF = 15000;
const INTERVAL_MAX = 120000;
const START_H = 9, START_M = 50;
const END_H = 14, END_M = 36;
const TRADING_DOW = [0, 1, 2, 3, 4];

function log(msg) {
  const line = new Date().toISOString() + " " + msg;
  try { fs.appendFileSync(LOG, line + "\n"); } catch (e) {}
  console.log(line);
}
// Cairo-explicit time (machine-local on the PC, UTC on cloud runners — both correct)
function cairoParts(d) {
  const p = new Intl.DateTimeFormat("en-GB", { timeZone: "Africa/Cairo", weekday: "short", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(d);
  const g = (t) => p.find(x => x.type === t)?.value ?? "";
  const wdMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  return { dow: wdMap[g("weekday")] ?? 0, hh: parseInt(g("hour"), 10) || 0, mm: parseInt(g("minute"), 10) || 0 };
}
function minutesOfDay(d) { const c = cairoParts(d); return c.hh * 60 + c.mm; }
function isTradingDay(d) { return TRADING_DOW.includes(cairoParts(d).dow); }
function cairoDate(d) { return new Intl.DateTimeFormat("en-CA", { timeZone: "Africa/Cairo" }).format(d); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function loadEnv() {
  const env = { DATABASE_URL: process.env.DATABASE_URL || "" };
  try {
    for (const l of fs.readFileSync(path.join(__dirname, "..", ".env"), "utf8").split("\n")) {
      const m = l.match(/^\s*([A-Z_]+)\s*=\s*(.*)\s*$/);
      if (m) env[m[1]] = m[2].replace(/"/g, "");
    }
  } catch (e) { /* no .env (cloud) — rely on env vars */ }
  return env;
}

async function fetchTv() {
  const res = await fetch(SCANNER_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
    body: JSON.stringify({
      filter: [{ left: "type", operation: "equal", right: "stock" }],
      options: { lang: "en" },
      markets: ["egypt"],
      symbols: { query: { types: [] }, tickers: [] },
      columns: ["name", "open", "high", "low", "close", "volume", "change"],
      sort: { sortBy: "name", sortOrder: "asc" },
    }),
  });
  if (!res.ok) throw new Error("scanner HTTP " + res.status);
  const j = await res.json();
  const q = {}, v = {};
  for (const item of j.data ?? []) {
    const d = item.d;
    const sym = String(item.s || "").replace("EGX:", "").toUpperCase();
    const open = Number(d && d[1]);
    const close = Number(d && d[4]);
    if (!(close > 0) || !(open > 0)) continue;
    q[sym] = close;
    v[sym] = Number(d && d[5]) || 0;
  }
  return { q, v };
}

(async () => {
  const once = process.argv.includes("--once");
  const env = loadEnv();
  const { Pool } = require("pg");
  const pool = new Pool({ connectionString: env.DATABASE_URL });

  if (once) {
    const { q, v } = await fetchTv();
    const syms = Object.keys(q);
    const now = new Date();
    const dateTag = cairoDate(now); // Cairo date — session never crosses Cairo midnight
    const batch = syms.map(s => [s, q[s], v[s]]);
    for (let i = 0; i < batch.length; i += 500) {
      const chunk = batch.slice(i, i + 500);
      const values = [];
      const params = [];
      chunk.forEach((row, ri) => {
        const base = ri * 4;
        values.push("($" + (base + 1) + ",$" + (base + 2) + ",$" + (base + 3) + ",$" + (base + 4) + ")");
        params.push(row[0], row[1], row[2], now.toISOString());
      });
      await pool.query("INSERT INTO tv_ticks (symbol, price, volume, ts) VALUES " + values.join(","), params);
    }
    const rec = { iso: now.toISOString(), ms: now.getTime(), n: syms.length, q };
    const outFile = path.join(__dirname, "tv_lag_snapshots_" + dateTag + ".jsonl");
    fs.appendFileSync(outFile, JSON.stringify(rec) + "\n");
    log("ONCE OK: " + syms.length + " symbols -> tv_ticks + " + outFile);
    await pool.end();
    return;
  }

  // single-instance lock (ignore stale lock older than 12h)
  try {
    const st = fs.statSync(LOCK);
    if (Date.now() - st.mtimeMs < 12 * 3600e3) { log("already running, exit"); return; }
    fs.unlinkSync(LOCK);
  } catch (e) { /* no lock */ }
  fs.writeFileSync(LOCK, String(process.pid));

  try {
    let d0 = new Date();
    if (!isTradingDay(d0)) { log("not a trading day, exit"); return; }
    if (minutesOfDay(d0) > END_H * 60 + END_M) { log("session over, exit"); return; }
    const dateTag = cairoDate(d0); // Cairo date — matches ticks' UTC day for our session window
    const outFile = path.join(__dirname, "tv_lag_snapshots_" + dateTag + ".jsonl");
    if (minutesOfDay(d0) < START_H * 60 + START_M) {
      const waitMs = (START_H * 60 + START_M - minutesOfDay(d0)) * 60e3 - d0.getSeconds() * 1e3;
      log("armed: waiting " + Math.round(waitMs / 60000) + " min until 09:50");
      await sleep(Math.max(0, waitMs));
    }

    // prune old tv_ticks (keep 3 days)
    try { await pool.query("DELETE FROM tv_ticks WHERE ts < now() - interval '3 days'"); log("pruned old tv_ticks"); }
    catch (e) { log("prune failed: " + e.message); }

    let interval = SAMPLE_MS, fails = 0, ok = 0, writes = 0, sampleNo = 0;
    let prevQ = null;
    log("session started, sampling every " + SAMPLE_MS / 1000 + "s");
    while (true) {
      const d = new Date();
      if (!isTradingDay(d) || minutesOfDay(d) > END_H * 60 + END_M) break;
      try {
        const { q, v } = await fetchTv();
        ok++; fails = 0; interval = SAMPLE_MS;
        sampleNo++;
        // JSONL snapshot every 3rd sample
        if (sampleNo % JSONL_EVERY === 1) {
          const rec = { iso: d.toISOString(), ms: d.getTime(), n: Object.keys(q).length, q };
          fs.appendFileSync(outFile, JSON.stringify(rec) + "\n");
        }
        // diff write: only symbols whose price changed
        const now = d;
        const batch = [];
        for (const s of Object.keys(q)) {
          if (prevQ && prevQ[s] === q[s]) continue;
          batch.push([s, q[s], v[s]]);
        }
        for (let i = 0; i < batch.length; i += 500) {
          const chunk = batch.slice(i, i + 500);
          const values = [];
          const params = [];
          chunk.forEach((row, ri) => {
            const base = ri * 4;
            values.push("($" + (base + 1) + ",$" + (base + 2) + ",$" + (base + 3) + ",$" + (base + 4) + ")");
            params.push(row[0], row[1], row[2], now.toISOString());
          });
          await pool.query("INSERT INTO tv_ticks (symbol, price, volume, ts) VALUES " + values.join(","), params);
          writes += chunk.length;
        }
        prevQ = q;
        if (ok % 60 === 1) log("samples=" + ok + " dbWrites=" + writes + " lastDiff=" + batch.length);
      } catch (e) {
        fails++;
        interval = Math.min(INTERVAL_MAX, interval * 2);
        log("fail #" + fails + ": " + e.message + " -> backoff " + interval / 1000 + "s");
      }
      await sleep(interval);
    }
    fs.writeFileSync(path.join(__dirname, "tv_bridge_DONE_" + dateTag + ".txt"), "samples=" + ok + " writes=" + writes + " at " + new Date().toISOString() + "\n");
    log("DONE: samples=" + ok + " dbWrites=" + writes);
  } finally {
    try { fs.unlinkSync(LOCK); } catch (e) {}
    try { await pool.end(); } catch (e) {}
  }
})().catch(e => { log("FATAL: " + e.message); process.exit(1); });
