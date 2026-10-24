'use strict';
// Zero-dependency server (Node 18+). Serves public/index.html and /api/snapshot built from Upstox data.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');
const { Readable } = require('stream'), { StringDecoder } = require('string_decoder');
const E = process.env, API = 'https://api.upstox.com', PORT = E.PORT || 3000;
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Upstox's instrument file has no sector field, so sectors come from this map.
// Unmapped F&O symbols show under OTHERS; add fixes in a sectors.json file: {"SYMBOL":"SECTOR"}.
const RAW = {
  'BANKING': 'HDFCBANK ICICIBANK SBIN AXISBANK KOTAKBANK INDUSINDBK BANKBARODA PNB FEDERALBNK IDFCFIRSTB AUBANK BANDHANBNK CANBK RBLBANK UNIONBANK INDIANB YESBANK BANKINDIA',
  'FINANCIAL SERVICES': 'BAJFINANCE BAJAJFINSV CHOLAFIN SBILIFE HDFCLIFE MUTHOOTFIN SHRIRAMFIN LICHSGFIN PFC RECLTD ICICIPRULI ICICIGI HDFCAMC SBICARD MANAPPURAM JIOFIN LICI ANGELONE CDSL BSE MCX CAMS HUDCO IREDA KFINTECH NUVAMA PNBHOUSING',
  'IT': 'TCS INFY WIPRO HCLTECH TECHM LTIM COFORGE PERSISTENT MPHASIS OFSS KPITTECH TATAELXSI TATATECH',
  'PHARMA & HEALTHCARE': 'SUNPHARMA CIPLA DRREDDY DIVISLAB LUPIN AUROPHARMA TORNTPHARM ZYDUSLIFE ALKEM BIOCON GLENMARK MANKIND LAURUSLABS APOLLOHOSP FORTIS MAXHEALTH SYNGENE',
  'AUTO': 'MARUTI M&M ASHOKLEY BAJAJ-AUTO EICHERMOT HEROMOTOCO TVSMOTOR TATAMOTORS BHARATFORG BOSCHLTD MOTHERSON BALKRISIND APOLLOTYRE EXIDEIND SONACOMS UNOMINDA TIINDIA',
  'FMCG': 'HINDUNILVR ITC NESTLEIND BRITANNIA DABUR TATACONSUM GODREJCP MARICO COLPAL UNITDSPR UBL VBL PATANJALI RADICO',
  'METALS & MINING': 'TATASTEEL JSWSTEEL HINDALCO VEDL JINDALSTEL NMDC SAIL NATIONALUM HINDZINC APLAPOLLO COALINDIA',
  'POWER & ENERGY': 'RELIANCE NTPC POWERGRID TATAPOWER ADANIGREEN ADANIENSOL ADANIPOWER NHPC JSWENERGY TORNTPOWER SUZLON INOXWIND PREMIERENE WAAREEENER',
  'OIL & GAS': 'ONGC BPCL IOC GAIL HINDPETRO OIL PETRONET IGL MGL ATGL',
  'REALTY': 'DLF GODREJPROP OBEROIRLTY PRESTIGE LODHA PHOENIXLTD',
  'TELECOM': 'BHARTIARTL IDEA INDUSTOWER TATACOMM',
  'CAPITAL GOODS & INFRA': 'LT SIEMENS ABB BHEL HAL BEL CGPOWER POLYCAB HAVELLS DIXON KEI CUMMINSIND BDL MAZDOCK SOLARINDS RVNL NCC IRFC IRCTC CONCOR GMRAIRPORT ADANIPORTS ADANIENT',
  'CEMENT': 'ULTRACEMCO GRASIM AMBUJACEM SHREECEM DALBHARAT',
  'CHEMICALS': 'UPL PIDILITIND SRF PIIND TATACHEM DEEPAKNTR NAVINFLUOR',
  'CONSUMER & RETAIL': 'TITAN TRENT DMART PAGEIND JUBLFOOD NYKAA ETERNAL SWIGGY INDHOTEL KALYANKJIL VOLTAS BLUESTARCO ASIANPAINT BERGEPAINT NAUKRI INDIGO PAYTM POLICYBZR'
};
const SECTOR_OF = {};
for (const [sec, l] of Object.entries(RAW)) for (const s of l.split(' ')) SECTOR_OF[s] = sec;
try { Object.assign(SECTOR_OF, JSON.parse(fs.readFileSync(path.join(__dirname, 'sectors.json'), 'utf8'))); } catch (e) {}
const IDX = [['NIFTY 50', 'NSE_INDEX|Nifty 50'], ['BANK NIFTY', 'NSE_INDEX|Nifty Bank'], ['FINNIFTY', 'NSE_INDEX|Nifty Fin Service'], ['INDIA VIX', 'NSE_INDEX|India VIX']];

/* ---------- auth ---------- */
const token = () => E.UPSTOX_ANALYTICS_TOKEN || '';
const mode = () => token() ? 'analytics token' : 'none';
const sha = s => crypto.createHash('sha256').update(String(s)).digest();
function authed(req) { // optional HTTP basic auth for the whole site
  if (!E.DASHBOARD_PASSWORD) return true;
  const h = req.headers.authorization || '';
  if (!h.startsWith('Basic ')) return false;
  const s = Buffer.from(h.slice(6), 'base64').toString();
  return crypto.timingSafeEqual(sha(s.slice(s.indexOf(':') + 1)), sha(E.DASHBOARD_PASSWORD));
}

/* ---------- Upstox helpers ---------- */
// Upstox limits: 50 req/s, 500/min, 2000/30 min. All calls go through one pacer (~6-7/s) and back off hard on 429.
let nextAt = 0;
async function slot() { const now = Date.now(), t = Math.max(now, nextAt); nextAt = t + 150; if (t > now) await sleep(t - now); }
async function up(p) {
  const waits = [5000, 15000, 30000, 60000];
  for (let i = 0; ; i++) {
    await slot();
    const r = await fetch(API + p, { headers: { Authorization: 'Bearer ' + token(), Accept: 'application/json' } });
    if (r.status === 429) { if (i >= waits.length) throw new Error('Upstox rate limit hit'); nextAt = Math.max(nextAt, Date.now() + waits[i]); continue; }
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error((j.errors && j.errors[0] && j.errors[0].message) || 'Upstox HTTP ' + r.status);
    return j;
  }
}
function prevSession() { // weekends/holidays: latest weekday session, then one weekday earlier (exchange holidays not handled)
  const d = new Date(Date.now() + 19800000), wk = () => [0, 6].includes(d.getUTCDay());
  while (wk()) d.setUTCDate(d.getUTCDate() - 1);
  do d.setUTCDate(d.getUTCDate() - 1); while (wk());
  return d.toISOString().slice(0, 10);
}
const istDay = (off = 0) => new Date(Date.now() + 19800000 + off * 864e5).toISOString().slice(0, 10);
async function pool(items, n, fn) { let i = 0; await Promise.all(Array.from({ length: n }, async () => { while (i < items.length) await fn(items[i++]); })); }

// F&O universe = every underlying that has a stock future, with its nearest unexpired contract.
// The file is streamed and only FUT rows are parsed, to stay inside small-instance memory.
let INST = null, INST_DAY = '', instP = null;
async function loadInst() {
  const r = await fetch('https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz');
  if (!r.ok) throw new Error('Instrument file HTTP ' + r.status);
  const dec = new StringDecoder('utf8'), best = {}, now = Date.now();
  let tail = '';
  const take = p => {
    if (!/"instrument_type"\s*:\s*"FUT"/.test(p)) return;
    let o; try { o = JSON.parse('{' + p.replace(/^\[/, '').replace(/^\{/, '').replace(/\}\]?\s*$/, '') + '}'); } catch (e) { return; }
    if (o.segment !== 'NSE_FO' || !String(o.underlying_key || '').startsWith('NSE_EQ|') || o.expiry < now) return;
    const b = best[o.underlying_symbol];
    if (!b || o.expiry < b.expiry) best[o.underlying_symbol] = { eq: o.underlying_key, fut: o.instrument_key, expiry: o.expiry };
  };
  for await (const ch of Readable.fromWeb(r.body).pipe(zlib.createGunzip())) {
    const parts = (tail + dec.write(ch)).split('},{'); tail = parts.pop(); parts.forEach(take);
  }
  take(tail + dec.end());
  if (!Object.keys(best).length) throw new Error('No F&O instruments found in instrument file');
  INST = best; INST_DAY = istDay();
}
function ensureInst() {
  if (INST && INST_DAY === istDay()) return Promise.resolve();
  return instP || (instP = loadInst().finally(() => { instP = null; }));
}

// Daily candles (close + OI) are fetched once per IST day and cached in memory.
const D = { day: '', data: {}, done: 0, total: 0, busy: null, err: '', last: 0, ok: false };
async function candles(k) {
  const j = await up(`/v3/historical-candle/${encodeURIComponent(k)}/days/1/${prevSession()}/${istDay(-130)}`);
  const c = ((j.data && j.data.candles) || []).slice().sort((a, b) => a[0] < b[0] ? -1 : 1);
  return { close: c.map(x => x[4]), oi: c.map(x => x[6]) };
}
function startDaily() {
  const day = istDay();
  if (D.day !== day) { D.day = day; D.data = {}; D.ok = false; D.last = 0; }
  const keys = [...IDX.map(x => x[1]), ...Object.values(INST).flatMap(x => [x.eq, x.fut])], need = keys.filter(k => !D.data[k]);
  if (D.busy || !need.length || Date.now() - D.last < 90000) return; // only fetch what is missing, at most every 90 s
  D.busy = (async () => {
    D.total = need.length; D.done = 0;
    await pool(need, 4, async k => { try { D.data[k] = await candles(k); } catch (e) { D.err = e.message; console.warn('candles', k, e.message); } D.done++; });
    D.ok = Object.keys(D.data).length > keys.length / 2;
  })().catch(e => { D.err = e.message; }).finally(() => { D.busy = null; D.last = Date.now(); });
}
const ema = (a, n) => { if (a.length < n) return null; const k = 2 / (n + 1); let e = a[0]; for (let i = 1; i < a.length; i++) e = a[i] * k + e * (1 - k); return +e.toFixed(2); };
const fail = (m, code) => Object.assign(new Error(m), { code });

let QC = { t: 0, v: null }; // quote cache, 5 s, protects Upstox rate limits
async function snapshot() {
  if (!token()) throw fail('Feed: not connected. Set UPSTOX_ANALYTICS_TOKEN in the Render environment.', 503);
  await ensureInst(); startDaily();
  if (D.busy && !D.ok) return { loading: true, done: D.done, total: D.total };
  if (!D.ok) throw fail('History not ready: ' + (D.err || 'waiting') + '. Retrying shortly.', 503);
  const keys = [...IDX.map(x => x[1]), ...Object.values(INST).map(x => x.eq)], fresh = QC.v && Date.now() - QC.t < 5000, byTok = fresh ? QC.v : {};
  if (!fresh) {
    for (let i = 0; i < keys.length; i += 400) {
      const q = await up('/v3/market-quote/quotes?instrument_key=' + encodeURIComponent(keys.slice(i, i + 400).join(',')));
      for (const v of Object.values(q.data || {})) byTok[v.instrument_token] = v;
    }
    QC = { t: Date.now(), v: byTok };
  }
  const stocks = Object.entries(INST).map(([sym, x]) => {
    const e = D.data[x.eq], f = D.data[x.fut], q = byTok[x.eq];
    if (!e || !q || !e.close.length) return null;
    const c = e.close, n = c.length, o = f ? f.oi : [], m = o.length;
    return { symbol: sym, sector: SECTOR_OF[sym] || 'OTHERS', prev_close: c[n - 1], pre_price: q.last_price,
      pre_volume: null, avg_pre_volume: null,
      prev_change_pct: n > 1 ? (c[n - 1] - c[n - 2]) / c[n - 2] * 100 : null,
      prev_oi_change_pct: m > 1 && o[m - 2] > 0 ? (o[m - 1] - o[m - 2]) / o[m - 2] * 100 : null,
      ema20: ema(c, 20), ema50: ema(c, 50) };
  }).filter(Boolean);
  const indices = IDX.map(([name, k]) => {
    const q = byTok[k] || {}, c = D.data[k]; let pc = c && c.close.length ? c.close[c.close.length - 1] : null;
    if (pc == null && typeof q.net_change === 'number' && typeof q.last_price === 'number') pc = +(q.last_price - q.net_change).toFixed(2); // fallback if index history is unavailable
    return { name, prev_close: pc, price: q.last_price };
  });
  const have = new Set(stocks.map(x => x.symbol)), missing = Object.keys(INST).filter(x => !have.has(x));
  return { as_of: new Date(QC.t).toISOString(), source: 'Upstox (' + mode() + ')', auth: mode(), indices, stocks, universe: Object.keys(INST).length, missing };
}

/* ---------- http ---------- */
const send = (res, c, b, t = 'application/json') => { res.writeHead(c, { 'Content-Type': t, 'Cache-Control': 'no-store' }); res.end(typeof b === 'string' ? b : JSON.stringify(b)); };
http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url, 'http://x');
    if (u.pathname === '/healthz') return send(res, 200, 'ok', 'text/plain');
    if (!authed(req)) { res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="F&O dashboard"' }); return res.end(); }
    if (u.pathname === '/api/snapshot') { try { return send(res, 200, await snapshot()); } catch (e) { return send(res, e.code || 502, { error: e.message }); } }
    if (u.pathname === '/') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html'))); }
    send(res, 404, 'Not found', 'text/plain');
  } catch (e) { send(res, 500, { error: e.message }); }
}).listen(PORT, () => console.log('F&O dashboard on :' + PORT + (E.DASHBOARD_PASSWORD ? '' : ' (WARNING: no DASHBOARD_PASSWORD set, site is public)')));
