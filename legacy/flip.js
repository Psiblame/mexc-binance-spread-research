'use strict';
throw new Error('Network-enabled legacy program is quarantined. Use src/demo.js for offline research.');
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const axios = require('axios');
const WebSocket = require('ws');

const { MEXC_WEB_TOKEN } = require('../src/config');
const TOKEN = MEXC_WEB_TOKEN;
const POSITION_HISTORY_LOG = path.join(__dirname, 'Flip.position_history.log');
const SYSTEM_LOG = path.join(__dirname, 'Flip.system.log');

// ======================== CONFIG ========================
const SYMBOLS                   = ['BCH_USDT',];
const BALANCE_USE_FRACTION      = 0.30;
const LEVERAGE                  = 100;
/** 0 = без потолка по нотационалу (USDT). Раньше было 2000 USDT — это не то же самое, что vol в контрактах. */
const MAX_ORDER_NOTIONAL_USDT   = 0;
/** Потолок по полю vol на MEXC (контракты). Напр. 2000 контрактов BCH_USDT = 20 BCH ≈ 20×цена USDT. */
const MAX_ORDER_VOL_CONTRACTS   = 2000;
const NEED_ORDERS               = false;
const USE_LIMIT_ORDER           = false;
const LIMIT_WAIT_MS             = 500;
const MARGIN_MODE               = 'cross'; // 'cross' | 'isolated'
const ENFORCE_ZERO_FEE_TICKERS  = true;
const MEXC_WS_DEPTH_LIMIT       = 5;
const WS_RECONNECT_DELAY_MS     = 2000;
const MEXC_WS_STALE_MS          = 3000;
const BINANCE_WS_STALE_MS       = 3000;
const ENTRY_SPREAD_STEPS        = 6;
const CLOSE_REVERSE_STEPS       = 1;
const MAIN_TICK_MS              = 50;
const STATUS_LOG_INTERVAL_MS    = 5000;
const BALANCE_FETCH_INTERVAL_MS = 2000;
const STATS_INTERVAL_MS         = 60000;
const PENDING_ORDER_TIMEOUT_MS  = 8000;
const OPEN_TYPE                 = MARGIN_MODE === 'cross' ? 2 : 1;

if (!['cross', 'isolated'].includes(MARGIN_MODE)) {
    throw new Error(`Invalid MARGIN_MODE: ${MARGIN_MODE}. Use 'cross' or 'isolated'.`);
}

// ======================== HELPERS ========================
function md5(v) { return crypto.createHash('md5').update(v, 'utf8').digest('hex'); }

function signReq(key, obj) {
    const nonce = String(Date.now());
    const g = md5(key + nonce).substr(7);
    const sign = md5(nonce + JSON.stringify(obj) + g);
    return { nonce, sign };
}

function tsPrefix() {
    const d = new Date();
    const p2 = n => String(n).padStart(2, '0');
    const p3 = n => String(n).padStart(3, '0');
    return `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}.${p3(d.getMilliseconds())} `;
}

const DASHBOARD_REFRESH_MS = 500;
const UI = { lastDashboardAt: 0 };

function logClock() { return '[' + new Date().toTimeString().slice(0, 8) + '] '; }
function appendSystemLog(msg) {
    try {
        fs.appendFileSync(SYSTEM_LOG, logClock() + msg + '\n', 'utf8');
    } catch {}
}
function logLine(msg) {
    appendSystemLog(msg);
    if (!process.stdout.isTTY) console.log(logClock() + msg);
}
function logEvent(msg) { logLine(msg); }

const RISK_CONTROL_CODES = new Set([6020, 2027, 2034, 2039, 2040, 2041, 2042, 2043, 2044, 2046, 602]);
const RISK_CONTROL_PATTERNS = [/risk.?control/i, /insufficient.?margin/i, /margin.?not.?enough/i, /liquidat/i, /reduce.?only/i, /position.?limit/i];
function isCriticalOpenError(resp) {
    if (!resp) return false;
    const code = Number(resp.code ?? resp.errno ?? 0);
    if (RISK_CONTROL_CODES.has(code)) return true;
    const msg = String(resp.message ?? resp.msg ?? '');
    for (const p of RISK_CONTROL_PATTERNS) { if (p.test(msg)) return true; }
    return false;
}
function criticalExit(reason) {
    const msg = `CRITICAL STOP | ${reason}`;
    appendSystemLog(msg);
    appendPositionHistory(msg);
    if (process.stdout.isTTY) process.stdout.write('\x1b[H\x1b[J');
    process.stderr.write(`\n\x1b[41m\x1b[37m  ${msg}  \x1b[0m\n`);
    process.exit(1);
}
function appendPositionHistory(msg) {
    try {
        fs.appendFileSync(POSITION_HISTORY_LOG, logClock() + msg + '\n', 'utf8');
    } catch (e) {
        console.error(tsPrefix() + 'position_history_write_failed: ' + (e.message || e));
    }
}

function mexcToBin(s) { return s.replace('_', ''); }
function wsFresh(ts, now, maxAge) { return ts !== null && (now - ts) <= maxAge; }
function closeRef(dir, bid, ask) { return dir === 'long' ? bid : dir === 'short' ? ask : null; }
function displayName(s) { return s.endsWith('_USDT') ? s.slice(0, -5) : s; }
function formatPrice(v) { return Number.isFinite(v) ? Number(v).toFixed(5) : 'n/a'; }
function formatVol(v) { return Number.isFinite(v) ? String(Number(v)) : 'n/a'; }
function formatSignedUsd(v) {
    if (!Number.isFinite(v)) return 'n/a';
    return `${v >= 0 ? '+' : ''}${Number(v).toFixed(4)} USDT`;
}
function summarizeRecentPnlUsd(tradeHistory, nowTs) {
    const windows = { '10m': 600, '30m': 1800, '1h': 3600, '3h': 10800, '5h': 18000 };
    const totals = { '10m': 0, '30m': 0, '1h': 0, '3h': 0, '5h': 0 };
    for (const trade of tradeHistory) {
        if (!trade || !Number.isFinite(trade.ts) || !Number.isFinite(trade.pnlUsd)) continue;
        const age = nowTs - trade.ts;
        for (const [label, seconds] of Object.entries(windows)) {
            if (age <= seconds) totals[label] += trade.pnlUsd;
        }
    }
    return totals;
}
function summarizeRecentWinLoss(tradeHistory, nowTs) {
    const windows = { '10m': 600, '30m': 1800, '1h': 3600, '3h': 10800, '5h': 18000 };
    const totals = {
        '10m': { win: 0, loss: 0 },
        '30m': { win: 0, loss: 0 },
        '1h': { win: 0, loss: 0 },
        '3h': { win: 0, loss: 0 },
        '5h': { win: 0, loss: 0 },
    };
    for (const trade of tradeHistory) {
        if (!trade || !Number.isFinite(trade.ts) || !Number.isFinite(trade.pnlUsd)) continue;
        const age = nowTs - trade.ts;
        for (const [label, seconds] of Object.entries(windows)) {
            if (age <= seconds) {
                if (trade.pnlUsd >= 0) totals[label].win++;
                else totals[label].loss++;
            }
        }
    }
    return totals;
}
function formatRecentPnlUsdSummary(windowTotals) {
    return `10m=${formatSignedUsd(windowTotals['10m'])}`
        + ` | 30m=${formatSignedUsd(windowTotals['30m'])}`
        + ` | 1h=${formatSignedUsd(windowTotals['1h'])}`
        + ` | 3h=${formatSignedUsd(windowTotals['3h'])}`
        + ` | 5h=${formatSignedUsd(windowTotals['5h'])}`;
}
function formatRecentWinLossSummary(windowTotals) {
    return `10m=${windowTotals['10m'].win}/${windowTotals['10m'].loss}`
        + ` | 30m=${windowTotals['30m'].win}/${windowTotals['30m'].loss}`
        + ` | 1h=${windowTotals['1h'].win}/${windowTotals['1h'].loss}`
        + ` | 3h=${windowTotals['3h'].win}/${windowTotals['3h'].loss}`
        + ` | 5h=${windowTotals['5h'].win}/${windowTotals['5h'].loss}`;
}
function padRight(v, w) {
    const s = String(v ?? '');
    return s.length >= w ? s.slice(0, w) : s + ' '.repeat(w - s.length);
}
function tableBorder(w1, w2, w3 = null) {
    if (w3 === null) return `+${'-'.repeat(w1 + 2)}+${'-'.repeat(w2 + 2)}+`;
    return `+${'-'.repeat(w1 + 2)}+${'-'.repeat(w2 + 2)}+${'-'.repeat(w3 + 2)}+`;
}
function renderTwoColRows(rows, w1 = 16, w2 = 34) {
    const out = [tableBorder(w1, w2)];
    for (const [k, v] of rows) out.push(`| ${padRight(k, w1)} | ${padRight(v, w2)} |`);
    out.push(tableBorder(w1, w2));
    return out;
}
function renderThreeColRows(header, rows, w1 = 8, w2 = 16, w3 = 12) {
    const out = [tableBorder(w1, w2, w3)];
    out.push(`| ${padRight(header[0], w1)} | ${padRight(header[1], w2)} | ${padRight(header[2], w3)} |`);
    out.push(tableBorder(w1, w2, w3));
    for (const row of rows) out.push(`| ${padRight(row[0], w1)} | ${padRight(row[1], w2)} | ${padRight(row[2], w3)} |`);
    out.push(tableBorder(w1, w2, w3));
    return out;
}
function buildDashboardLines(snapshot) {
    const rowsTop = [
        ['Time', new Date().toTimeString().slice(0, 8)],
        ['Symbol', snapshot.symbol],
        ['Position', snapshot.position],
        ['Pending', snapshot.pending],
        ['Balance', snapshot.balance],
        ['Total PnL', snapshot.totalPnl],
        ['Entry', snapshot.entry],
        ['Current Px', snapshot.current],
        ['Volume', snapshot.volume],
        ['Unrealized', snapshot.unrealized],
        ['MEXC Bid/Ask', `${snapshot.mexcBid} / ${snapshot.mexcAsk}`],
        ['BIN Bid/Ask', `${snapshot.binBid} / ${snapshot.binAsk}`],
        ['Spread', snapshot.spread],
        ['Binance tick', snapshot.binanceTick ?? 'n/a'],
    ];
    const rowsWin = [
        ['10m', snapshot.pnl['10m'], snapshot.wl['10m']],
        ['30m', snapshot.pnl['30m'], snapshot.wl['30m']],
        ['1h', snapshot.pnl['1h'], snapshot.wl['1h']],
        ['3h', snapshot.pnl['3h'], snapshot.wl['3h']],
        ['5h', snapshot.pnl['5h'], snapshot.wl['5h']],
    ];
    return [
        'Flip Dashboard',
        ...renderTwoColRows(rowsTop),
        ...renderThreeColRows(['Window', 'PnL', 'W/L'], rowsWin),
    ];
}
function renderDashboard(snapshot) {
    if (!process.stdout.isTTY) return;
    const lines = buildDashboardLines(snapshot);
    process.stdout.write('\x1b[H\x1b[J' + lines.join('\n') + '\n');
}
function respCode(resp) { return resp?.code ?? 'null'; }
function respMsg(resp) { return resp?.message ?? resp?.msg ?? 'null'; }
function sessionUptimeLog() {
    return formatUptime(Math.max(0, (Date.now() - S.startTs) / 1000));
}

function logTradeOpened(direction, symbol, entryPrice, volume, orderId = null) {
    const balStr = S.bal !== null ? S.bal.toFixed(4) : 'n/a';
    const msg =
        `OPEN ${direction.toUpperCase()} | symbol=${symbol}`
        + ` | entry=${formatPrice(entryPrice)}`
        + ` | vol=${formatVol(volume)}`
        + ` | leverage=${LEVERAGE}`
        + ` | margin=${MARGIN_MODE}`
        + ` | balance=${balStr}`
        + ` | uptime=${sessionUptimeLog()}`
        + ` | orderId=${orderId ?? 'NULL'}`;
    logLine(msg);
    appendPositionHistory(msg);
}
function logTradeClosed(direction, symbol, entryPrice, exitPrice, volume, tradePnl, totalPnl) {
    const recentPnl = formatRecentPnlUsdSummary(summarizeRecentPnlUsd(S.tradeHistory, Date.now() / 1000));
    const balStr = S.bal !== null ? S.bal.toFixed(4) : 'n/a';
    const msg =
        `CLOSE ${direction.toUpperCase()} | symbol=${symbol}`
        + ` | entry=${formatPrice(entryPrice)}`
        + ` | exit=${formatPrice(exitPrice)}`
        + ` | vol=${formatVol(volume)}`
        + ` | leverage=${LEVERAGE}`
        + ` | margin=${MARGIN_MODE}`
        + ` | balance=${balStr}`
        + ` | tradeUsd=${formatSignedUsd(tradePnl)}`
        + ` | totalPnl=${formatSignedUsd(totalPnl)}`
        + ` | uptime=${sessionUptimeLog()}`
        + ` | trades=${S.trades}`
        + ` | win=${S.wins}`
        + ` | loss=${S.losses}`
        + ` | ${recentPnl}`;
    logLine(msg);
    appendPositionHistory(msg);
}

function formatUptime(sec) {
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = Math.floor(sec % 60);
    return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function pickFirstNumber(arr, def = null) {
    for (const v of arr) {
        if (v === null || v === undefined) continue;
        const n = Number(v);
        if (Number.isFinite(n)) return n;
    }
    return def;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ======================== MEXC REST (futures.mexc.com) ========================
const FUTURES_BASE  = 'https://futures.mexc.com/api/v1';
const CONTRACT_BASE = 'https://contract.mexc.com/api/v1';

function futuresHeaders(payload) {
    const sig = signReq(TOKEN, payload);
    return {
        'Content-Type': 'application/json',
        Authorization: TOKEN,
        'x-mxc-sign': sig.sign,
        'x-mxc-nonce': sig.nonce,
        'User-Agent': 'Mozilla/5.0',
        Origin: 'https://futures.mexc.com',
    };
}

function contractHeaders() {
    const sig = signReq(TOKEN, {});
    return {
        Accept: 'application/json',
        'Content-Type': 'application/json;charset=UTF-8',
        Authorization: TOKEN,
        'x-mxc-sign': sig.sign,
        'x-mxc-nonce': sig.nonce,
        'User-Agent': 'Mozilla/5.0',
        Origin: 'https://www.mexc.com',
    };
}

async function mxPost(ep, body) {
    try {
        const r = await axios.post(FUTURES_BASE + ep, body, { headers: futuresHeaders(body), timeout: 15000 });
        return r.data;
    } catch (e) { return { success: false, code: -1, msg: e.message }; }
}

async function mxGet(ep, params) {
    const p = params || {};
    try {
        const r = await axios.get(FUTURES_BASE + ep, { headers: futuresHeaders(p), params: p, timeout: 15000 });
        return r.data;
    } catch (e) { return { success: false, code: -1, msg: e.message }; }
}

async function contractSignedGet(ep) {
    try {
        const r = await axios.get(CONTRACT_BASE + ep, { headers: contractHeaders(), timeout: 15000 });
        return r.data;
    } catch (e) { return { success: false, code: -1, msg: e.message }; }
}

async function httpGetJson(url, t = 10000) {
    try { return (await axios.get(url, { timeout: t })).data; } catch { return null; }
}

// ======================== MEXC ORDER API ========================
async function createMarketOrder(symbol, vol, side, leverage) {
    return mxPost('/private/order/create', {
        symbol, price: 0, vol, side, type: 5, openType: OPEN_TYPE, leverage,
    });
}

async function createLimitOrder(symbol, vol, side, leverage, price) {
    return mxPost('/private/order/create', {
        symbol, price, vol, side, type: 1, openType: OPEN_TYPE, leverage,
    });
}

async function cancelOrder(orderId) { return mxPost('/private/order/cancel', [orderId]); }
async function getOrderById(orderId) { return mxGet('/private/order/get/' + orderId); }
async function getOpenPositions(symbol) { return mxGet('/private/position/open_positions', { symbol }); }

function isOk(r) { return r && (r.success === true || r.code === 0); }

function extractOid(r) {
    if (!r) return null;
    const d = r.data;
    if (d == null) return null;
    if (typeof d !== 'object') { const s = String(d).trim(); return s || null; }
    if (d.orderId) return String(d.orderId);
    if (Array.isArray(d) && d[0]?.orderId) return String(d[0].orderId);
    return null;
}

function orderState(info) {
    const s = info?.data?.state;
    if (s === undefined || s === null) return null;
    return parseInt(s, 10);
}

function findPosition(resp, symbol, dir) {
    const type = dir === 'long' ? 1 : 2;
    const arr = resp?.data;
    if (!Array.isArray(arr)) return null;
    for (const p of arr) {
        if (!p || p.symbol !== symbol) continue;
        if (Number(p.positionType ?? 0) !== type) continue;
        if (Number(p.holdVol ?? 0) <= 0) continue;
        if (Number(p.state ?? 0) === 3) continue;
        return p;
    }
    return null;
}

function listClosable(resp, symbol) {
    const arr = resp?.data;
    if (!Array.isArray(arr)) return [];
    const out = [];
    for (const p of arr) {
        if (!p || p.symbol !== symbol) continue;
        if (Number(p.holdVol ?? 0) <= 0) continue;
        if (Number(p.state ?? 0) === 3) continue;
        const t = Number(p.positionType ?? 0);
        if (t !== 1 && t !== 2) continue;
        out.push(p);
    }
    return out;
}

function parseUsdtBalance(resp) {
    if (!isOk(resp)) return null;
    const assets = resp?.data;
    if (!Array.isArray(assets)) return null;
    let sum = 0;
    for (const a of assets) {
        if (!a) continue;
        if (String(a.currency || a.asset || '').toUpperCase() !== 'USDT') continue;
        const n = Number(
            a.equity
            ?? a.walletBalance
            ?? a.balance
            ?? a.totalBalance
            ?? a.availableBalance
            ?? 0
        );
        sum += Number.isFinite(n) ? n : 0;
    }
    return sum;
}

function resolveLevForClose(pos, det, levUser) {
    const lp = Number(pos?.leverage ?? 0);
    const mx = Math.max(1, Number(det?.maxLeverage ?? levUser));
    if (lp > 0) return Math.min(Math.max(1, lp), mx);
    return Math.min(Math.max(1, levUser), mx);
}

function extractExchangePnl(orderData, posResp, symbol, dir) {
    const fields = ['profit', 'closeProfitLoss', 'closeProfit', 'realised', 'realized'];
    for (const f of fields) {
        if (orderData[f] != null && Number.isFinite(Number(orderData[f]))) return Number(orderData[f]);
    }
    const positions = posResp?.data;
    if (Array.isArray(positions)) {
        const type = dir === 'long' ? 1 : 2;
        for (const p of positions) {
            if (!p || p.symbol !== symbol || Number(p.positionType ?? 0) !== type) continue;
            for (const f of fields) {
                if (p[f] != null && Number.isFinite(Number(p[f]))) return Number(p[f]);
            }
        }
    }
    return null;
}

// ======================== BINANCE ========================
async function getBinanceTickSize(binSymbol) {
    try {
        const r = await axios.get('https://fapi.binance.com/fapi/v1/exchangeInfo', { timeout: 15000 });
        const syms = r.data?.symbols;
        if (!Array.isArray(syms)) return null;
        for (const s of syms) {
            if (s.symbol !== binSymbol) continue;
            if (!Array.isArray(s.filters)) return null;
            for (const f of s.filters) {
                if (f.filterType === 'PRICE_FILTER' && f.tickSize) return parseFloat(f.tickSize);
            }
        }
        return null;
    } catch (e) { console.error('getBinanceTickSize:', e.message); return null; }
}

// ======================== CONTRACT DETAIL + FEES ========================
async function fetchContractDetail(symbol) {
    const j = await httpGetJson(`https://contract.mexc.com/api/v1/contract/detail?symbol=${encodeURIComponent(symbol)}`);
    if (!j?.data) return null;
    const d = j.data;
    d.makerFeeRate = pickFirstNumber([d.makerFeeRate, d.makerFee, d.makerFeeRatio, d.makerRate, d.makerFeeScale]);
    d.takerFeeRate = pickFirstNumber([d.takerFeeRate, d.takerFee, d.takerFeeRatio, d.takerRate, d.takerFeeScale]);
    return d;
}

function feePolicy(det) {
    const maker = det?.makerFeeRate ?? null;
    const taker = det?.takerFeeRate ?? null;
    const makerN = maker !== null && Number.isFinite(Number(maker)) ? Number(maker) : null;
    const takerN = taker !== null && Number.isFinite(Number(taker)) ? Number(taker) : null;
    const bothKnown = makerN !== null && takerN !== null;
    const hasPos = (makerN || 0) > 0 || (takerN || 0) > 0;
    return {
        makerFeeRate: makerN, takerFeeRate: takerN,
        blockOpenOrders: ENFORCE_ZERO_FEE_TICKERS && !(bothKnown && !hasPos),
    };
}

// ======================== VOLUME ========================
function adjVol(vol, volScale) {
    if (vol <= 0) return 0;
    if (volScale <= 0) return Math.floor(vol);
    const m = 10 ** volScale;
    return Math.floor(vol * m) / m;
}

function effMaxVol(det) {
    const mv = Number(det?.maxVol ?? 0);
    const lmv = Number(det?.limitMaxVol ?? 0);
    if (mv > 0 && lmv > 0) return Math.min(mv, lmv);
    return mv > 0 ? mv : (lmv > 0 ? lmv : 0);
}

function maxNotionalFromExch(det, price) {
    const em = effMaxVol(det);
    if (em <= 0) return Number.MAX_SAFE_INTEGER;
    const cs = Number(det?.contractSize ?? 1);
    if (price <= 0 || cs <= 0) return Number.MAX_SAFE_INTEGER;
    return em * price * cs;
}

function computeVol(bal, det, refPrice, bookCap, levUser) {
    const cs = Number(det?.contractSize ?? 1);
    const vs = Number(det?.volScale ?? 0);
    const minV = Number(det?.minVol ?? 1);
    const em = effMaxVol(det);
    const maxLevDet = Number(det?.maxLeverage ?? levUser);
    const eLev = Math.min(Math.max(1, levUser), Math.max(1, maxLevDet));
    if (refPrice <= 0 || cs <= 0 || bal <= 0) return null;
    let maxN = bal * BALANCE_USE_FRACTION * eLev;
    maxN = Math.min(maxN, maxNotionalFromExch(det, refPrice));
    if (MAX_ORDER_NOTIONAL_USDT > 0) maxN = Math.min(maxN, MAX_ORDER_NOTIONAL_USDT);
    let vol = maxN / (refPrice * cs);
    vol = adjVol(vol, vs);
    if (em > 0) vol = Math.min(vol, adjVol(em, vs));
    if (MAX_ORDER_VOL_CONTRACTS > 0) vol = Math.min(vol, adjVol(MAX_ORDER_VOL_CONTRACTS, vs));
    return vol < minV ? null : vol;
}

function notionalUsdt(vol, price, cs) {
    return vol > 0 && price > 0 && cs > 0 ? vol * price * cs : 0;
}

function pnlLinear(entry, exit, holdVol, cs, dir) {
    if (holdVol <= 0 || cs <= 0) return 0;
    return dir === 'long' ? (exit - entry) * holdVol * cs : (entry - exit) * holdVol * cs;
}

function wsCapQty(side, bidQty, askQty) {
    if (side === 1 || side === 2) return (askQty != null && askQty > 0) ? askQty : null;
    if (side === 3 || side === 4) return (bidQty != null && bidQty > 0) ? bidQty : null;
    return null;
}

// ======================== LIMIT ORDER HELPER ========================
async function tryOpenOptLimit(useLimitOrder, waitMs, symbol, vol, side, lev, limitPrice) {
    const createResp = useLimitOrder
        ? await createLimitOrder(symbol, vol, side, lev, limitPrice)
        : await createMarketOrder(symbol, vol, side, lev);
    const result = { create: createResp, orderId: extractOid(createResp), filled: false, canceled: false, orderInfo: null, cancelResponse: null };
    if (!useLimitOrder) return result;
    if (!isOk(createResp) || !result.orderId) return result;
    await sleep(Math.max(0, waitMs));
    const oi = await getOrderById(result.orderId);
    result.orderInfo = oi;
    if (orderState(oi) === 3) { result.filled = true; return result; }
    result.cancelResponse = await cancelOrder(result.orderId);
    result.canceled = true;
    return result;
}

// ======================== STATE ========================
const S = {
    mexcBid: {}, mexcAsk: {}, mexcBidQty: {}, mexcAskQty: {},
    mexcDepthTs: {}, mexcQuoteTs: {},
    binBid: {}, binAsk: {}, binBidQty: {}, binAskQty: {},
    binQuoteTs: {}, prevBinBid: {},
    tickSize: {}, detail: {}, feeLogged: {},
    position: null, posSym: null, entryPrice: null,
    posHoldVol: null, dryHoldVol: null,
    pending: null, pendOid: null, pendSince: null, pendEntry: null, pendSym: null,
    openOid: null, closeOid: null,
    totalPnl: 0, pnlBy: {}, trades: 0, wins: 0, losses: 0, tradeHistory: [],
    startTs: Date.now(), bal: null, balTs: 0, lastStatusLogAt: 0,
};

for (const sym of SYMBOLS) {
    S.mexcBid[sym] = null; S.mexcAsk[sym] = null;
    S.mexcBidQty[sym] = null; S.mexcAskQty[sym] = null;
    S.mexcDepthTs[sym] = null; S.mexcQuoteTs[sym] = null;
    S.binBid[sym] = null; S.binAsk[sym] = null;
    S.binBidQty[sym] = null; S.binAskQty[sym] = null;
    S.binQuoteTs[sym] = null; S.prevBinBid[sym] = null;
    S.pnlBy[sym] = 0; S.feeLogged[sym] = false;
}

// ======================== BALANCE ========================
async function fetchBalance() {
    const r = await contractSignedGet('/private/account/assets');
    const p = parseUsdtBalance(r);
    if (p !== null) S.bal = p;
    S.balTs = Date.now();
}

// ======================== STATS ========================
function emitStats(src = 'timer') {
    if (process.stdout.isTTY && src === 'timer') return;
    const elapsed = (Date.now() - S.startTs) / 1000;
    const avg = S.trades > 0 ? S.totalPnl / S.trades : null;
    const hrs = Math.max(elapsed, 60) / 3600;
    const perH = hrs > 0 ? S.totalPnl / hrs : 0;
    const balStr = S.bal !== null ? S.bal.toFixed(2) + ' USDT' : 'n/a';
    const byTicker = SYMBOLS.map(s => `${displayName(s)}=${(S.pnlBy[s] || 0).toFixed(2)}`).join(', ');
    const nowTs = Date.now() / 1000;
    const recentPnlWindows = summarizeRecentPnlUsd(S.tradeHistory, nowTs);
    const recentWinLoss = summarizeRecentWinLoss(S.tradeHistory, nowTs);
    let totalStr = S.totalPnl.toFixed(2) + ' USDT';
    if (SYMBOLS.length > 1) totalStr += ` (${byTicker})`;
    logLine(
        `STATS | source=${src}`
        + ` | totalPnl=${totalStr}`
        + ` | trades=${S.trades}`
        + ` | win=${S.wins}`
        + ` | loss=${S.losses}`
        + ` | avgPnl=${avg !== null ? avg.toFixed(2) + ' USDT' : 'n/a'}`
        + ` | uptime=${formatUptime(elapsed)}`
        + ` | pnlPerHour=${perH.toFixed(2)} USDT`
        + ` | balance=${balStr}`
        + ` | ${formatRecentPnlUsdSummary(recentPnlWindows)}`
        + ` | wl=${formatRecentWinLossSummary(recentWinLoss)}`
        + (src === 'key:T' ? ` | pnlByTicker=${byTicker}` : '')
    );
}

// ======================== MEXC WS ========================
function startMexcWs(sym) {
    let done = false;
    const ws = new WebSocket('wss://contract.mexc.com/edge');
    let ping = null;

    function reconnect(reason) {
        if (done) return; done = true;
        if (ping) clearInterval(ping);
        S.mexcBid[sym] = null; S.mexcAsk[sym] = null;
        S.mexcBidQty[sym] = null; S.mexcAskQty[sym] = null;
        S.mexcDepthTs[sym] = null; S.mexcQuoteTs[sym] = null;
        logLine(`MEXC reconnect scheduled | symbol=${sym} | reason=${reason} | in=${(WS_RECONNECT_DELAY_MS / 1000).toFixed(1)}s`);
        setTimeout(() => startMexcWs(sym), WS_RECONNECT_DELAY_MS);
    }

    ws.on('open', () => {
        logLine(`CONNECTED MEXC | symbol=${sym}`);
        ws.send(JSON.stringify({ method: 'sub.ticker', param: { symbol: sym } }));
        ws.send(JSON.stringify({ method: 'sub.depth.full', param: { symbol: sym, limit: MEXC_WS_DEPTH_LIMIT } }));
        ping = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'ping' })); }, 15000);
    });

    ws.on('message', raw => {
        let d; try { d = JSON.parse(raw); } catch { return; }
        if (!d) return;
        if (d.data?.bid1 !== undefined) {
            S.mexcBid[sym] = Number(d.data.bid1);
            S.mexcAsk[sym] = Number(d.data.ask1);
            S.mexcQuoteTs[sym] = Date.now();
        }
        if (d.channel === 'push.depth.full' && d.symbol === sym && d.data) {
            const bids = d.data.bids, asks = d.data.asks;
            if (Array.isArray(bids) && bids[0]?.[1] !== undefined) {
                const q = Number(bids[0][1]); S.mexcBidQty[sym] = q > 0 ? q : null;
            }
            if (Array.isArray(asks) && asks[0]?.[1] !== undefined) {
                const q = Number(asks[0][1]); S.mexcAskQty[sym] = q > 0 ? q : null;
            }
            S.mexcDepthTs[sym] = Date.now();
        }
    });

    ws.on('close', () => { reconnect('close'); });
    ws.on('error', e => { logLine(`MEXC error | symbol=${sym} | msg=${e.message}`); reconnect('error'); });
}

// ======================== BINANCE WS ========================
const binToMexc = {};
for (const sym of SYMBOLS) binToMexc[mexcToBin(sym)] = sym;

function startBinanceWs() {
    let done = false;
    const streams = SYMBOLS.map(s => mexcToBin(s).toLowerCase() + '@bookTicker');
    const ws = new WebSocket('wss://fstream.binance.com/stream?streams=' + streams.join('/'));

    function clearBin() {
        for (const s of SYMBOLS) {
            S.binBid[s] = null; S.binAsk[s] = null;
            S.binBidQty[s] = null; S.binAskQty[s] = null;
            S.binQuoteTs[s] = null; S.prevBinBid[s] = null;
        }
    }
    function reconnect(reason) {
        if (done) return; done = true;
        clearBin();
        logLine(`BINANCE reconnect scheduled | reason=${reason} | in=${(WS_RECONNECT_DELAY_MS / 1000).toFixed(1)}s`);
        setTimeout(startBinanceWs, WS_RECONNECT_DELAY_MS);
    }

    ws.on('open', () => logLine('CONNECTED BINANCE'));

    ws.on('message', raw => {
        let outer; try { outer = JSON.parse(raw); } catch { return; }
        if (!outer) return;
        const d = outer.data || outer;
        if (!d || d.b === undefined) return;
        const bs = String(d.s || '');
        if (!bs || !binToMexc[bs]) return;
        const sym = binToMexc[bs];
        S.binBid[sym] = Number(d.b);
        S.binAsk[sym] = Number(d.a);
        if (d.B !== undefined) S.binBidQty[sym] = Number(d.B);
        if (d.A !== undefined) S.binAskQty[sym] = Number(d.A);
        S.binQuoteTs[sym] = Date.now();
    });

    ws.on('close', () => { reconnect('close'); });
    ws.on('error', e => { logLine(`BINANCE error | msg=${e.message}`); reconnect('error'); });
}

// ======================== HELPERS for state clear ========================
function clearPending() {
    S.pending = null; S.pendOid = null; S.pendSince = null;
    S.pendEntry = null; S.pendSym = null;
}
function clearPosition() {
    S.position = null; S.posSym = null; S.entryPrice = null;
    S.posHoldVol = null; S.dryHoldVol = null;
    S.openOid = null; S.closeOid = null;
    clearPending();
}
function recordClosedTrade(pnlUsd) {
    if (!Number.isFinite(pnlUsd)) return;
    S.tradeHistory.push({ ts: Date.now() / 1000, pnlUsd });
    if (S.tradeHistory.length > 5000) S.tradeHistory.shift();
    if (pnlUsd >= 0) S.wins++;
    else S.losses++;
}

// ======================== MAIN TICK ========================
async function mainTick() {
    const now = Date.now();

    if (now - S.balTs >= BALANCE_FETCH_INTERVAL_MS) {
        try { await fetchBalance(); } catch (e) { logLine('BALANCE FETCH FAILED | msg=' + e.message); }
    }

    for (const s of SYMBOLS) {
        if (!S.detail[s]) S.detail[s] = await fetchContractDetail(s);
    }

    let trigLC = false, trigSC = false, justClosed = false;

    // ======== PENDING ORDER POLL ========
    if (NEED_ORDERS && S.pending && S.pendOid && S.pendSym) {
        const sp = S.pendSym;
        let oi, pi;
        try { oi = await getOrderById(S.pendOid); } catch (e) { oi = { success: false, code: -1, msg: e.message }; }
        try { pi = await getOpenPositions(sp); } catch (e) { pi = { success: false, code: -1, msg: e.message }; }

        const det = S.detail[sp];
        let csPoll = 1;
        if (det) { const c = Number(det.contractSize ?? 1); csPoll = c > 0 ? c : 1; }
        const mbP = S.mexcBid[sp], maP = S.mexcAsk[sp];

        if (isOk(oi)) {
            const od = oi.data || {};
            const os = Number(od.state ?? 0);
            const dap = Number(od.dealAvgPrice ?? 0);
            const dv = Number(od.dealVol ?? 0);

            // OPEN LONG FILLED
            if (S.pending === 'open_long' && os === 3) {
                const mp = findPosition(pi, sp, 'long');
                if (mp || dv > 0) {
                    S.position = 'long'; S.posSym = sp;
                    S.entryPrice = mp ? Number(mp.holdAvgPrice || S.pendEntry || mbP || 0)
                        : (dap > 0 ? dap : (S.pendEntry || mbP || 0));
                    S.posHoldVol = mp ? Number(mp.holdVol || dv) : dv;
                    if (S.posHoldVol <= 0 && dv > 0) S.posHoldVol = dv;
                    logTradeOpened('long', sp, S.entryPrice, S.posHoldVol, S.pendOid);
                    clearPending(); S.closeOid = null;
                }
            }
            // OPEN SHORT FILLED
            else if (S.pending === 'open_short' && os === 3) {
                const mp = findPosition(pi, sp, 'short');
                if (mp || dv > 0) {
                    S.position = 'short'; S.posSym = sp;
                    S.entryPrice = mp ? Number(mp.holdAvgPrice || S.pendEntry || maP || 0)
                        : (dap > 0 ? dap : (S.pendEntry || maP || 0));
                    S.posHoldVol = mp ? Number(mp.holdVol || dv) : dv;
                    if (S.posHoldVol <= 0 && dv > 0) S.posHoldVol = dv;
                    logTradeOpened('short', sp, S.entryPrice, S.posHoldVol, S.pendOid);
                    clearPending(); S.closeOid = null;
                }
            }
            // CLOSE LONG FILLED
            else if (S.pending === 'close_long' && os === 3) {
                const cr = closeRef('long', mbP, maP) || 0;
                const exitP = dap > 0 ? dap : cr;
                const prevV = S.posHoldVol > 0 ? S.posHoldVol : 0;
                const remPos = findPosition(pi, sp, 'long');
                const exRemV = remPos ? Number(remPos.holdVol ?? 0) : 0;
                let remV;
                if (dv > 0 && prevV > 0) { remV = Math.max(0, prevV - dv); if (exRemV > 0) remV = Math.min(remV, exRemV); }
                else remV = exRemV;
                let hv = dv > 0 ? dv : (remV > 0 && prevV > remV ? Math.max(0, prevV - remV) : prevV);
                if (hv <= 0 && remV <= 0 && prevV > 0) hv = prevV;
                const exPnl = extractExchangePnl(od, pi, sp, 'long');
                const profit = exPnl !== null ? exPnl : pnlLinear(S.entryPrice, exitP, hv, csPoll, 'long');
                S.totalPnl += profit;
                S.pnlBy[sp] = (S.pnlBy[sp] || 0) + profit;

                if (remV > 0) {
                    logEvent(`CLOSE_LONG_PARTIAL price=${exitP.toFixed(5)} volume_usdt=${notionalUsdt(hv, exitP, csPoll).toFixed(2)} remaining_usdt=${notionalUsdt(remV, cr > 0 ? cr : exitP, csPoll).toFixed(2)} pnl=${profit.toFixed(4)}USDT`);
                    S.position = 'long'; S.posSym = sp;
                    if (remPos && Number(remPos.holdAvgPrice ?? 0) > 0) S.entryPrice = Number(remPos.holdAvgPrice);
                    S.posHoldVol = remV;
                    clearPending(); S.openOid = null; S.closeOid = null;
                    const rLev = resolveLevForClose(remPos || { leverage: LEVERAGE }, det || {}, LEVERAGE);
                    const rr = await createMarketOrder(sp, remV, 4, rLev);
                    if (isOk(rr)) {
                        S.closeOid = extractOid(rr); S.pending = 'close_long';
                        S.pendOid = S.closeOid; S.pendSince = Date.now(); S.pendSym = sp;
                        logLine(`CLOSE LONG SENT (retry) | symbol=${sp} | orderId=${S.closeOid ?? 'NULL'} | vol=${formatVol(remV)}`);
                    } else { logLine(`CLOSE LONG FAILED (retry) | symbol=${sp} | ${'code=' + respCode(rr) + ' | msg=' + respMsg(rr)}`); trigLC = true; }
                } else {
                    S.trades++;
                    recordClosedTrade(profit);
                    logTradeClosed('long', sp, S.entryPrice, exitP, hv, profit, S.totalPnl);
                    clearPosition();
                }
            }
            // CLOSE SHORT FILLED
            else if (S.pending === 'close_short' && os === 3) {
                const cr = closeRef('short', mbP, maP) || 0;
                const exitP = dap > 0 ? dap : cr;
                const prevV = S.posHoldVol > 0 ? S.posHoldVol : 0;
                const remPos = findPosition(pi, sp, 'short');
                const exRemV = remPos ? Number(remPos.holdVol ?? 0) : 0;
                let remV;
                if (dv > 0 && prevV > 0) { remV = Math.max(0, prevV - dv); if (exRemV > 0) remV = Math.min(remV, exRemV); }
                else remV = exRemV;
                let hv = dv > 0 ? dv : (remV > 0 && prevV > remV ? Math.max(0, prevV - remV) : prevV);
                if (hv <= 0 && remV <= 0 && prevV > 0) hv = prevV;
                const exPnl = extractExchangePnl(od, pi, sp, 'short');
                const profit = exPnl !== null ? exPnl : pnlLinear(S.entryPrice, exitP, hv, csPoll, 'short');
                S.totalPnl += profit;
                S.pnlBy[sp] = (S.pnlBy[sp] || 0) + profit;

                if (remV > 0) {
                    logEvent(`CLOSE_SHORT_PARTIAL price=${exitP.toFixed(5)} volume_usdt=${notionalUsdt(hv, exitP, csPoll).toFixed(2)} remaining_usdt=${notionalUsdt(remV, cr > 0 ? cr : exitP, csPoll).toFixed(2)} pnl=${profit.toFixed(4)}USDT`);
                    S.position = 'short'; S.posSym = sp;
                    if (remPos && Number(remPos.holdAvgPrice ?? 0) > 0) S.entryPrice = Number(remPos.holdAvgPrice);
                    S.posHoldVol = remV;
                    clearPending(); S.openOid = null; S.closeOid = null;
                    const rLev = resolveLevForClose(remPos || { leverage: LEVERAGE }, det || {}, LEVERAGE);
                    const rr = await createMarketOrder(sp, remV, 2, rLev);
                    if (isOk(rr)) {
                        S.closeOid = extractOid(rr); S.pending = 'close_short';
                        S.pendOid = S.closeOid; S.pendSince = Date.now(); S.pendSym = sp;
                        logLine(`CLOSE SHORT SENT (retry) | symbol=${sp} | orderId=${S.closeOid ?? 'NULL'} | vol=${formatVol(remV)}`);
                    } else { logLine(`CLOSE SHORT FAILED (retry) | symbol=${sp} | ${'code=' + respCode(rr) + ' | msg=' + respMsg(rr)}`); trigSC = true; }
                } else {
                    S.trades++;
                    recordClosedTrade(profit);
                    logTradeClosed('short', sp, S.entryPrice, exitP, hv, profit, S.totalPnl);
                    clearPosition();
                }
            }

            // ORDER FAILED (state 4 or 5)
            if (S.pending && (os === 4 || os === 5)) {
                const fa = S.pending;
                logLine(`ORDER FAILED/CANCELED | action=${fa} | orderId=${S.pendOid ?? 'NULL'} | state=${os}`);
                if (fa === 'open_long' || fa === 'open_short') S.openOid = null;
                if (fa === 'close_long' || fa === 'close_short') {
                    S.closeOid = null;
                    const cd = fa === 'close_long' ? 'long' : 'short';
                    const rp = findPosition(pi, sp, cd);
                    if (rp) {
                        S.position = cd; S.posSym = sp;
                        if (Number(rp.holdAvgPrice ?? 0) > 0) S.entryPrice = Number(rp.holdAvgPrice);
                        S.posHoldVol = Number(rp.holdVol ?? S.posHoldVol ?? 0);
                    }
                    if (cd === 'long') trigLC = true; else trigSC = true;
                }
                clearPending();
            }
        }

        // ORDER TIMEOUT
        if (S.pending && S.pendSince && (now - S.pendSince) > PENDING_ORDER_TIMEOUT_MS) {
            const ta = S.pending;
            logLine(`ORDER CONFIRM TIMEOUT | action=${ta} | orderId=${S.pendOid ?? 'NULL'}`);
            if (ta === 'open_long' || ta === 'open_short') S.openOid = null;
            if (ta === 'close_long' || ta === 'close_short') {
                S.closeOid = null;
                const cd = ta === 'close_long' ? 'long' : 'short';
                const rp = findPosition(pi, sp, cd);
                if (rp) {
                    S.position = cd; S.posSym = sp;
                    if (Number(rp.holdAvgPrice ?? 0) > 0) S.entryPrice = Number(rp.holdAvgPrice);
                    S.posHoldVol = Number(rp.holdVol ?? S.posHoldVol ?? 0);
                }
                if (cd === 'long') trigLC = true; else trigSC = true;
            }
            clearPending();
        }
    }

    // ======== RESOLVE PRICES FOR POSITION SYMBOL ========
    let mexcBid = null, mexcAsk = null, binBid = null, binAsk = null;
    let tickSz = null, det = null, cs = 1, eLev = LEVERAGE;

    if (S.position && S.posSym) {
        const sp = S.posSym;
        mexcBid = S.mexcBid[sp]; mexcAsk = S.mexcAsk[sp];
        binBid = S.binBid[sp]; binAsk = S.binAsk[sp];
        tickSz = S.tickSize[sp]; det = S.detail[sp];
        if (det) {
            const c = Number(det.contractSize ?? 1); cs = c > 0 ? c : 1;
            const mx = Number(det.maxLeverage ?? LEVERAGE);
            eLev = Math.min(Math.max(1, LEVERAGE), Math.max(1, mx));
        }
    }

    const dispSym = S.posSym || SYMBOLS[0];
    const dmb = S.mexcBid[dispSym], dma = S.mexcAsk[dispSym];
    const dbb = S.binBid[dispSym], dba = S.binAsk[dispSym];
    let spread = 0;
    if (dmb != null && dma != null && dma > 0 && dbb != null && dba != null) {
        spread = ((dbb - dma) / dma) * 100;
    }

    let binMove = null, posFresh = false;
    if (S.position && S.posSym) {
        const sp = S.posSym;
        const pb = S.prevBinBid[sp];
        if (pb !== null && binBid !== null) binMove = binBid - pb;
        posFresh = wsFresh(S.mexcQuoteTs[sp], now, MEXC_WS_STALE_MS) && wsFresh(S.binQuoteTs[sp], now, BINANCE_WS_STALE_MS);
    }

    let closeTh = null;
    if (S.position && S.posSym && tickSz !== null) closeTh = tickSz * CLOSE_REVERSE_STEPS;

    // ======== CLOSE SIGNAL DETECTION ========
    if (S.position === 'long') {
        if (posFresh && closeTh !== null && binMove !== null && mexcBid !== null
            && binMove <= (-closeTh + 1e-12)) trigLC = true;
    }
    if (S.position === 'short') {
        if (posFresh && closeTh !== null && binMove !== null && mexcAsk !== null
            && binMove >= (closeTh - 1e-12)) trigSC = true;
    }

    // ======== EXECUTE CLOSE LONG ========
    if (trigLC && S.position === 'long' && !S.pending) {
        if (!NEED_ORDERS) {
            const ep = closeRef('long', mexcBid, mexcAsk) || 0;
            const hv = S.posHoldVol > 0 ? S.posHoldVol : (S.dryHoldVol || 0);
            const pr = pnlLinear(S.entryPrice, ep, hv, cs, 'long');
            S.totalPnl += pr; if (S.posSym) S.pnlBy[S.posSym] = (S.pnlBy[S.posSym] || 0) + pr;
            S.trades++;
            recordClosedTrade(pr);
            logTradeClosed('long', S.posSym, S.entryPrice, ep, hv, pr, S.totalPnl);
            clearPosition(); justClosed = true;
        } else {
            if (!S.posHoldVol || S.posHoldVol <= 0) { logLine('CLOSE LONG SKIP | reason=noVol'); }
            else {
                const cv = S.posHoldVol;
                const clp = closeRef('long', mexcBid, mexcAsk) || (mexcBid || mexcAsk || 0);
                const cr = await createMarketOrder(S.posSym, cv, 4, eLev);
                if (isOk(cr)) {
                    S.closeOid = extractOid(cr); S.pending = 'close_long';
                    S.pendOid = S.closeOid; S.pendSym = S.posSym; S.pendSince = Date.now();
                    logLine(`CLOSE LONG SENT | symbol=${S.posSym} | orderId=${S.closeOid ?? 'NULL'} | vol=${formatVol(cv)}`);
                } else { logLine(`CLOSE LONG FAILED | symbol=${S.posSym} | ${'code=' + respCode(cr) + ' | msg=' + respMsg(cr)}`); }
            }
        }
    }

    // ======== EXECUTE CLOSE SHORT ========
    if (trigSC && S.position === 'short' && !S.pending) {
        if (!NEED_ORDERS) {
            const ep = closeRef('short', mexcBid, mexcAsk) || 0;
            const hv = S.posHoldVol > 0 ? S.posHoldVol : (S.dryHoldVol || 0);
            const pr = pnlLinear(S.entryPrice, ep, hv, cs, 'short');
            S.totalPnl += pr; if (S.posSym) S.pnlBy[S.posSym] = (S.pnlBy[S.posSym] || 0) + pr;
            S.trades++;
            recordClosedTrade(pr);
            logTradeClosed('short', S.posSym, S.entryPrice, ep, hv, pr, S.totalPnl);
            clearPosition(); justClosed = true;
        } else {
            if (!S.posHoldVol || S.posHoldVol <= 0) { logLine('CLOSE SHORT SKIP | reason=noVol'); }
            else {
                const cv = S.posHoldVol;
                const clp = closeRef('short', mexcBid, mexcAsk) || (mexcAsk || mexcBid || 0);
                const cr = await createMarketOrder(S.posSym, cv, 2, eLev);
                if (isOk(cr)) {
                    S.closeOid = extractOid(cr); S.pending = 'close_short';
                    S.pendOid = S.closeOid; S.pendSym = S.posSym; S.pendSince = Date.now();
                    logLine(`CLOSE SHORT SENT | symbol=${S.posSym} | orderId=${S.closeOid ?? 'NULL'} | vol=${formatVol(cv)}`);
                } else { logLine(`CLOSE SHORT FAILED | symbol=${S.posSym} | ${'code=' + respCode(cr) + ' | msg=' + respMsg(cr)}`); }
            }
        }
    }

    // ======== ENTRY SCAN ========
    if (!justClosed && S.position === null && !S.pending) {
        for (const sym of SYMBOLS) {
            const mb = S.mexcBid[sym], ma = S.mexcAsk[sym];
            const mbQ = S.mexcBidQty[sym], maQ = S.mexcAskQty[sym];
            const bb = S.binBid[sym], ba = S.binAsk[sym];
            const mxF = wsFresh(S.mexcQuoteTs[sym], now, MEXC_WS_STALE_MS);
            const bnF = wsFresh(S.binQuoteTs[sym], now, BINANCE_WS_STALE_MS);
            if (!bnF) S.prevBinBid[sym] = null;
            if (!mxF || !bnF) continue;
            if (mb == null || ma == null || bb == null || ba == null || ma <= 0) continue;
            const tick = S.tickSize[sym];
            if (tick == null) continue;
            const pb = S.prevBinBid[sym];
            if (pb === null) continue;

            const binMoveE = bb - pb;
            const entryTh = tick * ENTRY_SPREAD_STEPS;
            const longGap = (ba - ma) >= (entryTh - 1e-12);
            const shortGap = (mb - bb) >= (entryTh - 1e-12);

            const detE = S.detail[sym];
            let csE = 1, eLevE = LEVERAGE;
            if (detE) {
                const c = Number(detE.contractSize ?? 1); csE = c > 0 ? c : 1;
                const mx = Number(detE.maxLeverage ?? LEVERAGE);
                eLevE = Math.min(Math.max(1, LEVERAGE), Math.max(1, mx));
            }
            const fp = feePolicy(detE);
            if (fp.blockOpenOrders) {
                criticalExit(`FREE COMMISSION LOST sym=${sym} maker=${fp.makerFeeRate ?? 'n/a'} taker=${fp.takerFeeRate ?? 'n/a'} — zero-fee status removed, stopping immediately`);
            }
            S.feeLogged[sym] = false;

            // ---- LONG ENTRY ----
            if (binMoveE >= (tick - 1e-12) && longGap) {
                if (!NEED_ORDERS) {
                    if (detE && S.bal > 0) {
                        const cap = wsCapQty(1, mbQ, maQ);
                        const ov = computeVol(S.bal, detE, ma, cap, eLevE);
                        if (ov != null && ov > 0) {
                            S.position = 'long'; S.posSym = sym; S.entryPrice = mb;
                            S.dryHoldVol = ov; S.posHoldVol = ov;
                            S.openOid = 'DRY-L-' + Date.now();
                            logTradeOpened('long', sym, mb, ov, S.openOid);
                        }
                    }
                } else if (!detE || !S.bal || S.bal <= 0) {
                    logLine(`OPEN LONG SKIP | symbol=${sym} | reason=noContractOrBalance`);
                } else {
                    const cap = wsCapQty(1, mbQ, maQ);
                    const ov = computeVol(S.bal, detE, ma, cap, eLevE);
                    if (!ov || ov <= 0) { logLine(`OPEN LONG SKIP | symbol=${sym} | reason=vol0`); }
                    else {
                        const lp = ma + tick;
                        const att = await tryOpenOptLimit(USE_LIMIT_ORDER, LIMIT_WAIT_MS, sym, ov, 1, eLevE, lp);
                        const or = att.create;
                        if (isOk(or)) {
                            S.openOid = att.orderId;
                            logLine(`OPEN LONG SENT | symbol=${sym} | orderId=${S.openOid ?? 'NULL'} | vol=${formatVol(ov)}`);
                            if (USE_LIMIT_ORDER) {
                                if (att.filled) {
                                    S.position = 'long'; S.posSym = sym;
                                    S.entryPrice = (att.orderInfo?.data?.dealAvgPrice > 0) ? Number(att.orderInfo.data.dealAvgPrice) : mb;
                                    S.posHoldVol = att.orderInfo?.data?.dealVol ? Number(att.orderInfo.data.dealVol) : ov;
                                    logTradeOpened('long', sym, S.entryPrice, S.posHoldVol, S.openOid);
                                } else {
                                    if (att.cancelResponse) logLine(`OPEN LONG CANCELED | symbol=${sym} | ${'code=' + respCode(att.cancelResponse) + ' | msg=' + respMsg(att.cancelResponse)}`);
                                    S.openOid = null;
                                }
                            } else {
                                S.pending = 'open_long'; S.pendOid = S.openOid;
                                S.pendSym = sym; S.pendSince = Date.now(); S.pendEntry = mb;
                            }
                        } else {
                            logLine(`OPEN LONG FAILED | symbol=${sym} | ${'code=' + respCode(or) + ' | msg=' + respMsg(or)}`);
                            if (isCriticalOpenError(or)) criticalExit(`RISK CONTROL (LONG) sym=${sym} code=${respCode(or)} msg=${respMsg(or)}`);
                        }
                    }
                }
                if (S.position !== null || S.pending) break;
            }

            // ---- SHORT ENTRY ----
            if (binMoveE <= (-tick + 1e-12) && shortGap) {
                if (!NEED_ORDERS) {
                    if (detE && S.bal > 0) {
                        const cap = wsCapQty(3, mbQ, maQ);
                        const ov = computeVol(S.bal, detE, mb, cap, eLevE);
                        if (ov != null && ov > 0) {
                            S.position = 'short'; S.posSym = sym; S.entryPrice = ma;
                            S.dryHoldVol = ov; S.posHoldVol = ov;
                            S.openOid = 'DRY-S-' + Date.now();
                            logTradeOpened('short', sym, ma, ov, S.openOid);
                        }
                    }
                } else if (!detE || !S.bal || S.bal <= 0) {
                    logLine(`OPEN SHORT SKIP | symbol=${sym} | reason=noContractOrBalance`);
                } else {
                    const cap = wsCapQty(3, mbQ, maQ);
                    const ov = computeVol(S.bal, detE, mb, cap, eLevE);
                    if (!ov || ov <= 0) { logLine(`OPEN SHORT SKIP | symbol=${sym} | reason=vol0`); }
                    else {
                        const lp = mb - tick;
                        const att = await tryOpenOptLimit(USE_LIMIT_ORDER, LIMIT_WAIT_MS, sym, ov, 3, eLevE, lp);
                        const or = att.create;
                        if (isOk(or)) {
                            S.openOid = att.orderId;
                            logLine(`OPEN SHORT SENT | symbol=${sym} | orderId=${S.openOid ?? 'NULL'} | vol=${formatVol(ov)}`);
                            if (USE_LIMIT_ORDER) {
                                if (att.filled) {
                                    S.position = 'short'; S.posSym = sym;
                                    S.entryPrice = (att.orderInfo?.data?.dealAvgPrice > 0) ? Number(att.orderInfo.data.dealAvgPrice) : ma;
                                    S.posHoldVol = att.orderInfo?.data?.dealVol ? Number(att.orderInfo.data.dealVol) : ov;
                                    logTradeOpened('short', sym, S.entryPrice, S.posHoldVol, S.openOid);
                                } else {
                                    if (att.cancelResponse) logLine(`OPEN SHORT CANCELED | symbol=${sym} | ${'code=' + respCode(att.cancelResponse) + ' | msg=' + respMsg(att.cancelResponse)}`);
                                    S.openOid = null;
                                }
                            } else {
                                S.pending = 'open_short'; S.pendOid = S.openOid;
                                S.pendSym = sym; S.pendSince = Date.now(); S.pendEntry = ma;
                            }
                        } else {
                            logLine(`OPEN SHORT FAILED | symbol=${sym} | code=${respCode(or)} | msg=${respMsg(or)}`);
                            if (isCriticalOpenError(or)) criticalExit(`RISK CONTROL (SHORT) sym=${sym} code=${respCode(or)} msg=${respMsg(or)}`);
                        }
                    }
                }
                if (S.position !== null || S.pending) break;
            }
        }
    }

    // ======== STATUS ========
    if ((now - UI.lastDashboardAt) >= DASHBOARD_REFRESH_MS) {
        const posTag = S.position === null ? 'flat' : `${S.posSym || ''}/${S.position}`;
        const entryDisp = S.position && S.entryPrice != null ? formatPrice(S.entryPrice) : 'n/a';
        const currentPx = S.position === 'long'
            ? formatPrice(mexcBid)
            : S.position === 'short'
                ? formatPrice(mexcAsk)
                : 'n/a';
        const currentRef = S.position === 'long'
            ? mexcBid
            : S.position === 'short'
                ? mexcAsk
                : null;
        const holdVol = S.position && Number.isFinite(Number(S.posHoldVol)) && Number(S.posHoldVol) > 0
            ? Number(S.posHoldVol)
            : S.position && Number.isFinite(Number(S.dryHoldVol)) && Number(S.dryHoldVol) > 0
                ? Number(S.dryHoldVol)
                : null;
        const unrealized = S.position && holdVol !== null && S.entryPrice != null && currentRef != null
            ? pnlLinear(S.entryPrice, currentRef, holdVol, cs, S.position)
            : null;
        const recentPnlWindows = summarizeRecentPnlUsd(S.tradeHistory, now / 1000);
        const recentWinLoss = summarizeRecentWinLoss(S.tradeHistory, now / 1000);
        const tsDash = S.tickSize[dispSym];
        renderDashboard({
            symbol: dispSym,
            position: posTag,
            pending: S.pending || 'none',
            balance: S.bal !== null ? S.bal.toFixed(4) + ' USDT' : 'n/a',
            totalPnl: S.totalPnl.toFixed(2) + ' USDT',
            entry: entryDisp,
            current: currentPx,
            volume: holdVol !== null ? formatVol(holdVol) : 'n/a',
            unrealized: unrealized !== null ? formatSignedUsd(unrealized) : 'n/a',
            mexcBid: formatPrice(dmb),
            mexcAsk: formatPrice(dma),
            binBid: formatPrice(dbb),
            binAsk: formatPrice(dba),
            spread: spread.toFixed(4) + '%',
            binanceTick: tsDash != null
                ? `${tsDash} (= шаг цены Binance USDT-M; entrySteps×tick / closeSteps×tick)`
                : 'n/a',
            pnl: {
                '10m': formatSignedUsd(recentPnlWindows['10m']),
                '30m': formatSignedUsd(recentPnlWindows['30m']),
                '1h': formatSignedUsd(recentPnlWindows['1h']),
                '3h': formatSignedUsd(recentPnlWindows['3h']),
                '5h': formatSignedUsd(recentPnlWindows['5h']),
            },
            wl: {
                '10m': `${recentWinLoss['10m'].win}/${recentWinLoss['10m'].loss}`,
                '30m': `${recentWinLoss['30m'].win}/${recentWinLoss['30m'].loss}`,
                '1h': `${recentWinLoss['1h'].win}/${recentWinLoss['1h'].loss}`,
                '3h': `${recentWinLoss['3h'].win}/${recentWinLoss['3h'].loss}`,
                '5h': `${recentWinLoss['5h'].win}/${recentWinLoss['5h'].loss}`,
            },
        });
        UI.lastDashboardAt = now;
    }

    // ======== UPDATE PREV BIN BID ========
    for (const s of SYMBOLS) {
        const b = S.binBid[s];
        if (b !== null && wsFresh(S.binQuoteTs[s], now, BINANCE_WS_STALE_MS)) S.prevBinBid[s] = b;
        else S.prevBinBid[s] = null;
    }
}

// ======================== SHUTDOWN ========================
let shutdownDone = false;
async function shutdown(reason) {
    if (shutdownDone) return;
    shutdownDone = true;
    console.error(tsPrefix() + 'stop ' + reason);

    if (NEED_ORDERS) {
        logLine('SHUTDOWN closing positions');
        for (let r = 0; r < 12; r++) {
            let found = false;
            for (const sym of SYMBOLS) {
                if (!S.detail[sym]) S.detail[sym] = await fetchContractDetail(sym);
                const det = S.detail[sym];
                const vs = det ? Number(det.volScale ?? 0) : 0;
                const posResp = await getOpenPositions(sym);
                const cl = listClosable(isOk(posResp) ? posResp : {}, sym);
                for (const p of cl) {
                    found = true;
                    const hv = adjVol(Number(p.holdVol ?? 0), vs);
                    if (hv <= 0) continue;
                    const pt = Number(p.positionType ?? 0);
                    const side = pt === 1 ? 4 : 2;
                    const lev = resolveLevForClose(p, det || {}, LEVERAGE);
                    logLine(`SHUTDOWN close sym=${sym} side=${side} v=${hv} lev=${lev}`);
                    const cr = await createMarketOrder(sym, hv, side, lev);
                    if (!isOk(cr)) logLine('SHUTDOWN close_err ' + JSON.stringify(cr));
                    await sleep(400);
                    try {
                        const vr = await getOpenPositions(sym);
                        const remHv = (isOk(vr) && Array.isArray(vr.data) ? vr.data : [])
                            .filter(pp => pp && pp.symbol === sym && Number(pp.positionType ?? 0) === pt && Number(pp.holdVol ?? 0) > 0)
                            .reduce((mx, pp) => Math.max(mx, Number(pp.holdVol ?? 0)), 0);
                        if (remHv <= 0) logLine(`SHUTDOWN verify closed sym=${sym} pos=${pt}`);
                        else logLine(`SHUTDOWN verify still_open sym=${sym} pos=${pt} holdVol=${remHv}`);
                    } catch {}
                }
            }
            if (!found) { logLine('SHUTDOWN flat'); break; }
            await sleep(400);
        }
    } else {
        logLine('SHUTDOWN close skip dryrun');
    }

    process.exit(0);
}

// ======================== MAIN ========================
async function main() {
    for (const sym of SYMBOLS) {
        S.tickSize[sym] = await getBinanceTickSize(mexcToBin(sym));
        S.detail[sym] = await fetchContractDetail(sym);
    }

    logLine(`START | symbols=${SYMBOLS.join(',')} | onePos=yes | marginMode=${MARGIN_MODE} | openType=${OPEN_TYPE} | maxVolContracts=${MAX_ORDER_VOL_CONTRACTS || 'off'} | maxNotionalUsdt=${MAX_ORDER_NOTIONAL_USDT || 'off'} | entrySteps=${ENTRY_SPREAD_STEPS} | closeSteps=${CLOSE_REVERSE_STEPS} | tick=${MAIN_TICK_MS}ms`);

    for (const sym of SYMBOLS) {
        const ts = S.tickSize[sym];
        logLine(`SYMBOL READY | symbol=${sym} | binance=${mexcToBin(sym)} | tickSize=${ts !== null ? String(ts) : 'NOT LOADED'}`);
        const det = S.detail[sym];
        if (det) {
            const emv = effMaxVol(det);
            const fp = feePolicy(det);
            logLine(`MEXC DETAIL | symbol=${sym} | contractSize=${det.contractSize ?? '?'} | volScale=${det.volScale ?? '?'} | effMaxVol=${emv > 0 ? emv : '-'} | makerFee=${fp.makerFeeRate ?? 'n/a'} | takerFee=${fp.takerFeeRate ?? 'n/a'} | tradable=${fp.blockOpenOrders ? 'no' : 'yes'}`);
        } else {
            logLine(`MEXC DETAIL | symbol=${sym} | status=NOT LOADED`);
        }
    }

    await fetchBalance();

    for (const sym of SYMBOLS) startMexcWs(sym);
    startBinanceWs();

    setInterval(() => {
        fetchBalance().catch(() => {});
        emitStats('timer');
    }, STATS_INTERVAL_MS);

    if (process.stdin.isTTY) {
        process.stdin.setRawMode(true);
        process.stdin.resume();
        process.stdin.on('data', buf => {
            const s = buf.toString();
            if (s.includes('t') || s.includes('T')) emitStats('key:T');
        });
    }

    let busy = false;
    setInterval(async () => {
        if (busy) return;
        busy = true;
        try { await mainTick(); } catch (e) { logLine('tick_err: ' + (e.stack || e.message || e)); }
        finally { busy = false; }
    }, MAIN_TICK_MS);

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
