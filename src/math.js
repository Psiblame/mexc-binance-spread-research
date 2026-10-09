'use strict';
// Derived from reviewed Flip.js.bak fee, contract sizing and PnL helpers.
const ENFORCE_ZERO_FEE_TICKERS = true;
function feePolicy(det) {
    const maker = det?.makerFeeRate ?? null;
    const taker = det?.takerFeeRate ?? null;
    const knownRate = value => {
      if (!(typeof value === 'number' || (typeof value === 'string' && value.trim() !== ''))) return null;
      const number = Number(value);
      return Number.isFinite(number) ? number : null;
    };
    const makerN = knownRate(maker);
    const takerN = knownRate(taker);
    const bothKnown = makerN !== null && takerN !== null;
    const hasPos = (makerN || 0) > 0 || (takerN || 0) > 0;
    return {
        makerFeeRate: makerN, takerFeeRate: takerN,
        blockOpenOrders: ENFORCE_ZERO_FEE_TICKERS && !(bothKnown && !hasPos),
    };
}

// ======================== VOLUME ========================
function originalAdjVol(vol, volScale) {
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
function notionalUsdt(vol, price, cs) {
    if (![vol, price, cs].every(Number.isFinite) || vol < 0 || price <= 0 || cs <= 0) throw new RangeError('Invalid contract notional inputs');
    return vol > 0 && price > 0 && cs > 0 ? vol * price * cs : 0;
}

function pnlLinear(entry, exit, holdVol, cs, dir) {
    if (![entry, exit, holdVol, cs].every(Number.isFinite) || entry <= 0 || exit <= 0 || holdVol < 0 || cs <= 0 || !['long', 'short'].includes(dir)) throw new RangeError('Invalid contract prices, volume or direction');
    if (holdVol <= 0 || cs <= 0) return 0;
    return dir === 'long' ? (exit - entry) * holdVol * cs : (entry - exit) * holdVol * cs;
}

function adjVol(vol, scale) {
  if (!Number.isFinite(vol) || !Number.isInteger(scale) || scale < 0 || scale > 12) throw new RangeError('Invalid volume or scale');
  return originalAdjVol(vol, scale);
}
function wsFresh(ts, now, maxAge) {
  return Number.isFinite(ts) && Number.isFinite(now) && Number.isFinite(maxAge) && maxAge >= 0 && ts <= now && now - ts <= maxAge;
}
module.exports = { feePolicy, adjVol, effMaxVol, notionalUsdt, pnlLinear, wsFresh };
