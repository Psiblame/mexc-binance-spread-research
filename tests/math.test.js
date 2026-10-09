const test = require('node:test');
const assert = require('node:assert/strict');
const m = require('../src/math');
test('unknown or positive fees block entry', () => {
  assert.equal(m.feePolicy({}).blockOpenOrders, true);
  assert.equal(m.feePolicy({makerFeeRate: 0, takerFeeRate: 'invalid'}).blockOpenOrders, true);
  assert.equal(m.feePolicy({makerFeeRate: 0, takerFeeRate: 0.0001}).blockOpenOrders, true);
  assert.equal(m.feePolicy({makerFeeRate: 0, takerFeeRate: 0}).blockOpenOrders, false);
  assert.equal(m.feePolicy({makerFeeRate: '0', takerFeeRate: '0'}).blockOpenOrders, false);
  assert.equal(m.feePolicy({makerFeeRate: -0.0001, takerFeeRate: 0}).blockOpenOrders, false);
  for (const invalid of ['', ' ', false, []]) {
    assert.equal(m.feePolicy({makerFeeRate: 0, takerFeeRate: invalid}).blockOpenOrders, true);
  }
});
test('contract quantity truncates without increasing exposure', () => {
  assert.equal(m.adjVol(12.3456, 2), 12.34);
  assert.equal(m.adjVol(-1, 2), 0);
  assert.throws(() => m.adjVol(NaN, 2));
  assert.throws(() => m.adjVol(1, -1));
});
test('exchange quantity limits use the lower positive bound', () => {
  assert.equal(m.effMaxVol({maxVol: 2000, limitMaxVol: 1500}), 1500);
  assert.equal(m.effMaxVol({maxVol: 100}), 100);
});
test('contract size determines gross notional and directional gross PnL', () => {
  assert.equal(m.notionalUsdt(10, 100, 0.1), 100);
  assert.equal(m.pnlLinear(100, 102, 10, 0.1, 'long'), 2);
  assert.equal(m.pnlLinear(100, 102, 10, 0.1, 'short'), -2);
  assert.throws(() => m.pnlLinear(100, 102, 10, 0.1, 'unknown'));
  assert.throws(() => m.pnlLinear(0, 102, 10, 0.1, 'long'));
  assert.throws(() => m.notionalUsdt(Infinity, 100, 0.1));
});
test('stale, missing and future timestamps fail freshness validation', () => {
  assert.equal(m.wsFresh(1000, 2000, 1000), true);
  assert.equal(m.wsFresh(999, 2000, 1000), false);
  assert.equal(m.wsFresh(2001, 2000, 1000), false);
  assert.equal(m.wsFresh(null, 2000, 1000), false);
});
