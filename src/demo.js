const m = require('./math');
console.log(JSON.stringify({fictional: true, volume: m.adjVol(12.3456, 2), grossPnl: m.pnlLinear(100, 102, 10, 0.1, 'long'), feePolicy: m.feePolicy({makerFeeRate: 0, takerFeeRate: 0})}, null, 2));
