# Legacy review

Source: `Flip.js.bak`. The `legacy/` file retains the original strategy logic for inspection with an unconditional early execution guard. This copy intentionally has no supported live invocation.

Original execution flow: MEXC / Binance WebSockets → freshness checks → entry/exit state machine → MEXC REST orders → position reconciliation and local logs.

Changes made to the copy: removed automatic dependency installation; replaced file-based credential loading with environment references; blocked legacy execution before any imports or side effects. Project-specific calculation corrections are in `src/math.js` and do not silently alter the original trading algorithms.

Known concerns:

- Original computeVol receives bookCap but does not use it; depth-based sizing remains incomplete.
- Original high leverage, fixed reconnect delay and shutdown order submission require simulation before use.
- Gross PnL helpers exclude fees, funding, slippage and realized fill differences.
- JavaScript Number arithmetic remains approximate; basic helper domains are narrower than a financial library.

Historical adapter dependencies are not installed by the offline package. Configuration, token maps, blacklists, log/state files and PM2 processes referenced by legacy code are not supplied as real data. Relocating this source changes historical file locations; those paths must be redesigned before a future integration. Neither endpoint compatibility nor live account behavior was verified.
