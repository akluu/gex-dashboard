// Pure math for the GEX page: no DOM, no page state. Loaded by index.html
// (served at /gexmath.js) AND by tests/test_gex_ratio.py under node, so the
// test runs the code the browser runs. gexRatio() mirrors gex_ratio() in
// gex/aggregate.py -- change both together.
//
// UNITS. Stored per row: call/put_gamma_oi = sum(gamma*OI*100), the change in
// share delta per $1 move ("raw gamma", GEXStream's published GEX unit), and
// call/put_delta_oi = sum(delta*OI*100), delta SHARES. Our dollar figures:
// GEX = raw gamma * S^2 * 0.01 ($ per 1% move), DEX = delta shares * S.
"use strict";
const GexMath = (() => {
  const real = v => (typeof v === "number" && Number.isFinite(v)) ? v : null;

  // Full names label tooltips and the trend axis; the short suffixes follow
  // a number fmt() has already marked with "$" (or not), in the header/notes.
  const UNITS = {
    usd: {gex: "$ per 1% move", dex: "$ delta", gexSfx: "per 1% move", dexSfx: "delta"},
    raw: {gex: "delta shares per $1 move (gamma × OI × 100)", dex: "delta shares",
          gexSfx: "shares per $1 move", dexSfx: "shares"},
  };

  // GEXStream's GEX Ratio over the rows in the SELECTED buckets: net calls
  // against puts at each strike (across those expiries) first, then
  // sum(positive nets) / sum(|nets|). null -- never 0 -- with nothing
  // selected, a zero denominator, or any missing/non-finite value (JSON null
  // would otherwise coerce to 0 in `+=`).
  function gexRatio(rows, buckets) {
    const sel = buckets instanceof Set ? buckets : new Set(buckets);
    const nets = new Map();
    for (const r of rows) {
      if (!sel.has(r.bucket)) continue;
      const k = real(r.strike), cg = real(r.call_gamma_oi), pg = real(r.put_gamma_oi);
      if (k === null || cg === null || pg === null) return null;
      nets.set(k, (nets.get(k) || 0) + (cg - pg));
    }
    let pos = 0, den = 0;
    for (const n of nets.values()) { den += Math.abs(n); if (n > 0) pos += n; }
    return (den > 0 && Number.isFinite(den)) ? pos / den : null;
  }

  // A raw sum in the chosen units. Dollar units need a finite, positive
  // spot; without one the value is unavailable (null), never scaled by 0 or
  // by some other snapshot's spot. S * S * 0.01 is grouped as in
  // net_exposure() so the result matches the server's to the bit.
  function gexIn(rawGamma, spot, units) {
    const g = real(rawGamma); if (g === null) return null;
    if (units === "raw") return g;
    const S = real(spot); return (S !== null && S > 0) ? g * (S * S * 0.01) : null;
  }
  function dexIn(rawDelta, spot, units) {
    const d = real(rawDelta); if (d === null) return null;
    if (units === "raw") return d;
    const S = real(spot); return (S !== null && S > 0) ? d * S : null;
  }

  // Trend points carry the STORED dollar GEX and that poll's own spot; raw
  // units divide by that spot -- never today's, never a substitute.
  function trendGex(p, units) {
    const g = real(p && p.gex); if (g === null) return null;
    if (units !== "raw") return g;
    const S = real(p.spot); return (S !== null && S > 0) ? g / (S * S * 0.01) : null;
  }

  // Magnitude-dependent scale: raw gamma is ~1000x smaller than dollars, so
  // a fixed billions scale would print 0.00 everywhere.
  function scaleFor(a) {
    return a >= 1e9 ? {div: 1e9, suf: "Bn"} : a >= 1e6 ? {div: 1e6, suf: "M"}
         : a >= 1e3 ? {div: 1e3, suf: "K"} : {div: 1, suf: ""};
  }
  // Signed and scaled: "+$9.223Bn" in dollars, "−2.01M" raw. "—" if unavailable.
  function fmt(v, units, digits) {
    if (real(v) === null) return "—";
    const a = Math.abs(v), sc = scaleFor(a);
    return `${v < 0 ? "−" : "+"}${units === "raw" ? "" : "$"}${(a / sc.div).toFixed(digits ?? 2)}${sc.suf}`;
  }
  const fmtRatio = r => real(r) === null ? "N/A" : r.toFixed(3);

  // ---- Intraday heatmap (step 5) -------------------------------------------
  // Diverging ramp, interpolated in OKLab from a neutral grey midpoint to two
  // poles: blue = net positive (calls > puts), red = net negative (the page's
  // put red). Not the page's call green: in a heatmap the sign is hue only,
  // and green/red fails the colour-blind check (validator, dark surface:
  // deutan ΔE 2.2; blue/red ≈ 26, both poles in the lightness band).
  const HEAT = {pos: "#4493f8", neg: "#f85149", mid: "#383835", steps: 32};
  const toLin = c => c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  const toSrgb = c => c <= 0.0031308 ? 12.92 * c : 1.055 * c ** (1 / 2.4) - 0.055;
  function hexToOklab(hex) {
    const n = parseInt(hex.slice(1), 16);
    const [r, g, b] = [(n >> 16) & 255, (n >> 8) & 255, n & 255].map(v => toLin(v / 255));
    const l = Math.cbrt(0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b);
    const m = Math.cbrt(0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b);
    const s = Math.cbrt(0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b);
    return [0.2104542553 * l + 0.7936177850 * m - 0.0040720468 * s,
            1.9779984951 * l - 2.4285922050 * m + 0.4505937099 * s,
            0.0259040371 * l + 0.7827717662 * m - 0.8086757660 * s];
  }
  function oklabToHex([L, A, B]) {
    const l = (L + 0.3963377774 * A + 0.2158037573 * B) ** 3;
    const m = (L - 0.1055613458 * A - 0.0638541728 * B) ** 3;
    const s = (L - 0.0894841775 * A - 1.2914855480 * B) ** 3;
    return "#" + [4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s,
                  -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s,
                  -0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s]
      .map(c => Math.round(Math.min(1, Math.max(0, toSrgb(c))) * 255).toString(16).padStart(2, "0")).join("");
  }
  // arm[0] is the midpoint, arm[steps] the pole.
  function heatRamp(spec = HEAT) {
    const mid = hexToOklab(spec.mid);
    const arm = pole => { const p = hexToOklab(pole);
      return Array.from({length: spec.steps + 1}, (_, i) => oklabToHex(mid.map((v, j) => v + (p[j] - v) * i / spec.steps))); };
    return {pos: arm(spec.pos), neg: arm(spec.neg), steps: spec.steps};
  }
  // Linear intensity: |v| / cap, saturating above the cap. null for no value.
  function heatColor(v, cap, ramp) {
    if (real(v) === null) return null;
    if (!(cap > 0) || v === 0) return ramp.pos[0];
    const i = Math.min(ramp.steps, Math.round(Math.abs(v) / cap * ramp.steps));
    return (v > 0 ? ramp.pos : ramp.neg)[i];
  }
  // Colour cap: the q-quantile (nearest rank) of |value| over EVERY valid cell
  // of the transmitted grid in the chosen unit -- one cap for both zoom levels,
  // so zooming never recolours a cell; session-to-date, so it moves as
  // snapshots arrive. Each column converts with its OWN spot; a column without
  // one has no dollar values. {cap, rule}: rule "quantile"; "max" when fewer
  // than 1 - q of the cells are non-zero (the quantile is 0, which would paint
  // real values as zero); "all zero"; "none" (no valid cell).
  function heatCap(columns, units, q = 0.99) {
    const a = [];
    for (const c of columns) {
      if (!Array.isArray(c.values)) continue;
      for (const v of c.values) { const x = gexIn(v, c.spot, units); if (x !== null) a.push(Math.abs(x)); }
    }
    if (!a.length) return {cap: null, rule: "none"};
    a.sort((x, y) => x - y);
    const p = a[Math.max(0, Math.ceil(q * a.length) - 1)], mx = a[a.length - 1];
    return mx === 0 ? {cap: 0, rule: "all zero"} : p > 0 ? {cap: p, rule: "quantile"} : {cap: mx, rule: "max"};
  }
  // Column i holds the last observed value from t_i until the next snapshot,
  // capped at maxGapMs (a collection pause is a gap, not a stretched cell);
  // the last column is NOT extended into unobserved time. ms: sorted times.
  function heatSpans(ms, maxGapMs) {
    return ms.map((t, i) => { const n = ms[i + 1];
      return [t, n !== undefined && n - t <= maxGapMs ? n : t]; });
  }
  // Whole-pixel geometry. Column i owns [a, b) with a = floor(x start),
  // b = ceil(x end), at least minW wide (an instantaneous or sub-pixel column
  // must stay visible), clamped to [lo, hi]. Columns overlap when snapshots
  // are denser than the pixels; the page PAINTS in time order and heatOwner()
  // HIT-TESTS with the last interval containing x, so both give the same
  // owner -- the latest snapshot (they had disagreed).
  function heatGeometry(xs, minW, lo, hi) {
    return xs.map(([x0, x1]) => {
      let a = Math.floor(x0), b = Math.max(Math.ceil(x1), a + minW);
      if (b > hi) { b = hi; a = Math.min(a, b - minW); }
      return [Math.max(lo, a), b];
    });
  }
  function heatOwner(geo, x) {
    for (let i = geo.length - 1; i >= 0; i--) if (x >= geo[i][0] && x < geo[i][1]) return i;
    return -1;
  }
  // The columns that own at least one pixel (the rest sit behind later ones).
  function heatVisible(geo) {
    const own = new Set();
    let lo = Infinity, hi = -Infinity;
    for (const [a, b] of geo) { lo = Math.min(lo, a); hi = Math.max(hi, b); }
    for (let p = lo; p < hi; p++) { const i = heatOwner(geo, p); if (i >= 0) own.add(i); }
    return own;
  }
  const heatHidden = geo => geo.length - heatVisible(geo).size;
  // Strike rows on whole pixels: each row's top and bottom are its band edges
  // ROUNDED, and a band's lower edge is exactly the next row's upper edge, so
  // rows partition the pixels -- [top, bottom), no overlap. Painting and the
  // tooltip both use these (they had disagreed at fractional edges).
  // rows: ascending strikes with .lo/.hi band edges; y: price -> pixel.
  function heatRowPixels(rows, y) {
    for (const r of rows) { r.top = Math.round(y(r.hi)); r.bottom = Math.round(y(r.lo)); }
    return rows;
  }
  const heatRowAt = (rows, py) => rows.find(r => py >= r.top && py < r.bottom) || null;
  function median(a) {
    if (!a.length) return null;
    const b = [...a].sort((x, y) => x - y), h = b.length >> 1;
    return b.length % 2 ? b[h] : (b[h - 1] + b[h]) / 2;
  }

  return {UNITS, gexRatio, gexIn, dexIn, trendGex, scaleFor, fmt, fmtRatio,
          HEAT, hexToOklab, heatRamp, heatColor, heatCap, heatSpans, heatGeometry, heatOwner, heatVisible, heatHidden, heatRowPixels, heatRowAt, median};
})();
if (typeof module !== "undefined" && module.exports) module.exports = GexMath;
