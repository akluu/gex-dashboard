"""Daily accuracy report. Run once per trading day by gex-validate.timer
(Mon-Fri 12:00 America/New_York); safe to run by hand.

What it can and cannot establish -- see "How accuracy is checked" in README.md:
inputs and arithmetic are checkable; the dealer-positioning assumption
(calls +, puts -) is not identifiable from a public chain, and nothing here
claims otherwise.

Four checks on ONE frozen stored poll -- the day's latest complete poll whose
newest trade is dated today:

  1. occ_oi      -- every contract side's open interest against OCC's free
                    series search (the clearing house, the authoritative
                    upstream). Exact integer equality; non-overlap is
                    classified, and anything unexplained is a discrepancy.
  2. greeks      -- CBOE's gamma/delta against Black-Scholes-Merton on CBOE's
                    own IV, on a clean cohort. A FIXED benchmark (r=4%, q=0)
                    is the pass/flag basis; a fitted-r diagnostic is reported
                    beside it, never instead of it. This is consistency, not
                    independent validation: IV and greeks share one source.
  3. arithmetic  -- net GEX/DEX recomputed from the stored aggregate with an
                    independent loop, against the totals the collector
                    stored; gross call/put GEX and net/gross (fragility);
                    coverage vs the previous trading session.
  4. stale_open  -- when today's first snapshot with a today-dated trade was
                    stored, and how many mixed-age snapshots preceded it.

Plus a REPORT-ONLY diagnostic (not part of PASS -- added 2026-09-30, audit
F12): short_dated -- the same BSM consistency question for 0-6 DTE, which
check 2 leaves out although 0DTE dominates near-spot gamma.

Every run writes a result -- including "skipped" and "error" -- so an old
pass can never stand in for today's check. Output:
<root>/validation/date=YYYY-MM-DD.json (+ the raw OCC response beside it).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import sys
import tempfile
from collections import Counter
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from .aggregate import _NY
from .market_calendar import holiday_name, options_close

OCC_URL = "https://marketdata.theocc.com/series-search?symbolType=U&symbol={symbol}"
OCC_TIMEOUT = 30

FIXED_R, FIXED_Q = 0.04, 0.0          # best fit on the 2026-09-24 live checkpoint
R_GRID = (0.0, 0.02, 0.03, 0.04, 0.05)
COHORT_DTE = (7, 60)
COHORT_ABS_DELTA = (0.2, 0.8)
COHORT_MAX_REL_SPREAD = 0.10
# OUT-OF-THE-MONEY only (calls K >= S, puts K <= S). Added after the first
# run (2026-09-29): every large delta error was an in-the-money put (delta
# -0.7..-0.8, 3-8 weeks out; CBOE's delta MORE negative than BSM's), the
# signature of American early-exercise value that European BSM omits -- calls
# alone had delta-error p95 0.0075. ITM puts are still reported, separately,
# as a diagnostic.
MIN_COHORT = 200
# Provisional alerts, NOT certification thresholds.
FLAG_MEDIAN_GAMMA_ERR = 0.05
FLAG_DELTA_P95 = 0.02

SIDES = (("C", "call_"), ("P", "put_"))


# ----------------------------------------------------------------- helpers

def _atomic_write_json(obj: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(obj, fh, indent=1, default=str, allow_nan=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and not holiday_name(d)


def prev_trading_day(d: date) -> date:
    d -= timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


def _trade_date(newest_trade: str | None) -> date | None:
    """CBOE's newest trade stamp is naive EASTERN; its date part is the ET date."""
    try:
        return datetime.fromisoformat(str(newest_trade)).date() if newest_trade else None
    except ValueError:
        return None


# ------------------------------------------------------------ poll choice

def quarantined_stamps(root: Path, day: date) -> set[str]:
    """Stamps of polls quarantined for `day`. On the VM a quarantined poll is
    MOVED out of metadata/aggregates, but the append-only Mac backup keeps the
    original too -- so exclusion must be explicit, not assumed."""
    q = root / "quarantine" / day.isoformat()
    out = set()
    for f in q.glob("*.parquet") if q.exists() else []:
        for prefix in ("meta_", "agg_"):
            if f.name.startswith(prefix):
                out.add(f.name[len(prefix):])
    return out


def day_polls(root: Path, symbol: str, day: date) -> list[tuple[str, Path, Path]]:
    """Complete (metadata + aggregate) polls of a day, oldest first, never a
    quarantined one."""
    sym = symbol.lstrip("_")
    bad = quarantined_stamps(root, day)
    mdir = root / "metadata" / f"symbol={sym}" / f"date={day.isoformat()}"
    adir = root / "aggregates" / f"symbol={sym}" / f"date={day.isoformat()}"
    out = []
    for m in sorted(mdir.glob("meta_*.parquet")) if mdir.exists() else []:
        stamp = m.name[len("meta_"):]
        a = adir / f"agg_{stamp}"
        if a.exists() and stamp not in bad:
            out.append((stamp, m, a))
    return out


def latest_live_poll(root: Path, symbol: str, day: date, unreadable: list | None = None):
    """(stamp, meta_row, agg_rows) of the day's latest usable poll whose newest
    trade is dated `day`, or None. Polls that could not be read on the way are
    appended to `unreadable` -- an unreadable newer poll is a data problem to
    report, not something to step over silently."""
    for stamp, m, a in reversed(day_polls(root, symbol, day)):
        try:
            meta = pq.read_table(m).to_pylist()[0]
        except Exception as e:
            if unreadable is not None:
                unreadable.append({"stamp": stamp, "file": "metadata",
                                   "error": f"{type(e).__name__}: {e}"[:200]})
            continue
        if meta.get("usable") and _trade_date(meta.get("newest_trade_time")) == day:
            try:
                return stamp, meta, pq.read_table(a).to_pylist()
            except Exception as e:
                if unreadable is not None:
                    unreadable.append({"stamp": stamp, "file": "aggregate",
                                       "error": f"{type(e).__name__}: {e}"[:200]})
                continue
    return None


# ---------------------------------------------------------------- checks

def parse_occ_series(text: str, symbol: str) -> tuple[dict, dict]:
    """OCC series-search text -> ({(expiry ISO, strike): (call OI, put OI)},
    counts). Only rows for exactly `symbol`: the same file also lists other
    products on the underlying (4SPY, 2SPY, 1SPY -- ~2,500 rows on
    2026-09-29) that are not in CBOE's standard chain; a first version that
    kept them reported 2,298 spurious "unexplained" rows.

    Every row that STARTS with the symbol must have the exact shape
    SYM yyyy mm dd int dec C P callOI putOI poslimit, with non-negative
    integer OI, and a unique (expiry, strike); anything else is counted as
    unparsed or duplicate, and either count blocks a match. (A first version
    decided "header or data" from the year field before looking at the
    symbol, so a garbled SPY row vanished silently.)"""
    out, other_products, unparsed, duplicates = {}, 0, 0, 0
    sym = symbol.lstrip("_")
    for line in text.splitlines():
        p = line.split()
        if not p:
            continue
        if p[0] != sym:
            if len(p) >= 2 and p[1].isdigit() and len(p[1]) == 4:
                other_products += 1                # data row, other product
            continue                               # header / other product
        try:
            if len(p) != 11 or p[6:8] != ["C", "P"]:
                raise ValueError("shape")
            exp = date(int(p[1]), int(p[2]), int(p[3])).isoformat()
            strike = float(f"{int(p[4])}.{p[5]}")
            c_oi, p_oi = int(p[8]), int(p[9])
            if c_oi < 0 or p_oi < 0:
                raise ValueError("negative OI")
        except ValueError:
            unparsed += 1
            continue
        if (exp, strike) in out:
            duplicates += 1
            continue
        out[(exp, strike)] = (float(c_oi), float(p_oi))
    return out, {"rows": len(out), "other_products_skipped": other_products,
                 "unparsed_rows": unparsed, "duplicate_rows": duplicates}


def check_occ(rows: list[dict], day: date, symbol: str, out_dir: Path,
              fetch=None, prev_keys: set | None = None) -> dict:
    """Exact per-side OI equality against OCC. `fetch(url) -> (status, bytes)`
    is injectable for tests; the default is one bounded HTTP GET.
    `prev_keys`: (expiry, strike) keys of the previous session's poll, the
    evidence needed to call a CBOE-only row a new listing."""
    if fetch is None:
        import requests

        def fetch(url):
            r = requests.get(url, timeout=OCC_TIMEOUT,
                             headers={"User-Agent": "gex_tool/0.1 (personal validation)"})
            return r.status_code, r.content

    url = OCC_URL.format(symbol=symbol.lstrip("_"))
    retrieved = datetime.now(timezone.utc)
    try:
        status, body = fetch(url)
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}
    if status != 200 or not body:
        return {"status": "error", "error": f"http {status}, {len(body or b'')} bytes"}
    sha = hashlib.sha256(body).hexdigest()
    # Immutable evidence: named by content hash and published atomically, so
    # a rerun can never change or truncate the file an earlier report cites.
    raw_path = out_dir / f"occ_{symbol.lstrip('_')}_{day.isoformat()}_{sha[:16]}.txt"
    if not raw_path.exists():
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(out_dir), prefix=".tmp_", suffix=".txt")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, raw_path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
    occ, parse_counts = parse_occ_series(body.decode("utf-8", "replace"), symbol)
    if len(occ) < 1000:
        return {"status": "error", "error": f"OCC response parsed to only {len(occ)} rows",
                "sha256": sha, "parse": parse_counts}

    # Mandatory OI must be present, numeric, finite, non-negative and integral:
    # a first version turned a null OI into 0.0 and could then "match".
    invalid_oi = []
    ours = {}
    for r in rows:
        vals = (r.get("call_oi"), r.get("put_oi"))
        if not all(_valid_oi(v) for v in vals):
            invalid_oi.append({"expiry": r.get("expiry"), "strike": r.get("strike"),
                               "call_oi": vals[0], "put_oi": vals[1]})
            continue
        ours[(r["expiry"], float(r["strike"]))] = (float(vals[0]), float(vals[1]))
    common = sorted(set(ours) & set(occ))
    diffs = []
    for key in common:
        for i, side in enumerate("CP"):
            if ours[key][i] != occ[key][i]:
                diffs.append({"expiry": key[0], "strike": key[1], "side": side,
                              "cboe": ours[key][i], "occ": occ[key][i]})

    # Non-overlap: explained only by evidence, not exempted wholesale.
    occ_exp = {e for e, _ in occ}
    our_exp = {e for e, _ in ours}
    only_occ = [k for k in occ if k not in ours]
    only_ours = [k for k in ours if k not in occ]
    explained, unexplained = [], []
    for e, k in only_occ:
        (explained if e < day.isoformat() else unexplained).append(
            {"where": "occ_only", "expiry": e, "strike": k,
             "why": "expired before this session" if e < day.isoformat() else None})
    for e, k in only_ours:
        # Explained ONLY with evidence of a new listing: zero OI AND absent
        # from the previous session. Whole missing expiries get no blanket
        # pass (reproduced a 19,998-contract gap reading "match").
        zero_oi = ours[(e, k)] == (0.0, 0.0)
        new_since_prev = prev_keys is not None and (e, k) not in prev_keys
        if e not in occ_exp and zero_oi and new_since_prev:
            explained.append({"where": "cboe_only", "expiry": e, "strike": k,
                              "why": "new expiry since the previous session, zero OI"})
        elif e in occ_exp and zero_oi and new_since_prev:
            # Seen 2026-09-29: 121 new strikes on an existing expiry, zero OI,
            # absent from the previous session -- listed after OCC's file.
            explained.append({"where": "cboe_only", "expiry": e, "strike": k,
                              "why": "new strike since the previous session, zero OI"})
        else:
            unexplained.append({"where": "cboe_only", "expiry": e, "strike": k, "why": None,
                                "oi": ours[(e, k)], "in_previous_session": not new_since_prev
                                if prev_keys is not None else None})
    n_sides = 2 * len(common)
    ok = (not diffs and not unexplained and not invalid_oi
          and parse_counts["unparsed_rows"] == 0 and parse_counts["duplicate_rows"] == 0)
    return {
        "parse": parse_counts,
        "invalid_oi_rows": invalid_oi[:50], "n_invalid_oi": len(invalid_oi),
        "status": "match" if ok else "mismatch",
        "sides_compared": n_sides,
        "sides_equal": n_sides - len(diffs),
        "differences": diffs[:50], "n_differences": len(diffs),
        "total_oi_cboe": sum(ours[k][0] + ours[k][1] for k in common),
        "total_oi_occ": sum(occ[k][0] + occ[k][1] for k in common),
        "non_overlap_explained": _summarise(explained),
        "non_overlap_unexplained": unexplained[:50], "n_unexplained": len(unexplained),
        "occ_effective_date": {"inferred": prev_trading_day(day).isoformat(),
                               "confirmed": None,
                               "basis": "OCC series-search OI is believed to reflect the "
                                        "previous trading day's close; the file carries "
                                        "no as-of date"},
        "retrieved_at": retrieved.isoformat(), "url": url, "sha256": sha,
        "raw_file": raw_path.name,
        "expiries": {"cboe": len(our_exp), "occ": len(occ_exp)},
    }


def _valid_oi(v) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
            and v >= 0 and float(v).is_integer())


def _summarise(items: list[dict]) -> list[dict]:
    by = {}
    for it in items:
        k = (it["where"], it["expiry"], it["why"])
        by[k] = by.get(k, 0) + 1
    return [{"where": w, "expiry": e, "why": why, "rows": n} for (w, e, why), n in sorted(by.items())]


_N = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
_n = lambda x: math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bsm_gamma_delta(S, K, T, sigma, r, q, opt):
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    g = math.exp(-q * T) * _n(d1) / (S * sigma * math.sqrt(T))
    d = math.exp(-q * T) * (_N(d1) if opt == "C" else _N(d1) - 1.0)
    return g, d


def greek_cohort(rows: list[dict], meta: dict, valuation: datetime, itm_puts: bool = False):
    """Per-contract (strike, expiry, side, T, iv, gamma, delta, oi) recovered
    from the aggregate (side sum / (OI*100)) and filtered to the clean cohort.
    Returns (cohort, totals) where totals feed the coverage figures."""
    S = meta["spot"]
    cohort, tot_oi, tot_gross_gamma = [], 0.0, 0.0
    for r in rows:
        exp = date.fromisoformat(r["expiry"])
        close = datetime(exp.year, exp.month, exp.day, 16, 0, tzinfo=_NY)
        T = (close - valuation).total_seconds() / (365.0 * 86400.0)
        dte = (exp - valuation.astimezone(_NY).date()).days
        for opt, p in SIDES:
            oi = r.get(p + "oi") or 0.0
            if oi <= 0:
                continue
            gamma = r[p + "gamma_oi"] / (oi * 100.0)
            delta = r[p + "delta_oi"] / (oi * 100.0)
            tot_oi += oi
            tot_gross_gamma += abs(gamma) * oi
            iv, bid, ask = r.get(p + "iv"), r.get(p + "bid"), r.get(p + "ask")
            if not all(isinstance(v, (int, float)) and math.isfinite(v)
                       for v in (iv, bid, ask, gamma, delta)):
                continue                    # counted by check_greeks below
            if not (COHORT_DTE[0] <= dte <= COHORT_DTE[1] and T > 0):
                continue
            if not (COHORT_ABS_DELTA[0] < abs(delta) < COHORT_ABS_DELTA[1]):
                continue
            if not (iv and iv > 0 and gamma > 0 and bid and bid > 0 and ask and ask > bid):
                continue
            if (ask - bid) / ((ask + bid) / 2.0) > COHORT_MAX_REL_SPREAD:
                continue
            otm = r["strike"] >= S if opt == "C" else r["strike"] <= S
            if itm_puts:
                if not (opt == "P" and not otm):
                    continue                    # diagnostic: ITM puts only
            elif not otm:
                continue                        # benchmark: OTM only
            cohort.append((r["strike"], r["expiry"], opt, T, iv, gamma, delta, oi))
    return cohort, tot_oi, tot_gross_gamma


def _errors(cohort, S, r, q):
    g_err, d_err, w_oi, w_gg = [], [], [], []
    for K, _e, opt, T, iv, gamma, delta, oi in cohort:
        gb, db = bsm_gamma_delta(S, K, T, iv, r, q, opt)
        g_err.append(abs(gb / gamma - 1.0))
        d_err.append(abs(db - delta))
        w_oi.append(oi)
        w_gg.append(gamma * oi)
    return g_err, d_err, w_oi, w_gg


def _weighted_median(xs, ws):
    pairs = sorted(zip(xs, ws))
    half, acc = sum(ws) / 2.0, 0.0
    for x, w in pairs:
        acc += w
        if acc >= half:
            return x
    return None


def check_greeks(rows: list[dict], meta: dict) -> dict:
    S = meta.get("spot")
    trade = meta.get("newest_trade_time")
    if not S or not trade:
        return {"status": "error", "error": "stored poll lacks spot or newest_trade_time"}
    if not any(r.get("call_iv") is not None or r.get("put_iv") is not None for r in rows):
        return {"status": "not_run", "error": "poll predates schema v2 (no IV stored)"}
    # Valuation time: the newest trade is the latest OBSERVED trade, not a
    # synchronised greek/spot timestamp -- so sensitivity to it is reported.
    t_trade = datetime.fromisoformat(str(trade)).replace(tzinfo=_NY)
    cohort, tot_oi, tot_gg = greek_cohort(rows, meta, t_trade)
    if len(cohort) < MIN_COHORT:
        return {"status": "insufficient", "n_cohort": len(cohort), "min_cohort": MIN_COHORT}

    def stats(r, q, valuation=None):
        c = cohort if valuation is None else greek_cohort(rows, meta, valuation)[0]
        g, d, woi, wgg = _errors(c, S, r, q)
        return {
            "r": r, "q": q, "n": len(c),
            "gamma_rel_err_median": statistics.median(g),
            "gamma_rel_err_p95": _pct(g, 0.95),
            "share_gamma_err_over_10pct": sum(1 for x in g if x > 0.10) / len(g),
            "gamma_rel_err_oi_weighted_median": _weighted_median(g, woi),
            "gamma_rel_err_gross_gamma_weighted_mean": sum(x * w for x, w in zip(g, wgg)) / sum(wgg),
            "delta_abs_err_median": statistics.median(d),
            "delta_abs_err_p95": _pct(d, 0.95),
        }

    fixed = stats(FIXED_R, FIXED_Q)
    headline = [fixed[k] for k in ("gamma_rel_err_median", "gamma_rel_err_p95",
                                   "delta_abs_err_median", "delta_abs_err_p95")]
    if not all(isinstance(v, float) and math.isfinite(v) for v in headline):
        return {"status": "error", "error": "non-finite greek error statistics",
                "n_cohort": len(cohort)}
    nonfinite = sum(1 for r in rows for opt, p in SIDES
                    for k in ("iv", "bid", "ask")
                    if isinstance(r.get(p + k), float) and not math.isfinite(r[p + k]))
    itm = greek_cohort(rows, meta, t_trade, itm_puts=True)[0]
    itm_diag = None
    if itm:
        g, d, _w, _ww = _errors(itm, S, FIXED_R, FIXED_Q)
        itm_diag = {"n": len(itm), "gamma_rel_err_median": statistics.median(g),
                    "delta_abs_err_median": statistics.median(d),
                    "delta_abs_err_p95": _pct(d, 0.95),
                    "note": "excluded from the benchmark; HYPOTHESIS (not "
                            "established): American early exercise and/or dividend "
                            "convention"}
    fitted = min((stats(r, 0.0) for r in R_GRID), key=lambda s: s["gamma_rel_err_median"])
    # Timing sensitivity: value 15 min later than the newest trade (roughly
    # the feed delay) and compare the headline error.
    later = stats(FIXED_R, FIXED_Q, t_trade + timedelta(minutes=15))
    cohort_oi = sum(c[7] for c in cohort)
    cohort_gg = sum(c[5] * c[7] for c in cohort)
    flags = []
    if nonfinite:
        flags.append(f"{nonfinite} non-finite IV/bid/ask values in the poll")
    if fixed["gamma_rel_err_median"] > FLAG_MEDIAN_GAMMA_ERR:
        flags.append(f"median gamma error {fixed['gamma_rel_err_median']:.1%} > "
                     f"{FLAG_MEDIAN_GAMMA_ERR:.0%}")
    if fixed["delta_abs_err_p95"] > FLAG_DELTA_P95:
        flags.append(f"delta error p95 {fixed['delta_abs_err_p95']:.3f} > {FLAG_DELTA_P95}")
    return {
        "status": "investigate" if flags else "consistent",
        "scope": "consistency of CBOE's greeks with its own IV on the OTM cohort "
                 "only -- not the whole chain, not independent price validation",
        "flags": flags,
        "benchmark": fixed, "fitted_diagnostic": fitted,
        "itm_puts_diagnostic": itm_diag,
        "timing_sensitivity": {"valuation_plus_15min_gamma_median": later["gamma_rel_err_median"]},
        "cohort": {"dte": COHORT_DTE, "abs_delta": COHORT_ABS_DELTA, "moneyness": "OTM only",
                   "max_rel_spread": COHORT_MAX_REL_SPREAD, "n": len(cohort),
                   "oi_coverage": cohort_oi / tot_oi if tot_oi else None,
                   "gross_gamma_coverage": cohort_gg / tot_gg if tot_gg else None},
        "assumptions": ["q=0 is an empirical fit, not a verified dividend convention",
                        "no ex-dividend exclusion applied (SPY ex-dates unresolved)",
                        "SPY options are American; BSM is European",
                        "valuation time = newest observed trade, not a synchronised stamp"],
        "not_validated": ["0-6 DTE, including 0DTE", ">60 DTE", "ITM (calls and puts)",
                          "|delta| outside 0.2-0.8", "zero-OI contracts"],
    }


# ---- short-dated diagnostic (report-only; audit F12, design checked) ----
SHORT_COHORTS = ((0, 0, "0DTE"), (1, 1, "1DTE"), (2, 6, "2-6DTE"))
SHORT_MONEYNESS = 0.02           # |K/S - 1|
SHORT_ABS_DELTA = (0.05, 0.95)   # drops near-worthless wings
SHORT_MIN_GAMMA = 0.002          # CBOE rounds gamma to 4 decimals: >= 20x that resolution
SHORT_MAX_REL_SPREAD = 0.10
SHORT_MAX_ABS_SPREAD = 0.02      # or within 2 cents: penny-tick quotes are always "wide" in %
# Minimum contracts per cohort. 0DTE is lower BY MEASUREMENT: at the noon run
# (~4 h left) only the ~10 strikes within ~$5 of spot have |delta| > 0.05
# (09-29: 11, 09-30: 10) -- they carry the 0DTE gamma; the rest are wings.
SHORT_MIN_N = {"0DTE": 8, "1DTE": 20, "2-6DTE": 20}
# PROVISIONAL investigation triggers, not accuracy limits: calibrate
# from real runs before this is ever allowed to affect PASS.
SHORT_FLAG_GAMMA_ERR = 0.15
SHORT_FLAG_MULT = (0.67, 1.5)


def _expiry_closes(exp: date) -> dict:
    """Both maturity assumptions for an expiry, early closes included: the
    options close (16:15 ET, 13:15 on early-close days) and the equity close
    15 minutes earlier (16:00 / 13:00). Trading to 16:15 does not establish
    which one CBOE's greeks use -- Nasdaq marks these products at 16:00."""
    oc = datetime.combine(exp, options_close(exp, dtime(16, 15)), tzinfo=_NY)
    return {"16:15": oc, "16:00": oc - timedelta(minutes=15)}


def short_dated_cohort(rows: list[dict], meta: dict, valuation: datetime):
    """(items, excluded, gross) for 0-6 DTE contracts. Per-contract gamma and
    delta: v3's stored values when present, else recovered as side sum /
    (OI*100). Exclusions are counted by reason, gross gamma kept per cohort
    for the coverage figures."""
    S = meta["spot"]
    vday = valuation.astimezone(_NY).date()
    items, excluded, listed = [], Counter(), set()
    gross = {lab: [0.0, 0.0] for _a, _b, lab in SHORT_COHORTS}        # [all, in cohort]
    for r in rows:
        exp = date.fromisoformat(r["expiry"])
        dte = (exp - vday).days
        lab = next((l for a, b, l in SHORT_COHORTS if a <= dte <= b), None)
        if lab is None:
            continue
        closes = _expiry_closes(exp)
        T = {k: (v - valuation).total_seconds() / (365.0 * 86400.0) for k, v in closes.items()}
        listed.add(lab)
        for opt, p in SIDES:
            oi = r.get(p + "oi")
            if not (isinstance(oi, (int, float)) and not isinstance(oi, bool) and math.isfinite(oi)):
                excluded["missing or non-finite OI"] += 1
                continue
            if oi <= 0:
                excluded["zero OI"] += 1
                continue
            g, d = r.get(p + "gamma"), r.get(p + "delta")
            if g is None or d is None:                              # pre-v3: recover
                g, d = r[p + "gamma_oi"] / (oi * 100.0), r[p + "delta_oi"] / (oi * 100.0)
            iv, bid, ask, K = r.get(p + "iv"), r.get(p + "bid"), r.get(p + "ask"), r["strike"]
            if isinstance(g, (int, float)) and math.isfinite(g):
                gross[lab][0] += abs(g) * oi
            if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (iv, g, bid, ask)):
                excluded["non-finite or missing IV/gamma/quote"] += 1
            elif min(T.values()) <= 0:
                excluded["no time left"] += 1
            elif (K < S) if opt == "C" else (K > S):
                excluded["in the money"] += 1
            elif abs(K / S - 1) > SHORT_MONEYNESS:
                excluded["outside +-2% of spot"] += 1
            elif g < SHORT_MIN_GAMMA:
                excluded["gamma below resolution floor"] += 1
            elif isinstance(d, (int, float)) and math.isfinite(d) and \
                    not (SHORT_ABS_DELTA[0] < abs(d) < SHORT_ABS_DELTA[1]):
                excluded["|delta| outside 0.05-0.95"] += 1
            elif not (iv > 0 and bid > 0 and ask > bid) or (
                    (ask - bid) > SHORT_MAX_ABS_SPREAD + 1e-9 and (ask - bid) / ((ask + bid) / 2) > SHORT_MAX_REL_SPREAD):
                excluded["bad or wide quote"] += 1
            else:
                gross[lab][1] += g * oi
                items.append({"cohort": lab, "K": K, "expiry": r["expiry"], "opt": opt, "iv": iv, "g": g,
                              "d": d if isinstance(d, (int, float)) and math.isfinite(d) else None,
                              "oi": oi, "T": T})
    return items, excluded, gross, listed


def _short_stats(items, S, key):
    g_err, d_err = [], []
    for it in items:
        gb, db = bsm_gamma_delta(S, it["K"], it["T"][key], it["iv"], FIXED_R, FIXED_Q, it["opt"])
        g_err.append(abs(gb / it["g"] - 1.0))
        if it["d"] is not None:
            d_err.append(abs(db - it["d"]))
    return {"gamma_rel_err_median": statistics.median(g_err), "gamma_rel_err_p95": _pct(g_err, 0.95),
            "n_delta": len(d_err),
            "delta_abs_err_median": statistics.median(d_err) if d_err else None,
            "delta_abs_err_p95": _pct(d_err, 0.95) if d_err else None}


def _fit_multiplier(items, S):
    """Shared multiplier m on each contract's OWN 16:15-based T minimising the
    median |log(gamma_BSM / gamma_CBOE)| -- coarse geometric grid 0.2x..5x,
    then refined. An EFFECTIVE time, not a recovered maturity: gamma is not
    monotonic in T for every OTM contract, so a near-optimal range and any
    boundary hit are reported with it."""
    def obj(m):
        return statistics.median(abs(math.log(bsm_gamma_delta(S, it["K"], m * it["T"]["16:15"], it["iv"],
                                                              FIXED_R, FIXED_Q, it["opt"])[0] / it["g"]))
                                 for it in items)
    lo, hi, n = math.log(0.2), math.log(5.0), 60
    grid = [math.exp(lo + (hi - lo) * i / n) for i in range(n + 1)]
    vals = [(obj(m), m) for m in grid]
    best_v, best_m = min(vals)
    step = (hi - lo) / n
    fine = [math.exp(min(hi, max(lo, math.log(best_m) + step * (j / 20.0 - 1.0)))) for j in range(41)]
    vals += [(obj(m), m) for m in fine]
    best_v, best_m = min(vals)
    near = [m for v, m in vals if v <= best_v + 0.005]
    return {"multiplier_of_16_15_T": best_m, "gamma_log_err_median": best_v,
            "near_optimal_range": [min(near), max(near)],
            "boundary_hit": best_m <= grid[1] or best_m >= grid[-2]}


def check_short_dated(rows: list[dict], meta: dict) -> dict:
    """REPORT-ONLY short-dated BSM consistency diagnostic (0-6 DTE)."""
    S, trade = meta.get("spot"), meta.get("newest_trade_time")
    if not S or not trade:
        return {"status": "error", "error": "stored poll lacks spot or newest_trade_time"}
    if not any(r.get("call_iv") is not None or r.get("put_iv") is not None for r in rows):
        return {"status": "not_run", "error": "poll predates schema v2 (no IV stored)"}
    valuation = datetime.fromisoformat(str(trade)).replace(tzinfo=_NY)
    items, excluded, gross, listed = short_dated_cohort(rows, meta, valuation)
    cohorts, flags = {}, []
    for _a, _b, lab in SHORT_COHORTS:
        its = [it for it in items if it["cohort"] == lab]
        c = {"n": len(its), "expiries": sorted({it["expiry"] for it in its}),
             "gross_gamma_coverage": gross[lab][1] / gross[lab][0] if gross[lab][0] else None}
        if lab not in listed:                     # e.g. no 1-day expiry on a Friday
            c["status"] = "none listed"
            cohorts[lab] = c
            continue
        if len(its) < SHORT_MIN_N[lab]:
            c["status"] = "insufficient"
            cohorts[lab] = c
            continue
        c["at_16_15"] = _short_stats(its, S, "16:15")
        c["at_16_00"] = _short_stats(its, S, "16:00")
        c["fitted"] = _fit_multiplier(its, S)
        if len(c["expiries"]) == 1:                     # one maturity: an effective time is meaningful
            t = its[0]["T"]
            c["hours_to_16_15"], c["hours_to_16_00"] = t["16:15"] * 8760, t["16:00"] * 8760
            c["effective_hours"] = c["fitted"]["multiplier_of_16_15_T"] * c["hours_to_16_15"]
        f = []
        if min(c["at_16_15"]["gamma_rel_err_median"], c["at_16_00"]["gamma_rel_err_median"]) > SHORT_FLAG_GAMMA_ERR:
            f.append(f"{lab}: median gamma error > {SHORT_FLAG_GAMMA_ERR:.0%} under both close assumptions")
        m = c["fitted"]["multiplier_of_16_15_T"]
        if not (SHORT_FLAG_MULT[0] <= m <= SHORT_FLAG_MULT[1]) or c["fitted"]["boundary_hit"]:
            f.append(f"{lab}: effective-time multiplier {m:.2f} outside {SHORT_FLAG_MULT}"
                     + (" (search boundary)" if c["fitted"]["boundary_hit"] else ""))
        c["status"] = "investigate" if f else "consistent"
        flags += f
        cohorts[lab] = c
    short = [lab for lab, c in cohorts.items() if c["status"] == "insufficient"]
    if not listed:                                # nothing evaluated: never "consistent"
        return {"status": "not_run", "report_only": True, "error": "no 0-6 DTE expiries present",
                "cohorts": cohorts, "excluded": dict(excluded)}
    return {
        # "incomplete" whenever a LISTED cohort lacks evidence -- success is
        # never claimed for cohorts that could not be evaluated.
        "status": "investigate" if flags else ("incomplete" if short else "consistent"),
        "insufficient_cohorts": short,
        "report_only": True,
        "scope": "short-dated BSM consistency DIAGNOSTIC on CBOE's own IV -- not part of PASS, "
                 "not independent validation; thresholds provisional",
        "flags": flags, "cohorts": cohorts, "excluded": dict(excluded),
        "valuation": "newest observed trade (not a synchronised greek/spot stamp)",
        "cohort_rules": {"moneyness": SHORT_MONEYNESS, "abs_delta": SHORT_ABS_DELTA, "min_gamma": SHORT_MIN_GAMMA,
                         "max_rel_spread": SHORT_MAX_REL_SPREAD, "max_abs_spread": SHORT_MAX_ABS_SPREAD,
                         "min_n": SHORT_MIN_N, "otm_only": True},
    }


def check_arithmetic(rows: list[dict], meta: dict, prev_rows: list[dict] | None) -> dict:
    S = meta.get("spot")
    if not S:
        return {"status": "error", "error": "no spot"}
    call_g = put_g = dex = 0.0
    nonfinite = 0
    for r in rows:
        vals = [r.get("call_gamma_oi"), r.get("put_gamma_oi"), r.get("call_delta_oi"),
                r.get("put_delta_oi")]
        if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in vals):
            nonfinite += 1
            continue
        call_g += r["call_gamma_oi"]
        put_g += r["put_gamma_oi"]
        dex += r["call_delta_oi"] + r["put_delta_oi"]
    scale = S * S * 0.01
    net, gross = (call_g - put_g) * scale, (call_g + put_g) * scale
    stored_gex, stored_dex = meta.get("net_gex_per_pct"), meta.get("net_dex")
    # Tolerance on the GROSS sums: net cancellation makes a net-relative
    # tolerance meaningless.
    tol_gex = 1e-9 * gross
    tol_dex = 1e-9 * sum(abs(r.get("call_delta_oi") or 0.0) + abs(r.get("put_delta_oi") or 0.0)
                         for r in rows) * S
    gex_ok = stored_gex is not None and abs(net - stored_gex) <= tol_gex
    dex_ok = stored_dex is not None and abs(dex * S - stored_dex) <= tol_dex
    nonfinite_any = sum(1 for r in rows for k, v in r.items()
                        if isinstance(v, float) and not math.isfinite(v))
    mandatory = ("strike", "expiry", "call_gamma_oi", "put_gamma_oi", "call_delta_oi",
                 "put_delta_oi", "call_oi", "put_oi", "call_volume", "put_volume")
    missing_mandatory = sum(1 for r in rows for k in mandatory if r.get(k) is None)
    cov = {"strike_expiry_rows": len(rows),
           "non_finite_values_any_column": nonfinite_any,
           "missing_mandatory_values": missing_mandatory,
           "expiries": len({r["expiry"] for r in rows}),
           "non_finite_rows": nonfinite,
           "n_quote_collisions": meta.get("n_quote_collisions")}
    if prev_rows is not None:
        pe, te = {r["expiry"] for r in prev_rows}, {r["expiry"] for r in rows}
        cov["vs_previous_session"] = {"rows_prev": len(prev_rows),
                                      "expiries_added": sorted(te - pe),
                                      "expiries_removed": sorted(pe - te)}
    ok = (gex_ok and dex_ok and nonfinite == 0 and nonfinite_any == 0
          and missing_mandatory == 0 and not meta.get("n_quote_collisions"))
    return {
        "status": "reconciled" if ok else "investigate",
        "net_gex_recomputed": net, "net_gex_stored": stored_gex,
        "net_dex_recomputed": dex * S, "net_dex_stored": stored_dex,
        "gross_call_gex": call_g * scale, "gross_put_gex": put_g * scale,
        "net_over_gross": net / gross if gross else None,
        "coverage": cov,
        "note": "storage-to-total consistency; a shared upstream mistake survives both loops",
    }


def check_stale_open(root: Path, symbol: str, day: date) -> dict:
    polls = day_polls(root, symbol, day)
    before, first_live = 0, None
    for stamp, m, _a in polls:
        try:
            meta = pq.read_table(m).to_pylist()[0]
        except Exception:
            continue
        if _trade_date(meta.get("newest_trade_time")) == day:
            first_live = {"received_at": meta.get("received_at"),
                          "newest_trade": meta.get("newest_trade_time")}
            break
        before += 1
    return {"stored_before_first_today_trade": before,
            "first_observed_today_trade_snapshot": first_live,
            "note": "snapshots before it pair the previous session's greeks with "
                    "today's spot (mixed-age)"}


# ------------------------------------------------------------------ run

def run(root: Path, symbol: str, day: date, now: datetime | None = None,
        fetch=None) -> dict:
    now = now or datetime.now(timezone.utc)
    out_dir = root / "validation"
    result = {"date": day.isoformat(), "symbol": symbol, "run_at": now.isoformat(),
              "status": None}
    try:
        if not is_trading_day(day):
            result["status"] = "skipped"
            result["reason"] = holiday_name(day) or "weekend"
            return result
        unreadable: list = []
        live = latest_live_poll(root, symbol, day, unreadable)
        result["unreadable_polls"] = unreadable
        if live is None:
            if unreadable:
                result["status"] = "error"
                result["error"] = f"{len(unreadable)} unreadable poll(s) and no readable live poll"
            else:
                result["status"] = "skipped"
                result["reason"] = "no stored poll with a trade dated this session"
            return result
        stamp, meta, rows = live
        result["poll"] = {"stamp": stamp, "received_at": meta.get("received_at"),
                          "newest_trade": meta.get("newest_trade_time")}
        # OCC's response is undated: only compare it with a poll from the SAME
        # ET day the check runs on, so a delayed run cannot pair yesterday's
        # poll with a file that may already hold today's OI.
        prev_unreadable: list = []
        prev = latest_live_poll(root, symbol, prev_trading_day(day), prev_unreadable)
        result["previous_session_unreadable_polls"] = prev_unreadable
        # Degraded evidence is not conclusive: if the previous session had
        # unreadable polls, the one found may be an older fallback, and "not
        # in it" no longer proves "newly listed". New strikes then stay
        # unexplained (-> not a pass) instead of being excused.
        prev_keys = ({(r["expiry"], float(r["strike"])) for r in prev[2]}
                     if prev and not prev_unreadable else None)
        if now.astimezone(_NY).date() == day:
            result["occ_oi"] = check_occ(rows, day, symbol, out_dir, fetch, prev_keys)
        else:
            result["occ_oi"] = {"status": "not_run",
                                "reason": "run on a later day; OCC's undated file would "
                                          "not refer to this session"}
        result["greeks"] = check_greeks(rows, meta)
        # REPORT-ONLY (audit F12): deliberately NOT in the PASS tuple below.
        try:
            sd = check_short_dated(rows, meta)
            json.dumps(sd, allow_nan=False)           # a NaN here would sink the WHOLE report
            result["short_dated"] = sd
        except Exception as e:                        # a diagnostic must never sink the report
            result["short_dated"] = {"status": "error", "error": f"{type(e).__name__}: {e}"}
        result["arithmetic"] = check_arithmetic(rows, meta, prev[2] if prev else None)
        result["stale_open"] = check_stale_open(root, symbol, day)
        states = (result["occ_oi"]["status"], result["greeks"]["status"],
                  result["arithmetic"]["status"])
        # PASS only on the explicit success tuple. not_run / insufficient are
        # "incomplete", never a pass (a first version let them through).
        if (states == ("match", "consistent", "reconciled") and not unreadable
                and not prev_unreadable):
            result["status"] = "pass"
        elif "error" in states:
            result["status"] = "error"
        elif (any(x in ("mismatch", "investigate") for x in states) or unreadable
              or prev_unreadable):
            result["status"] = "investigate"
        else:
            result["status"] = "incomplete"
        return result
    except Exception as e:
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}: {e}"
        return result
    finally:
        path = out_dir / f"date={day.isoformat()}.json"
        try:
            _atomic_write_json(result, path)
        except ValueError as e:            # a NaN/inf slipped into the result
            _atomic_write_json({"date": result["date"], "symbol": symbol,
                                "run_at": result["run_at"], "status": "error",
                                "error": f"result not serialisable: {e}"}, path)
            result["status"] = "error"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Daily GEX accuracy report")
    ap.add_argument("--root", default="data")
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--date", help="session date YYYY-MM-DD (default: today, ET)")
    a = ap.parse_args(argv)
    day = (date.fromisoformat(a.date) if a.date
           else datetime.now(timezone.utc).astimezone(_NY).date())
    res = run(Path(a.root), a.symbol, day)
    print(json.dumps({"msg": "validation", "date": res["date"], "status": res["status"],
                      **{k: res[k].get("status") for k in ("occ_oi", "greeks", "arithmetic")
                         if isinstance(res.get(k), dict)}}), flush=True)
    return 0 if res["status"] in ("pass", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
