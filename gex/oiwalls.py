"""Front-expiry open interest -- GEXStream's "OI walls", computed on our data.

DEFINITIONS (gexstream.com/docs, re-read 2026-09-30): per-strike OI = call OI
+ put OI; Max OI strike = the largest combined OI; both on the FRONT
expiration. Descriptive only: open interest does not say who holds a
contract or whether it was bought or sold, and a large strike is not a
"pin" or a "magnet" (GEXStream's words, deliberately not repeated).

Design checked BEFORE building (2026-09-30):
  * The GLOBAL Max OI is kept even far from spot -- monthly expiries carry
    legacy positions (2026-09-18: top strikes 515-525 with spot ~760). Its
    distance from spot is shown, and a separately labelled near-spot top 3
    sits beside it; it never replaces the global definition.
  * OI change is SAME-EXPIRY: the front expiry's OI now vs that expiry in the
    PREVIOUS TRADING SESSION's snapshot. GEXStream's "OI Chg%" compares the
    front chain with the previous front chain -- a different expiry every
    session for SPY (-91% to +1078% on our data, driven by expiry kind) --
    and is not computed. A baseline session that was not captured, an
    expiry absent from it, or a zero baseline is UNAVAILABLE, never zero; a
    missing session is never skipped over and still called day-over-day.
  * Invalid OI (missing, non-finite, negative, fractional) makes the figure
    unavailable instead of counting as 0. LIMIT: the aggregator already
    stores a MISSING open_interest as 0.0 (`o.get("open_interest") or 0.0`),
    so a stored zero cannot be told from a missing value -- only what reached
    disk can be checked here.
  * Expiry kinds come from the calendar (3rd-Friday monthly, month-/quarter-
    end, end of week), not the weekday alone; labels may overlap.

SERIES, not dates (SPX, schema v4, 2026-10-03): open interest is
grouped by (expiry, root), because SPX lists an AM-settled (SPX) and a
PM-settled (SPXW) series on the same date. "Front" is the earliest SETTLEMENT
among the series the reader kept (an AM series is dropped on its own
settlement day), "next" the following series -- possibly the same date. The
OI change compares that exact series with itself in the previous trading
session, so an AM leg settling can never read as an OI drop. Rows without a
root (synthetic tests) keep the plain date as their key; the server gives
pre-v4 SPY rows root 'SPY', so SPY keys stay comparable across v3/v4.

VERIFIED ON THE STORE (18 day partitions, 2026-09-04..09-29): OI is constant
within a session (0 of ~6.6k keys differ first vs last poll), and the front
expiry is the session date on every trading day.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

from datetime import time as dtime

from .market_calendar import (AM_SETTLED_ROOTS, PM_SETTLED_INDEX_ROOTS, covered, holiday_name,
                              settlement_time)

WINDOW = 0.12        # the +-12% of spot every by-strike panel shows
TOP_N = 3


# ---- trading calendar --------------------------------------------------------

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and holiday_name(d) is None


def previous_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while not is_trading_day(p):
        p -= timedelta(days=1)
    return p


def next_trading_day(d: date) -> date:
    n = d + timedelta(days=1)
    while not is_trading_day(n):
        n += timedelta(days=1)
    return n


def trading_days(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def expiry_labels(d: date) -> list[str]:
    """Calendar-derived kinds of an expiry date; they can overlap. ["daily"]
    if none applies. A standard monthly is the 3rd Friday, moved to the
    preceding trading day when that Friday is a holiday (Juneteenth
    2026-06-19 is one)."""
    if not covered(d):
        return ["unclassified (holiday table ends " + "2028-12-31)"]
    labels = []
    first = d.replace(day=1)
    third_fri = first + timedelta(days=(4 - first.weekday()) % 7 + 14)
    monthly = third_fri if is_trading_day(third_fri) else previous_trading_day(third_fri)
    if d == monthly:
        labels.append("monthly (3rd Fri)" if d == third_fri else "monthly (3rd Fri, holiday-shifted)")
    nxt = next_trading_day(d)
    if nxt.month != d.month:
        labels.append("quarter-end" if d.month in (3, 6, 9, 12) else "month-end")
    if nxt.isocalendar()[:2] != d.isocalendar()[:2]:
        labels.append("end of week")
    return labels or ["daily"]


# ---- one snapshot ---------------------------------------------------------------

def _count(v) -> float | None:
    """A stored open interest as a non-negative whole number, else None."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    if not math.isfinite(v) or v < 0 or v != int(v):
        return None
    return float(v)


def _real(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v) if math.isfinite(v) else None


def _rank(items: list[tuple[float, float, float, float]], spot: float | None, n: int) -> list[dict]:
    """Top n of (strike, oi, call, put) by OI; ties broken nearer spot, then
    lower strike. `tied` flags an entry whose OI another strike matches
    exactly -- the tie-break, not the data, decides its place."""
    counts: dict[float, int] = {}
    for _, oi, _, _ in items:
        counts[oi] = counts.get(oi, 0) + 1
    near = (lambda k: abs(k - spot)) if spot else (lambda k: 0.0)
    ranked = sorted(items, key=lambda t: (-t[1], near(t[0]), t[0]))[:n]
    return [{"strike": k, "oi": oi, "call_oi": c, "put_oi": p,
             "dist": ((k - spot) / spot) if spot else None, "tied": counts[oi] > 1}
            for k, oi, c, p in ranked]


REGULAR_CLOSE = dtime(16, 15)      # SPY options close; settlement_time's default


def series_key(expiry: str, root: str | None) -> str:
    """The OI series identity: the date alone without a root, else 'date|ROOT'."""
    return expiry if not root else f"{expiry}|{root}"


def settle_label(root: str | None) -> str | None:
    return "AM" if root in AM_SETTLED_ROOTS else "PM" if root in PM_SETTLED_INDEX_ROOTS else None


def _series_label(expiry: str, root: str | None) -> str:
    lab = settle_label(root)
    return f"{expiry} {lab}" if lab else expiry


def summarize(rows, spot, session: date, window: float = WINDOW, top_n: int = TOP_N) -> dict:
    """Front-series OI summary of one snapshot's aggregate rows (strike,
    expiry, call_oi, put_oi, optional root). `expiry_totals` (keyed by
    series_key) covers EVERY series, so a later session can use this one as
    its same-series baseline."""
    spot = _real(spot)
    spot = spot if spot and spot > 0 else None
    per: dict[str, dict[float, list[float]]] = {}
    invalid: dict[str, int] = {}
    meta: dict[str, tuple[str, str | None]] = {}          # key -> (expiry, root)
    for r in rows:
        e, k, root = r.get("expiry"), _real(r.get("strike")), r.get("root")
        if not e:
            invalid["?"] = invalid.get("?", 0) + 1     # scope unknown: poisons every total
            continue
        key = series_key(e, root)
        meta[key] = (e, root)
        c, p = _count(r.get("call_oi")), _count(r.get("put_oi"))
        if k is None or c is None or p is None:        # this series only
            invalid[key] = invalid.get(key, 0) + 1
            continue
        cell = per.setdefault(key, {}).setdefault(k, [0.0, 0.0])
        cell[0] += c
        cell[1] += p
    expiry_totals = {key: (None if invalid.get(key) else sum(c + p for c, p in ks.values()))
                     for key, ks in per.items()}
    for key in invalid:
        if key != "?":
            expiry_totals.setdefault(key, None)

    def settles(key):
        e, root = meta[key]
        d = date.fromisoformat(e)
        return (d, settlement_time(root, d, REGULAR_CLOSE), root or "")
    live = sorted((key for key in expiry_totals if meta[key][0] >= session.isoformat()), key=settles)
    out = {"front_expiry": None, "front_root": None, "front_settle": None, "front_label": None,
           "front_series": None, "next_expiry": None, "next_root": None, "next_settle": None,
           "next_label": None, "next_total_oi": None,
           "labels": [], "total_oi": None, "call_oi": None, "put_oi": None,
           "n_strikes": 0, "top": [], "top_near": [], "strikes": [], "spot": spot,
           "expiry_totals": expiry_totals, "problems": []}
    if not live:
        out["problems"].append("no unexpired expiry in this snapshot")
        return out
    front = live[0]
    fe, froot = meta[front]
    out.update(front_expiry=fe, front_root=froot, front_settle=settle_label(froot),
               front_label=_series_label(fe, froot), front_series=front)
    # The next stored series and its CURRENT stored OI (already known -- no
    # need to wait for another session to describe it).
    if len(live) > 1:
        ne, nroot = meta[live[1]]
        out.update(next_expiry=ne, next_root=nroot, next_settle=settle_label(nroot),
                   next_label=_series_label(ne, nroot), next_total_oi=expiry_totals.get(live[1]))
    out["labels"] = expiry_labels(date.fromisoformat(fe))
    if invalid.get("?"):
        # A row with no expiry (or no strike) could belong to ANY expiry, the
        # front included, so no total is trustworthy -- including the ones a
        # later session would use as its baseline.
        out["problems"].append(f"{invalid['?']} row(s) with no expiry or strike -- their scope is "
                               "unknown, so no total can be trusted")
        out["expiry_totals"] = {key: None for key in expiry_totals}
        return out
    if invalid.get(front):
        out["problems"].append(f"{invalid[front]} front-expiry row(s) with missing or invalid open "
                               "interest -- totals and walls unavailable rather than counted as 0")
        return out
    ks = per.get(front, {})
    items = sorted((k, c + p, c, p) for k, (c, p) in ks.items())
    out.update({
        "total_oi": sum(t[1] for t in items),
        "call_oi": sum(t[2] for t in items),
        "put_oi": sum(t[3] for t in items),
        "n_strikes": len(items),
        "strikes": [[k, c, p] for k, _, c, p in items],
        "top": _rank(items, spot, top_n),
    })
    if not out["total_oi"]:
        # A largest-of-zeros is not a wall. (A stored zero may be missing.)
        out["top"] = []
        out["problems"].append("front expiry has zero open interest")
        return out
    if spot:
        out["top_near"] = _rank([t for t in items if abs(t[0] / spot - 1) <= window], spot, top_n)
    return out


def same_expiry_change(cur: dict, base: dict | None, base_session: date,
                       base_reason: str | None = None) -> dict:
    """The front expiry's OI in `cur` vs the SAME expiry in the previous
    trading session's summary. Unavailable (with the reason) whenever the
    baseline is missing -- never 0."""
    e = cur.get("front_series") or cur.get("front_expiry")     # the SAME series, root included
    res = {"available": False, "expiry": cur.get("front_expiry"), "series": cur.get("front_label"),
           "baseline_session": base_session.isoformat(),
           "current": cur.get("total_oi"), "baseline": None, "abs": None, "pct": None, "reason": None}
    if e is None or cur.get("total_oi") is None:
        res["reason"] = "current front-expiry OI unavailable"
    elif base is None:
        res["reason"] = base_reason or f"previous trading session {base_session} not captured"
    elif e not in base.get("expiry_totals", {}):
        res["reason"] = f"{cur.get('front_label') or e} not in the {base_session} snapshot"
    elif base["expiry_totals"][e] is None:
        res["reason"] = f"invalid open interest for {cur.get('front_label') or e} in the {base_session} snapshot"
    else:
        b = base["expiry_totals"][e]
        res.update(available=True, baseline=b, abs=cur["total_oi"] - b,
                   pct=((cur["total_oi"] - b) / b) if b > 0 else None)
        if b <= 0:
            res["reason"] = "zero baseline: percentage unavailable"
    return res
