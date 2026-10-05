"""Intraday GEX heatmap -- per-strike all-expiry net gamma, poll by poll.

Step 5 of the GEXStream build order. Design checked before any
code (2026-10-01); the points that shaped it:

A cell is exactly gex/gammatracks.py's n_k, from the SAME netting pass
(strike_nets): for one poll, the sum over ALL expiries of (call_gamma_oi -
put_gamma_oi) at strike k, in raw gamma units (gamma x OI x 100, delta shares
per $1 move). The bucket chips do not apply -- the scope of the header, the
trend and the tracks. So a poll's max-gamma strike is its most positive cell
WHEN that strike is inside the row window; a global extremum can lie outside
it (the tracks then show an edge arrow, the heatmap simply has no such row).

  * A poll with any missing, non-finite or boolean strike/gamma has no
    values (status "invalid"), never values computed from the other rows.
  * A strike absent from a poll is None, never 0 -- "no data" and "zero net
    gamma" are different things and the page draws them differently.
  * Rows: every strike seen in any column within [min spot x (1 - W),
    max spot x (1 + W)] over the columns with a valid spot; the per-poll
    cache keeps every strike, so a large move cannot leave a row half-empty
    because the cache dropped it.
  * Values are rounded to 4 decimals: CBOE's per-contract gamma has 4
    decimals (all 13,274 sides checked on a 2026-09-30 poll), so gamma x OI x
    100 is a multiple of 0.01 and the rounding removes only binary float
    noise (measured <= 1e-10) -- not a new numerical convention.
  * Descriptive only: calls +, puts - is an assumed dealer sign.
"""
from __future__ import annotations

import math

WINDOW = 0.12      # rows: +-12% around the session's spot range (the page zooms)
DECIMALS = 4


def _spot(v) -> float | None:
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0 else None


def grid(columns: list[dict], window: float = WINDOW) -> dict:
    """columns: one dict per placed poll, in time order, with t, spot,
    newest_trade, age_s, status, reason and nets (dict | None; None unless
    status == "ok"). Returns the strike rows and one value list per column
    aligned to them (None where the strike is absent or the column has no
    values)."""
    spots = [s for c in columns if c.get("nets") is not None for s in [_spot(c.get("spot"))] if s]
    if not spots:
        rows: list[float] = []
        reason = "no snapshot with both values and a valid spot this session: no strike window"
    else:
        lo, hi = min(spots) * (1 - window), max(spots) * (1 + window)
        rows = sorted({k for c in columns if c.get("nets") for k in c["nets"] if lo <= k <= hi})
        reason = None if rows else "no strike inside the window"
    out = []
    for c in columns:
        nets = c.get("nets")
        vals = None if nets is None else \
            [None if (v := nets.get(k)) is None else round(v, DECIMALS) for k in rows]
        out.append({"t": c.get("t"), "spot": c.get("spot"), "newest_trade": c.get("newest_trade"),
                    "age_s": c.get("age_s"), "status": c.get("status"), "reason": c.get("reason"),
                    "values": vals})
    return {"strikes": rows, "columns": out, "window": window, "reason": reason}
