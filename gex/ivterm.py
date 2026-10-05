"""IV term structure at fixed deltas, and a fixed-delta skew.

DEFINITIONS (gexstream.com/docs, read 2026-09-30): term structure =
"Implied vol across expirations at fixed deltas: ATM (50Δ) plus the 25Δ and
10Δ call and put wings". IV Ratio: Call IV = Σ(Call IV × Call OI) / Σ(Call
OI), Put IV likewise, IV Ratio = Call IV / (Call IV + Put IV); Net IV = Call
IV − Put IV. The IV is CBOE's model value (delayed), not ours.

Design checked (2026-09-30):
  * Per expiry, IV at the target delta by LINEAR interpolation in delta
    between the two bracketing quote-valid contracts on that side -- never
    extrapolated. "ATM" is the 50Δ CALL, labelled so (averaging call 50Δ and
    put -50Δ mixes strikes and settles nothing).
  * Bracket safeguards: call delta in (0, 1), put delta in (-1, 0); delta must
    fall as strike rises between the bracketing pair; an exact match is used
    as is, duplicates with different IVs are ambiguous; more than one bracket
    (non-monotonic data) is ambiguous; a bracket wider than MAX_DELTA_GAP or
    MAX_STRIKE_GAP of spot is refused (a missing quote must not yield a
    falsely precise wing). Each value carries its endpoints and weight.
  * Deltas: v3's stored per-contract delta (every contract, zero OI too);
    before v3, recovered from the OI-weighted sum where OI > 0 only --
    disclosed as reduced availability.
  * Skew = 25Δ put IV - 25Δ call IV per expiry, in vol points, from the SAME
    snapshot and expiry with the same safeguards -- unavailable if either wing
    is (never a neighbouring expiry or a carried-forward wing).

IV RATIO / NET IV DEFERRED (measured 2026-09-30):
GEXStream's OI-weighted formula is dominated by which contracts are eligible,
which they do not publish. On the 09-30 noon front expiry: all strikes ->
call 63% / put 274% (ratio 0.19); +-10% of spot -> 57/64 (0.47); +-10% and
quote-valid -> 30/20 (0.60) -- "puts richer" or "calls richer" depending on
an unstated rule, and even filtered, the two baskets hold different strikes
and deltas. If ever shown, only as a labelled "filtered OI-weighted IV
comparison" diagnostic with its sensitivity, never one headline ratio.
"""
from __future__ import annotations

import math

TARGETS = (("50Δ call", "C", 0.50), ("25Δ call", "C", 0.25), ("10Δ call", "C", 0.10),
           ("25Δ put", "P", -0.25), ("10Δ put", "P", -0.10))
MAX_REL_SPREAD = 0.20         # same display-quality threshold as the IV smile
MAX_DELTA_GAP = 0.15          # bracket endpoints at most this far apart in delta
MAX_STRIKE_GAP = 0.03         # ... and at most 3% of spot apart in strike
SIDES = (("C", "call_"), ("P", "put_"))


def _real(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v) if math.isfinite(v) else None


def side_points(rows, opt: str, stored_delta: bool):
    """Quote-valid (strike, delta, iv) for one side of one expiry, in strike
    order, plus exclusion counts. `stored_delta` comes from the poll's SCHEMA
    (v3 has per-contract delta): stored deltas are used as they are -- a
    missing one is unavailable, never recovered; before v3 the delta is
    recovered from the OI-weighted sum where OI > 0."""
    p = "call_" if opt == "C" else "put_"
    pts, excluded = [], {}
    for r in rows:
        k = _real(r.get("strike"))
        iv, bid, ask = _real(r.get(p + "iv")), _real(r.get(p + "bid")), _real(r.get(p + "ask"))
        if stored_delta:
            d = _real(r.get(p + "delta"))
        else:
            oi, doi = _real(r.get(p + "oi")), _real(r.get(p + "delta_oi"))
            d = doi / (oi * 100.0) if oi and oi > 0 and doi is not None else None
        why = None
        if k is None:
            why = "no strike"
        elif iv is None or iv <= 0:
            why = "no IV"
        elif bid is None or ask is None or bid <= 0 or ask <= bid:
            why = "no two-sided quote"
        elif (ask - bid) / ((ask + bid) / 2.0) > MAX_REL_SPREAD:
            why = "spread > 20% of mid"
        elif d is None:
            why = "no delta"
        elif not ((0 < d < 1) if opt == "C" else (-1 < d < 0)):
            why = "delta outside the side's range"
        if why:
            excluded[why] = excluded.get(why, 0) + 1
        else:
            pts.append((k, d, iv))
    pts.sort()
    return pts, excluded


def at_delta(pts, target: float, spot: float) -> dict:
    """IV at `target` delta from strike-ordered (strike, delta, iv) points."""
    ivs_at = {}
    for p in pts:
        ivs_at.setdefault(p[1], set()).add(p[2])
    exact = [p for p in pts if abs(p[1] - target) < 1e-12]
    if exact:
        ivs = {p[2] for p in exact}
        if len(ivs) > 1:
            return {"iv": None, "reason": "ambiguous: duplicate deltas with different IVs"}
        return {"iv": exact[0][2], "strikes": [exact[0][0]], "deltas": [exact[0][1]], "weight": 0.0}
    brackets = [(a, b) for a, b in zip(pts, pts[1:])
                if a[1] > b[1] and a[1] > target > b[1]]          # delta must FALL as strike rises
    if not brackets:
        return {"iv": None, "reason": "not bracketed (no extrapolation)"}
    if len(brackets) > 1:
        return {"iv": None, "reason": f"ambiguous: {len(brackets)} brackets (non-monotonic deltas)"}
    (ka, da, iva), (kb, db, ivb) = brackets[0]
    if len(ivs_at[da]) > 1 or len(ivs_at[db]) > 1:              # an endpoint's IV is itself ambiguous
        return {"iv": None, "reason": "ambiguous: duplicate deltas with different IVs at a bracket endpoint"}
    if da - db > MAX_DELTA_GAP + 1e-12 or (kb - ka) / spot > MAX_STRIKE_GAP + 1e-12:   # at the limit: allowed
        return {"iv": None, "reason": f"bracket too wide ({ka}-{kb}, delta {da:.2f}-{db:.2f})"}
    w = (da - target) / (da - db)
    return {"iv": iva + w * (ivb - iva), "strikes": [ka, kb], "deltas": [da, db], "weight": w}


def expiry_curve(rows, spot: float, stored_delta: bool) -> dict:
    """All five targets for one expiry's rows."""
    out, excluded = {}, {}
    cache = {}
    for opt in ("C", "P"):
        pts, ex = side_points(rows, opt, stored_delta)
        cache[opt] = pts
        for k, v in ex.items():
            excluded[f"{opt} {k}"] = v
    for label, opt, target in TARGETS:
        out[label] = at_delta(cache[opt], target, spot)
    p25, c25 = out["25Δ put"]["iv"], out["25Δ call"]["iv"]
    skew = ({"vol_pts": (p25 - c25) * 100.0} if p25 is not None and c25 is not None else
            {"vol_pts": None, "reason": "a 25Δ wing is unavailable in this expiry"})
    return {"values": out, "skew_25": skew, "n_valid": {"C": len(cache["C"]), "P": len(cache["P"])},
            "excluded": excluded,
            "delta_source": ("stored per-contract (v3)" if stored_delta else
                             "recovered from OI-weighted sums (pre-v3): zero-OI contracts unavailable")}

