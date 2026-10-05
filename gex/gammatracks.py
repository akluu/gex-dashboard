"""Intraday max/min gamma strikes -- our own descriptive extrema.

GEXStream lists "max/min gamma strikes" as a feature but does not define
them (gexstream.com/docs, read 2026-09-30), so the definition here is ours,
checked before building (2026-09-30):

  per poll, n_k = sum over ALL expiries of (call_gamma_oi - put_gamma_oi) at
  strike k (raw gamma: the header's and the net-GEX trend's scope; the
  bucket chips do not apply);
  max-gamma strike = argmax n_k among n_k > 0; min-gamma strike = argmin
  n_k among n_k < 0; "none" when no strike has that sign -- which is not the
  same thing as invalid data.

  * GLOBAL across all strikes. A +-12% restriction would silently redefine
    the metric as spot moves; the window is a display matter only.
  * Ties: nearer that poll's spot, then the lower strike; without a valid
    spot, the lower strike (distances and dollar values then unavailable,
    but the raw extrema and the ratio stay valid).
  * The runner-up of the same sign is kept, with gap = (|winner| -
    |runner-up|) / |winner|; exact ties are flagged. Any warning threshold
    on the page is a display heuristic, not a significance test.
  * Missing, non-finite or boolean strike/gamma in any row -> that poll's
    extrema and ratio are unavailable, not computed from the rest.
  * Descriptive only: calls +, puts - is an assumed dealer sign, and an
    extremum is not a "magnet" or a "pin".

Scale invariance: n_k in dollars is n_k * S^2 * 0.01 with S > 0, so the
argmax/argmin -- and the gaps -- are the same in either unit.
"""
from __future__ import annotations

from .aggregate import _real, gex_ratio


def _pick(items: list[tuple[float, float]], spot: float | None) -> dict | None:
    """Winner and runner-up by |n| among same-sign (strike, n) pairs."""
    if not items:
        return None
    near = (lambda k: abs(k - spot)) if spot else (lambda k: 0.0)
    ranked = sorted(items, key=lambda t: (-abs(t[1]), near(t[0]), t[0]))
    (k, n), rest = ranked[0], ranked[1:]
    out = {"strike": k, "raw": n, "dist": ((k - spot) / spot) if spot else None,
           "runner_up": None, "runner_raw": None, "gap": None, "tied": False}
    if rest:
        k2, n2 = rest[0]
        out.update(runner_up=k2, runner_raw=n2, gap=(abs(n) - abs(n2)) / abs(n), tied=abs(n2) == abs(n))
    return out


def strike_nets(rows) -> tuple[dict[float, float], int]:
    """n_k for one poll's rows (strike, call_gamma_oi, put_gamma_oi), and the
    number of rows with a missing, non-finite or boolean strike/gamma. The
    ONE netting pass shared by the tracks and the heatmap (gex/heatmap.py):
    a caller must treat bad > 0 as "this poll is unavailable"."""
    nets: dict[float, float] = {}
    bad = 0
    for r in rows:
        k, cg, pg = _real(r.get("strike")), _real(r.get("call_gamma_oi")), _real(r.get("put_gamma_oi"))
        if k is None or cg is None or pg is None:
            bad += 1
            continue
        nets[k] = nets.get(k, 0.0) + (cg - pg)
    return nets, bad


def extrema(rows, spot, netted: tuple[dict[float, float], int] | None = None) -> dict:
    """Max/min gamma strikes and the all-expiry GEX ratio of one poll's rows
    (strike, call_gamma_oi, put_gamma_oi). `netted` = strike_nets(rows) when
    the caller already has it, so the rows are netted once."""
    s = _real(spot)
    s = s if s and s > 0 else None
    nets, bad = netted if netted is not None else strike_nets(rows)
    if bad:
        return {"ok": False, "reason": f"{bad} row(s) with missing or non-finite strike/gamma -- "
                                       "extrema and ratio unavailable", "max": None, "min": None, "ratio": None}
    if not nets:
        return {"ok": False, "reason": "no rows", "max": None, "min": None, "ratio": None}
    mx = _pick([(k, n) for k, n in nets.items() if n > 0], s)
    mn = _pick([(k, n) for k, n in nets.items() if n < 0], s)
    return {"ok": True, "reason": None, "max": mx, "min": mn, "ratio": gex_ratio(rows),
            "max_none": None if mx else "no strike with positive net gamma",
            "min_none": None if mn else "no strike with negative net gamma",
            "n_strikes": len(nets), "spot_valid": s is not None}
