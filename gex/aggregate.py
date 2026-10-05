"""One-pass aggregation of a CBOE chain into per-strike exposure.

NO PANDAS IN THE HOT PATH. Measured on the real 12.7 MB / 28,492-contract
payload: this approach costs ~12-48 ms, while an earlier pandas parser's
per-expiration DataFrame construction costs ~848 ms -- more than
the entire rest of the pipeline including JSON decode.

Measured at schema v1 (bucketed rows): ~807 strikes, ~75 KB compact JSON
against a 12.7 MB input. Rows now keep the exact expiry (and, from v4, the
root): ~6.6k rows per SPY snapshot / ~14.6k per SPX, about 1.6 / 2.5 MB of
JSON to the page against 6 / 13 MB in. Either way the browser never sees a
contract.

UNITS -- read the original design notes before changing:
  `gamma * OI * 100 * S^2` is TOTAL DOLLAR GAMMA.
  GEX is conventionally quoted PER 1% underlying move, i.e. x 0.01.
  We store the unit-free `gamma * OI * 100` and derive on demand, so the UI
  can label whichever convention it shows. Being 100x off while remaining
  internally consistent is exactly the error that survives review.

SIGN CONVENTIONS:
  - calls +, puts - is an INDUSTRY POSITIONING CONVENTION, not observed
    dealer inventory. Open interest does not reveal who holds which side.
  - CBOE's put deltas are ALREADY NEGATIVE. Do not re-apply a put sign to
    delta. (Gamma is positive for both, hence the explicit sign there.)
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

# US options sessions are Eastern. A fixed -04:00 offset was used here first
# and was WRONG for half the year: under EST the session gate collected an
# hour before the open and stopped at 16:00 ET, dropping the last 25 minutes
# of trading and the entire post-close tail. Verified against 2027-01-15.
# zoneinfo handles the transitions; the fixed offset is only a fallback for
# a system with no tz database.
try:
    from zoneinfo import ZoneInfo
    _NY = ZoneInfo("America/New_York")
except Exception:                                     # pragma: no cover
    _NY = timezone(timedelta(hours=-5))


def session_date(now: datetime | None = None) -> date:
    """Today's trading date in New York terms."""
    now = now or datetime.now(timezone.utc)
    return now.astimezone(_NY).date()

# OCC symbol: SPX  260918 C 00200000  -> root, yymmdd, type, strike*1000
MIN_PLAUSIBLE_CONTRACTS = 2000   # SPY ~12.5k, SPX ~28k; a partial chain is a defect
# Fraction of open-interest-bearing contracts that must carry a nonzero gamma.
# CAUGHT IN PRODUCTION 2026-09-04 13:42 UTC, 12 minutes into the first live
# session: CBOE served 12,456 contracts with REAL open interest (9,635 nonzero)
# but EVERY greek and quote zeroed -- gamma 0.0, delta 0.0, iv 0.0, bid/ask 0.0.
# The contract-count guard passed (12,456 >> 2,000) and the null-check passed
# (they are explicitly 0.0, not None), so `gamma or 0.0` stored a snapshot
# reporting net GEX of exactly zero -- indistinguishable on a chart from a
# genuinely flat market.
#
# CAUSE UNKNOWN, and the obvious guess is WRONG. The first hypothesis was
# "greeks not yet computed at the open", but the log refutes it: polls at
# 13:30-13:41 all carried healthy greeks (~0.867 Bn net GEX) and the zeroing
# began at 13:42, twelve minutes AFTER the open, then persisted. So it is a
# mid-session condition, not a startup state. Possibly a CDN edge serving a
# degraded object. Do not encode the open-time theory anywhere.
MIN_GREEK_COVERAGE = 0.10

# The OCC roots of AM-settled (3rd-Friday, settled at the open) S&P 500 index
# options -- {"SPX"}; SPXW is PM-settled. Defined once, in market_calendar,
# next to their settlement clocks. Only these roots get the settlement-day
# missing-greeks allowance in aggregate().
from .market_calendar import AM_SETTLED_ROOTS

_TYPE_POS = -9
_STRIKE_LEN = 8
_DATE_LEN = 6


def parse_occ(sym: str) -> tuple[str, date, str, float] | None:
    """(root, expiry, 'C'|'P', strike) from an OCC option symbol, or None.

    Parsed from the RIGHT, which is what makes variable-length roots (SPX vs
    SPXW vs adjusted roots) safe -- the strike is always 8 digits with 3
    implied decimals and the date always 6 before the type character.

    The ROOT is returned rather than discarded: SPX is AM-settled and SPXW is
    PM-settled on the same calendar date, and phase 4's gamma flip cannot be
    correct without telling them apart.
    """
    if not sym or not isinstance(sym, str) or len(sym) < _STRIKE_LEN + _DATE_LEN + 1:
        return None
    try:
        strike = int(sym[-_STRIKE_LEN:]) / 1000.0
        opt_type = sym[_TYPE_POS]
        if opt_type not in ("C", "P"):
            return None
        d = sym[_TYPE_POS - _DATE_LEN:_TYPE_POS]
        root = sym[:_TYPE_POS - _DATE_LEN]
        if not root:
            return None
        expiry = date(2000 + int(d[0:2]), int(d[2:4]), int(d[4:6]))
        return root, expiry, opt_type, strike
    except (ValueError, IndexError, TypeError):
        return None


def bucket_for(expiry: date, today: date) -> str:
    """Expiry buckets. 0DTE is called out separately because it dominates SPX
    gamma and is the exact cohort an earlier gamma-flip implementation (outside
    this tool) silently dropped."""
    dte = (expiry - today).days
    if dte < 0:
        return "expired"
    if dte == 0:
        return "0DTE"
    if dte == 1:
        return "1D"
    if dte <= 7:
        return "2-7D"
    if dte <= 30:
        return "8-30D"
    return "30D+"


def _num(v) -> float | None:
    """A quote field as float, or None if absent or not numeric."""
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass
class StrikeRow:
    strike: float
    expiry: str = ""            # ISO date. Stored because a BUCKET CANNOT BE
                                # UNGROUPED LATER: without this, every historical
                                # heatmap and IV surface is permanently limited
                                # to five coarse ranges, and CBOE offers no
                                # backfill to repair it.
    root: str = ""              # OCC root (schema v4). PART OF THE ROW KEY: SPX
                                # (AM-settled monthlies) and SPXW (PM-settled)
                                # list the same strike on the same date; keyed on
                                # (strike, expiry) alone, ~3,096 sides per SPX
                                # poll collided and lost their quotes/greeks.
                                # Settlement is derived from the root at READ
                                # time, never stored.
    call_gamma_oi: float = 0.0   # sum(gamma * OI * 100) over calls
    put_gamma_oi: float = 0.0
    call_delta_oi: float = 0.0   # sum(delta * OI * 100) over calls
    put_delta_oi: float = 0.0    # deltas already negative from CBOE
    call_oi: float = 0.0
    put_oi: float = 0.0
    call_volume: float = 0.0
    put_volume: float = 0.0
    # Per-contract quotes (schema v2). ASSIGNED, never summed: SPY has one
    # root, so each (strike, expiry, side) is exactly one contract -- verified
    # on the 2026-09-28 payload, 0 duplicates in 13,294. A collision is
    # counted and blanks that side rather than silently keeping either value
    # (see n_quote_collisions for what it means for the exposure sums).
    # Stored exactly as CBOE sends them: an explicit 0.0 IV (no model value,
    # ~17% of contracts) stays 0.0, so readers must filter `iv > 0`. None
    # means the key was absent, a different fact from 0.0.
    call_iv: float | None = None
    put_iv: float | None = None
    call_bid: float | None = None
    call_ask: float | None = None
    put_bid: float | None = None
    put_ask: float | None = None
    # Per-contract greeks (schema v3), same assignment/collision rules as the
    # quotes above, and captured for ZERO-OI contracts too. Until v3 the only
    # gamma/delta on disk were the OI-weighted sums, which are 0 for a
    # zero-OI contract -- so a volume-weighted exposure (vGEX) or a delta-
    # based term structure could not be recovered for those, ever.
    call_gamma: float | None = None
    put_gamma: float | None = None
    call_delta: float | None = None
    put_delta: float | None = None


def aggregate(payload: dict, today: date | None = None,
              buckets: tuple[str, ...] | None = None) -> dict:
    """Aggregate a raw CBOE payload to one row per (strike, expiry, root),
    calls and puts side by side, each row labelled with its expiry bucket.

    Returns a JSON-serialisable dict with the unit-free sums plus the spot,
    so any GEX/DEX unit can be derived downstream without re-fetching.
    """
    data = payload.get("data", {})
    opts = data.get("options") or []
    spot = data.get("current_price")
    today = today or session_date()

    # Schema guard: a payload we cannot interpret must FAIL VISIBLY rather
    # than aggregate to plausible zeros. Silent zeros are indistinguishable
    # from a genuinely flat market on a chart.
    problems: list[str] = []
    if spot is None:
        problems.append("missing data.current_price -- cannot scale GEX/DEX")
    if not opts:
        problems.append("missing or empty data.options")
    # A TRUNCATED chain is the dangerous case: it parses, aggregates, and
    # reports usable=True with near-zero exposure -- indistinguishable from a
    # genuinely flat market when read back. Verified: a 5-contract payload
    # returned usable=True, rows=0, problems=[]. SPY carries ~12.5k contracts
    # and SPX ~28k, so anything under a few thousand is not a real chain.
    elif len(opts) < MIN_PLAUSIBLE_CONTRACTS:
        problems.append(f"implausibly small chain: {len(opts)} contracts "
                        f"(expected >= {MIN_PLAUSIBLE_CONTRACTS})")

    # (strike, expiry, root) -> StrikeRow. Keyed on exact expiry rather than
    # bucket: measured at 6,086 rows/poll for SPY (33 expirations) versus 1,261
    # bucketed, i.e. ~181 KB/poll and ~18.5 GB/yr, which is ~3 years on a 60 GB
    # disk. The root joined the key in schema v4 (see StrikeRow.root).
    grid: dict[tuple[float, str, str], StrikeRow] = {}
    skipped = 0
    seen_sides: set[tuple[float, str, str, str]] = set()
    collided: set[tuple[float, str, str, str]] = set()

    n_missing_greeks = 0
    # AM-settled SPX contracts on their own expiration day stop trading before
    # the session (they settle at the open), and what CBOE sends for them then
    # is UNOBSERVED -- none of the archived payloads fall on such a Friday (the
    # first one collected is 2026-10-16). If it is OI without greeks, counting
    # them as a problem would reject EVERY poll that day. So they are counted
    # here as a quality warning instead; their per-contract values are stored
    # as received (a missing greek stays null -- v3 null != 0) and BOTH their
    # weighted exposures are 0, which matches how the display treats an AM
    # contract on settlement day (excluded from the open).
    # Every other missing-greek case stays fatal. Revisit with the real
    # 2026-10-16 payloads, which the raw-every-poll window keeps.
    n_am_expiring_missing_greeks = 0
    for o in opts:
        parsed = parse_occ(o.get("option"))
        if parsed is None:
            skipped += 1
            continue
        root, expiry, opt_type, strike = parsed
        b = bucket_for(expiry, today)
        if b == "expired" or (buckets and b not in buckets):
            continue

        oi = o.get("open_interest") or 0.0
        vol = o.get("volume") or 0.0
        gamma = o.get("gamma")
        delta = o.get("delta")
        if oi and (gamma is None or delta is None):
            if root in AM_SETTLED_ROOTS and b == "0DTE":
                n_am_expiring_missing_greeks += 1
                # BOTH weighted exposures 0, even if one greek did arrive:
                # half a contract's exposure is worse than none (v4
                # review). The per-contract values below are taken from `o`
                # and stored exactly as received.
                gamma = delta = None
            else:
                n_missing_greeks += 1
        gamma = gamma or 0.0
        delta = delta or 0.0

        key = (strike, expiry.isoformat(), root)
        row = grid.get(key)
        if row is None:
            row = StrikeRow(strike=strike, expiry=expiry.isoformat(), root=root)
            grid[key] = row

        # VOLUME IS AGGREGATED REGARDLESS OF OPEN INTEREST. An earlier version
        # skipped the whole contract when OI was zero, which silently discarded
        # 8,444 contracts of volume across 421 series in a single real payload
        # -- day-traded strikes that opened and closed intraday are exactly the
        # ones with volume and no OI, and they are interesting.
        if opt_type == "C":
            row.call_volume += vol
        else:
            row.put_volume += vol

        # Quotes, like volume, are captured regardless of open interest: a
        # zero-OI strike still has a live bid/ask and a model IV, and the IV
        # smile needs those wings.
        side = (strike, row.expiry, root, opt_type)
        if side in seen_sides:
            collided.add(side)
        else:
            seen_sides.add(side)
            p = "call_" if opt_type == "C" else "put_"
            setattr(row, p + "iv", _num(o.get("iv")))
            setattr(row, p + "bid", _num(o.get("bid")))
            setattr(row, p + "ask", _num(o.get("ask")))
            setattr(row, p + "gamma", _num(o.get("gamma")))
            setattr(row, p + "delta", _num(o.get("delta")))

        # Exposure, however, genuinely requires open interest.
        if not oi:
            continue
        g_oi = gamma * oi * 100.0
        d_oi = delta * oi * 100.0
        if opt_type == "C":
            row.call_gamma_oi += g_oi
            row.call_delta_oi += d_oi
            row.call_oi += oi
        else:
            row.put_gamma_oi += g_oi
            row.put_delta_oi += d_oi     # CBOE pre-signs put delta
            row.put_oi += oi

    for (strike, exp, root, opt_type) in collided:
        p = "call_" if opt_type == "C" else "put_"
        r = grid[(strike, exp, root)]
        for f in ("iv", "bid", "ask", "gamma", "delta"):
            setattr(r, p + f, None)

    rows = []
    for key in sorted(grid):
        r = grid[key]
        if True:
            rows.append({
                "strike": r.strike, "expiry": r.expiry, "root": r.root,
                "bucket": bucket_for(date.fromisoformat(r.expiry), today),
                "call_gamma_oi": r.call_gamma_oi, "put_gamma_oi": r.put_gamma_oi,
                "call_delta_oi": r.call_delta_oi, "put_delta_oi": r.put_delta_oi,
                "call_oi": r.call_oi, "put_oi": r.put_oi,
                "call_volume": r.call_volume, "put_volume": r.put_volume,
                "call_iv": r.call_iv, "put_iv": r.put_iv,
                "call_bid": r.call_bid, "call_ask": r.call_ask,
                "put_bid": r.put_bid, "put_ask": r.put_ask,
                "call_gamma": r.call_gamma, "put_gamma": r.put_gamma,
                "call_delta": r.call_delta, "put_delta": r.put_delta,
            })

    # Greeks-zeroed guard. Not the same as the missing-greeks counter below:
    # these arrive as explicit 0.0, so no null check can see them.
    oi_rows = sum(1 for o in opts if (o.get("open_interest") or 0))
    if oi_rows:
        with_gamma = sum(1 for o in opts
                         if (o.get("open_interest") or 0) and (o.get("gamma") or 0))
        coverage = with_gamma / oi_rows
        if coverage < MIN_GREEK_COVERAGE:
            problems.append(
                f"greeks not populated: only {with_gamma}/{oi_rows} "
                f"({coverage:.1%}) of open-interest contracts have nonzero gamma "
                f"-- CBOE intermittently serves greeks-zeroed payloads; cause unknown")

    if not rows and opts:
        # Everything filtered out (e.g. an all-expired chain). Metadata would
        # otherwise be written alongside NO aggregate while the collector logs
        # a successful "stored".
        problems.append("no unexpired rows produced from a non-empty chain")
    if skipped:
        problems.append(f"{skipped} unparseable option symbols")
    if n_missing_greeks:
        problems.append(f"{n_missing_greeks} contracts with OI but missing gamma/delta")

    return {
        "spot": spot,
        "session_date": today.isoformat(),
        "n_contracts": len(opts),
        "n_rows": len(rows),
        "n_unparseable": skipped,
        # A QUALITY WARNING, deliberately not a `problems` entry (which would
        # mark the snapshot unusable and stop collection). Nonzero means the
        # (strike, expiry, root, side) key held more than one record, i.e.
        # CBOE repeated the same OCC symbol: that side's quotes are blanked,
        # and its exposure/OI/volume are SUMMED across the records -- likely
        # DOUBLE-COUNTED, so treat that key's exposure as suspect too. Before
        # v4 the key had no root, so this also counted (and blanked) every
        # SPX/SPXW same-date pair -- ~3,096 per SPX poll; 0 on every real SPY
        # payload.
        "n_quote_collisions": len(collided),
        # A QUALITY WARNING (see the comment above the contract loop): AM SPX
        # contracts expiring today that carry OI but no gamma/delta.
        "n_am_expiring_missing_greeks": n_am_expiring_missing_greeks,
        "problems": problems,          # non-empty => do NOT display as clean
        "usable": not problems,
        "rows": rows,
    }


def _real(v) -> float | None:
    """v as a finite float, else None. bool is excluded (it is an int)."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v) if math.isfinite(v) else None


def gex_ratio(rows) -> float | None:
    """GEXStream's GEX Ratio (gexstream.com/docs, read 2026-09-30): net calls
    against puts AT EACH STRIKE first -- across every row passed, i.e. across
    whichever expiries the caller selected -- then
    sum(positive strike nets) / sum(|strike nets|).

    Netting per strike is what separates it from gross call gamma over gross
    gamma: +10 and -9 at one strike (two expiries) with -2 at another is 1/3,
    not 10/21. Unit-free -- the S^2 * 0.01 dollar scale cancels -- so it needs
    no spot. 0.5 means the strike nets cancel (zero net gamma) under the
    calls +, puts - convention, which is an assumed sign, not observed dealer
    positioning.

    None (unavailable -- never 0.0) with no rows, a zero denominator, or any
    missing or non-finite input.
    """
    nets: dict[float, float] = {}
    for r in rows:
        k, cg, pg = _real(r.get("strike")), _real(r.get("call_gamma_oi")), _real(r.get("put_gamma_oi"))
        if k is None or cg is None or pg is None:
            return None
        nets[k] = nets.get(k, 0.0) + (cg - pg)
    den = sum(abs(n) for n in nets.values())
    if not den or not math.isfinite(den):
        return None
    return sum(n for n in nets.values() if n > 0) / den


def net_exposure(agg: dict, per_pct: bool = True) -> dict:
    """Totals. GEX sign convention: calls +, puts -. DEX: deltas already signed.

    per_pct=True returns GEX in dollars per 1% underlying move (the common
    quoting convention); False returns total dollar gamma.

    Also returns, over the same rows (all expiries, all strikes), the raw
    units GEXStream publishes -- `net_gamma_raw` = sum(gamma*OI*100), delta
    shares per $1 move; `net_delta_shares` = sum(delta*OI*100) -- and
    `gex_ratio`. Those need no spot, so they are computed even when the
    dollar figures are refused; each is None if it is not
    finite. `net_gex`/`net_dex` are the raw sums times the same scale as
    before, so stored metadata is unchanged to the bit.
    """
    def sums(rows):
        return (sum(r["call_gamma_oi"] - r["put_gamma_oi"] for r in rows),
                sum(r["call_delta_oi"] + r["put_delta_oi"] for r in rows))

    spot = agg.get("spot")
    if not spot:
        # Returning 0.0 here would render as a flat market rather than an
        # error. Refuse instead. This path never raised before, so a
        # malformed row only makes the raw figures unavailable here.
        try:
            raw_g, raw_d = sums(agg.get("rows") or [])
        except (KeyError, TypeError):
            raw_g = raw_d = None
        return {"net_gex": None, "net_dex": None, "spot": None,
                "unit": "unavailable", "problems": agg.get("problems", ["no spot"]),
                "net_gamma_raw": _real(raw_g), "net_delta_shares": _real(raw_d),
                "gex_ratio": gex_ratio(agg.get("rows") or [])}
    # With a spot, a malformed row raises exactly as it always did (the
    # collector logs "cycle failed") rather than being stored as None.
    raw_g, raw_d = sums(agg["rows"])
    raw = {"net_gamma_raw": _real(raw_g), "net_delta_shares": _real(raw_d),
           "gex_ratio": gex_ratio(agg["rows"])}
    scale = spot * spot * (0.01 if per_pct else 1.0)
    gex = raw_g * scale
    dex = raw_d * spot
    return {
        "net_gex": gex,
        "net_dex": dex,
        "unit": "USD per 1% move" if per_pct else "USD total",
        "spot": spot,
        "problems": agg.get("problems", []),
        **raw,
    }
