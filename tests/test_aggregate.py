"""Correctness tests for the aggregator, run against the REAL archived payload.

These exist because the failure mode for this tool is not crashing -- it is
displaying a plausible-looking wrong number. Every assertion below encodes a
convention from the original design notes that would be invisible if broken.
"""
import gzip, json, sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from gex.aggregate import parse_occ, bucket_for, aggregate, net_exposure, session_date

SAMPLE = Path("/tmp/gexchk.json")
ARCHIVE = Path(__file__).resolve().parents[1] / "data/fixtures/spx_chains"


def load_payload():
    if SAMPLE.exists():
        return json.loads(SAMPLE.read_text())
    cands = sorted(ARCHIVE.glob("*.json"))
    if not cands:
        raise SystemExit("no payload available")
    return json.loads(cands[-1].read_text())


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def main():
    ok = True
    print("OCC symbol parsing")
    ok &= check("SPX call", parse_occ("SPX260918C00200000") == ("SPX", date(2026, 9, 18), "C", 200.0))
    ok &= check("SPX put", parse_occ("SPX260918P05000000") == ("SPX", date(2026, 9, 18), "P", 5000.0))
    ok &= check("fractional strike", parse_occ("SPXW260904C05432500")[3] == 5432.5)
    ok &= check("garbage rejected", parse_occ("nonsense") is None)

    print("expiry bucketing")
    t = date(2026, 9, 4)
    ok &= check("0DTE", bucket_for(date(2026, 9, 4), t) == "0DTE")
    ok &= check("1D", bucket_for(date(2026, 9, 5), t) == "1D")
    ok &= check("2-7D", bucket_for(date(2026, 9, 10), t) == "2-7D")
    ok &= check("expired excluded", bucket_for(date(2026, 9, 3), t) == "expired")

    print("aggregation against the real payload")
    payload = load_payload()
    agg = aggregate(payload)
    ok &= check("spot present", agg["spot"] and agg["spot"] > 0, f"spot={agg['spot']}")
    ok &= check("rows produced", agg["n_rows"] > 100, f"{agg['n_rows']} strike-bucket rows")
    ok &= check("no unparseable symbols", agg["n_unparseable"] == 0,
                f"{agg['n_unparseable']} skipped")

    print("sign conventions -- the ones that silently corrupt output")
    # CBOE put deltas are already negative; if we had re-signed them the total
    # put delta would come out positive.
    put_delta = sum(r["put_delta_oi"] for r in agg["rows"])
    ok &= check("put delta_oi is negative (CBOE pre-signs it)", put_delta < 0,
                f"sum={put_delta:,.0f}")
    # Gamma is positive for both calls and puts before the convention is applied.
    put_gamma = sum(r["put_gamma_oi"] for r in agg["rows"])
    call_gamma = sum(r["call_gamma_oi"] for r in agg["rows"])
    ok &= check("raw gamma_oi positive for both sides", put_gamma > 0 and call_gamma > 0,
                f"call={call_gamma:,.0f} put={put_gamma:,.0f}")

    print("unit convention -- 100x errors survive review, so assert it")
    per_pct = net_exposure(agg, per_pct=True)
    total = net_exposure(agg, per_pct=False)
    ratio = total["net_gex"] / per_pct["net_gex"] if per_pct["net_gex"] else 0
    ok &= check("total = 100x per-1%", abs(ratio - 100.0) < 1e-6, f"ratio={ratio:.4f}")
    ok &= check("unit label present", "1%" in per_pct["unit"], per_pct["unit"])

    print("regression tests for bugs found in review")
    # Review 2026-09-04: root must be retained -- SPX is AM-settled and
    # SPXW PM-settled on the same date, and flip cannot be right without it.
    ok &= check("root retained, SPX vs SPXW distinguishable",
                parse_occ("SPX260918C00200000")[0] == "SPX"
                and parse_occ("SPXW260918C00200000")[0] == "SPXW")
    # A null/garbage symbol used to raise TypeError rather than returning None.
    ok &= check("null symbol returns None, does not raise", parse_occ(None) is None)
    ok &= check("short symbol returns None", parse_occ("ABC") is None)

    # THE VOLUME BUG: zero-OI contracts were skipped entirely, discarding their
    # volume. Measured at 8,444 contracts across 421 series in a real payload.
    # First version of this assertion was WRONG: it compared aggregated volume
    # against (all - zero_oi), ignoring that aggregate() also drops EXPIRED
    # contracts by design -- and 3.5M of the payload's 5.3M volume is expired.
    # The correct invariant is: aggregated == all - expired, exactly.
    opts_all = payload["data"]["options"]
    sd = session_date()
    zero_oi_vol = sum((o.get("volume") or 0) for o in opts_all
                      if not (o.get("open_interest") or 0) and (o.get("volume") or 0))
    exp_vol = 0.0
    for o in opts_all:
        pr = parse_occ(o.get("option"))
        if pr and bucket_for(pr[1], sd) == "expired":
            exp_vol += o.get("volume") or 0
    all_vol = sum((o.get("volume") or 0) for o in opts_all)
    agg_vol = sum(r["call_volume"] + r["put_volume"] for r in agg["rows"])
    ok &= check("volume == all minus expired (zero-OI volume retained)",
                abs(agg_vol - (all_vol - exp_vol)) < 1.0,
                f"agg={agg_vol:,.0f} expected={all_vol-exp_vol:,.0f}; "
                f"{zero_oi_vol:,.0f} of it is zero-OI that used to be dropped")

    # Session date must be Eastern, not UTC -- wrong for 4-5 hours every night.
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    utc_midnight_ish = _dt(2026, 9, 4, 2, 30, tzinfo=_tz.utc)   # 22:30 ET on 09-03
    ok &= check("session date is Eastern, not UTC",
                session_date(utc_midnight_ish) == date(2026, 9, 3),
                f"got {session_date(utc_midnight_ish)}")

    # A broken payload must refuse to produce numbers rather than render flat.
    broken = net_exposure({"spot": None, "rows": [], "problems": ["no spot"]})
    ok &= check("missing spot yields None, not 0.0",
                broken["net_gex"] is None and broken["unit"] == "unavailable")
    ok &= check("clean payload flagged usable", agg["usable"], f"problems={agg['problems']}")

    # Hand-calculated net check -- the previous tests asserted field signs but
    # would still pass if net_exposure() reversed an operation internally.
    hand_gex = sum(r["call_gamma_oi"] - r["put_gamma_oi"] for r in agg["rows"]) * agg["spot"]**2 * 0.01
    hand_dex = sum(r["call_delta_oi"] + r["put_delta_oi"] for r in agg["rows"]) * agg["spot"]
    ok &= check("net GEX matches hand calculation", abs(per_pct["net_gex"] - hand_gex) < 1.0)
    ok &= check("net DEX matches hand calculation", abs(per_pct["net_dex"] - hand_dex) < 1.0)

    print(f"\n  net GEX {per_pct['net_gex']/1e9:+.2f} Bn {per_pct['unit']}")
    print(f"  net DEX {per_pct['net_dex']/1e9:+.2f} Bn USD notional")
    print(f"\n{'ALL PASS' if ok else 'FAILURES PRESENT'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
