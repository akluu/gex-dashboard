"""The daily accuracy report: each check passes on real data AND fails when
the thing it checks is broken; every outcome -- including skipped and error
-- is written to disk.

Usage:  python3 tests/test_validate.py [DATA_ROOT] [OCC_FIXTURE]
Defaults: the local VM backup (needs the 2026-09-29 v2 polls and the
2026-09-28 session) and data/fixtures/occ_SPY_2026-09-29.txt (OCC's series
search as fetched 2026-09-29 18:45 UTC). No network: OCC is injected.

The fixture is gitignored with the rest of data/. It is byte-identical to the
VM's ~/gex_data/validation/occ_SPY_2026-09-29_943cc25fd439ffdf.txt (sha256
prefix 943cc25fd439ffdf). It is OCC data and is not distributed with this repository.

Only the two test days are copied into a temp root, and the copy is deleted
at the end: a first version copied the whole ~900 MB store on every run and
left it behind.
"""
import copy, json, shutil, sys, tempfile
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import gex.validate as V

DAY = date(2026, 9, 29)
NOON = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)      # 12:00 ET


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "data/vm_backup/gex_data"
    occ_path = Path(sys.argv[2]) if len(sys.argv) > 2 else HERE.parent / "data/fixtures/occ_SPY_2026-09-29.txt"
    occ_text = occ_path.read_bytes()
    root = Path(tempfile.mkdtemp(prefix="gex_test_validate_"))
    try:
        return _main(src, occ_text, root)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _main(src, occ_text, root):
    for d in ("metadata", "aggregates"):
        for day in (V.prev_trading_day(DAY), DAY):
            part = Path(d) / "symbol=SPY" / f"date={day.isoformat()}"
            shutil.copytree(src / part, root / part)
    fetch_ok = lambda url: (200, occ_text)
    ok = True

    print("OCC parser")
    occ, counts = V.parse_occ_series(occ_text.decode(), "SPY")
    ok &= check("only the SPY product, every row parsed",
                counts == {"rows": 6647, "other_products_skipped": 2471, "unparsed_rows": 0,
                           "duplicate_rows": 0}, str(counts))
    odd = "SPY   \t\t2026\t10\t02\t700\t000\tC \t5\t360000000\n"
    _, c2 = V.parse_occ_series(odd, "SPY")
    ok &= check("a row in an unexpected shape is counted, not guessed", c2["unparsed_rows"] == 1)
    _, c3 = V.parse_occ_series("SPY BAD 10 02 700 000 C P 5 6 360000000\n", "SPY")
    ok &= check("a garbled SPY row with a bad year is counted (not taken for a header)",
                c3["unparsed_rows"] == 1)
    good = "SPY 2026 10 02 700 000 C P 5 6 360000000\n"
    _, c4 = V.parse_occ_series(good + good, "SPY")
    ok &= check("a duplicate series is counted", c4["duplicate_rows"] == 1 and c4["rows"] == 1)
    _, c5 = V.parse_occ_series("SPY 2026 10 02 700 000 C P 5.5 6 360000000\n", "SPY")
    ok &= check("non-integer OI is rejected", c5["unparsed_rows"] == 1)

    live = V.latest_live_poll(root, "SPY", DAY)
    prev = V.latest_live_poll(root, "SPY", V.prev_trading_day(DAY))
    ok &= check("a live poll exists for the test day", live is not None and prev is not None)
    stamp, meta, rows = live
    prev_keys = {(r["expiry"], float(r["strike"])) for r in prev[2]}

    print("1. OI vs OCC")
    o = V.check_occ(rows, DAY, "SPY", root / "validation", fetch_ok, prev_keys)
    ok &= check("real data: exact match, all non-overlap explained",
                o["status"] == "match" and o["sides_equal"] == o["sides_compared"] > 12000
                and o["n_unexplained"] == 0, f"{o['sides_equal']}/{o['sides_compared']}")
    ok &= check("raw OCC response saved with its hash",
                (root / "validation" / o["raw_file"]).read_bytes() == occ_text and len(o["sha256"]) == 64)
    bad = copy.deepcopy(rows)
    tgt = next(r for r in bad if r["call_oi"] > 1000)
    tgt["call_oi"] += 1
    o2 = V.check_occ(bad, DAY, "SPY", root / "validation", fetch_ok, prev_keys)
    ok &= check("one contract off by one -> mismatch naming it",
                o2["status"] == "mismatch" and o2["n_differences"] == 1
                and o2["differences"][0]["strike"] == tgt["strike"])
    extra = copy.deepcopy(rows) + [dict(rows[0], strike=12345.0, call_oi=7.0)]
    o3 = V.check_occ(extra, DAY, "SPY", root / "validation", fetch_ok, prev_keys)
    ok &= check("a CBOE-only row WITH open interest is unexplained",
                o3["status"] == "mismatch" and o3["n_unexplained"] == 1)
    o4 = V.check_occ(rows, DAY, "SPY", root / "validation", fetch_ok, None)
    ok &= check("without previous-session evidence, new strikes are NOT excused",
                o4["status"] == "mismatch" and o4["n_unexplained"] > 0, f"{o4['n_unexplained']} unexplained")
    o5 = V.check_occ(rows, DAY, "SPY", root / "validation", lambda u: (503, b""), prev_keys)
    ok &= check("OCC down -> error, not a pass", o5["status"] == "error")
    # A whole expiry missing from OCC, with real OI and present yesterday,
    # must NOT be excused as a "new listing" (reproduced in a test).
    gone = "2026-10-16"
    trimmed = "\n".join(l for l in occ_text.decode().splitlines()
                        if not l.startswith("SPY") or "\t".join(gone.split("-")) not in l
                        and gone.replace("-", " ") not in " ".join(l.split()[1:4]))
    o6 = V.check_occ(rows, DAY, "SPY", root / "validation",
                     lambda u: (200, trimmed.encode()), prev_keys)
    ok &= check("an existing expiry missing from OCC -> mismatch, not excused",
                o6["status"] == "mismatch" and o6["n_unexplained"] > 0,
                f"{o6.get('n_unexplained')} unexplained")

    nul = copy.deepcopy(rows)
    next(r for r in nul if r["call_oi"] > 1000)["call_oi"] = None
    o7 = V.check_occ(nul, DAY, "SPY", root / "validation", fetch_ok, prev_keys)
    ok &= check("a NULL open interest is invalid, never a verified zero",
                o7["status"] == "mismatch" and o7["n_invalid_oi"] == 1)
    ok &= check("... and arithmetic reports the missing mandatory value",
                V.check_arithmetic(nul, meta, prev[2])["status"] == "investigate")
    ev = root / "validation" / o["raw_file"]
    before = ev.read_bytes()
    o8 = V.check_occ(rows, DAY, "SPY", root / "validation",
                     lambda u: (200, occ_text + b"\n"), prev_keys)
    ok &= check("OCC evidence is immutable: new content -> new file, old one untouched",
                o8["raw_file"] != o["raw_file"] and ev.read_bytes() == before)

    print("2. greeks")
    g = V.check_greeks(rows, meta)
    b = g["benchmark"]
    ok &= check("real data: consistent on the OTM cohort",
                g["status"] == "consistent" and b["n"] >= V.MIN_COHORT and b["r"] == V.FIXED_R,
                f"n={b['n']} gamma median {b['gamma_rel_err_median']:.2%}")
    ok &= check("ITM puts reported separately, not dropped",
                g["itm_puts_diagnostic"] and g["itm_puts_diagnostic"]["n"] > 0)
    skew = copy.deepcopy(rows)
    for r in skew:                       # CBOE gamma 20% too high everywhere
        r["call_gamma_oi"] *= 1.2
        r["put_gamma_oi"] *= 1.2
    ok &= check("gamma inconsistent with IV by 20% -> investigate",
                V.check_greeks(skew, meta)["status"] == "investigate")
    v1 = [{k: v for k, v in r.items() if not k.endswith(("_iv", "_bid", "_ask"))} for r in rows]
    ok &= check("pre-v2 poll (no IV) -> not_run", V.check_greeks(v1, meta)["status"] == "not_run")
    inf = copy.deepcopy(rows)
    for r in inf[:50]:
        r["call_iv"] = float("inf")
    gi = V.check_greeks(inf, meta)
    ok &= check("infinite IV -> flagged, never a clean 'consistent'",
                gi["status"] == "investigate" and any("non-finite" in f for f in gi["flags"]),
                str(gi.get("flags")))
    ok &= check("... and arithmetic coverage flags it too",
                V.check_arithmetic(inf, meta, prev[2])["status"] == "investigate")

    print("3. arithmetic")
    a = V.check_arithmetic(rows, meta, prev[2])
    ok &= check("real data: stored totals reconcile", a["status"] == "reconciled",
                f"net {a['net_gex_recomputed']/1e9:.4f} Bn, net/gross {a['net_over_gross']:.3f}")
    m2 = dict(meta, net_gex_per_pct=meta["net_gex_per_pct"] * 1.0001)
    ok &= check("stored net GEX off by 0.01% -> investigate",
                V.check_arithmetic(rows, m2, prev[2])["status"] == "investigate")
    ok &= check("coverage vs previous session lists the expiry roll",
                a["coverage"]["vs_previous_session"]["expiries_removed"] == ["2026-09-28"])

    print("4. stale open")
    s = V.check_stale_open(root, "SPY", DAY)
    ok &= check("first today-trade snapshot found after some mixed-age ones",
                s["first_observed_today_trade_snapshot"] and s["stored_before_first_today_trade"] > 0,
                str(s["stored_before_first_today_trade"]))

    print("quarantined polls are never used")
    qdir = root / "quarantine" / DAY.isoformat()
    qdir.mkdir(parents=True)
    (qdir / f"meta_{stamp}").write_bytes(b"")
    other = V.latest_live_poll(root, "SPY", DAY)
    ok &= check("the quarantined latest poll is skipped for an earlier one",
                other is not None and other[0] != stamp and other[0] < stamp, f"{stamp} -> {other[0]}")
    (qdir / f"meta_{stamp}").unlink()

    print("unreadable polls are reported, never stepped over silently")
    mdir = root / "metadata/symbol=SPY" / f"date={DAY.isoformat()}"
    newest_meta = mdir / f"meta_{stamp}"
    saved = newest_meta.read_bytes()
    newest_meta.write_bytes(b"not parquet")
    r = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    ok &= check("corrupt newest metadata -> recorded, and NOT a pass",
                r["status"] == "investigate" and len(r["unreadable_polls"]) == 1
                and r["poll"]["stamp"] != stamp, r["status"])
    newest_meta.write_bytes(saved)
    empty = root / "empty_store"          # inside root: removed by main()'s finally
    d = empty / "metadata/symbol=SPY" / f"date={DAY.isoformat()}"
    d.mkdir(parents=True)
    (empty / "aggregates/symbol=SPY" / f"date={DAY.isoformat()}").mkdir(parents=True)
    (d / "meta_120000_000000.parquet").write_bytes(b"x")
    (empty / "aggregates/symbol=SPY" / f"date={DAY.isoformat()}" / "agg_120000_000000.parquet").write_bytes(b"x")
    r = V.run(empty, "SPY", DAY, now=NOON, fetch=fetch_ok)
    ok &= check("ALL polls unreadable -> error, not 'skipped'", r["status"] == "error", r["status"])

    pdir = root / "metadata/symbol=SPY" / f"date={V.prev_trading_day(DAY).isoformat()}"
    pmeta = pdir / f"meta_{prev[0]}"
    psaved = pmeta.read_bytes()
    pmeta.write_bytes(b"not parquet")
    r = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    ok &= check("corrupt PREVIOUS-session poll -> recorded, listing evidence not trusted, not a pass",
                r["status"] == "investigate" and len(r["previous_session_unreadable_polls"]) == 1
                and r["occ_oi"]["n_unexplained"] > 0, r["status"])
    pmeta.write_bytes(psaved)

    print("run(): every outcome is written")
    r = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    f = json.loads((root / "validation/date=2026-09-29.json").read_text())
    ok &= check("full run passes and is on disk", r["status"] == "pass" and f["status"] == "pass")
    r = V.run(root, "SPY", DAY, now=datetime(2026, 9, 30, 16, tzinfo=timezone.utc), fetch=fetch_ok)
    ok &= check("run on a LATER day: OCC not compared (its file is undated)",
                r["occ_oi"]["status"] == "not_run")
    ok &= check("... and the overall result is 'incomplete', NOT pass",
                r["status"] == "incomplete", r["status"])
    rc = V.main(["--root", str(root), "--date", "2026-11-26"])
    ok &= check("exit code 0 for skipped", rc == 0)
    r = V.run(root, "SPY", date(2026, 11, 26), now=NOON, fetch=fetch_ok)
    ok &= check("holiday -> skipped, written",
                r["status"] == "skipped" and (root / "validation/date=2026-11-26.json").exists())
    r = V.run(root, "SPY", date(2026, 10, 1), now=NOON, fetch=fetch_ok)
    ok &= check("no poll that day -> skipped, written",
                r["status"] == "skipped" and (root / "validation/date=2026-10-01.json").exists())

    def boom(url):
        raise RuntimeError("unexpected")
    real = V.check_greeks
    V.check_greeks = lambda *a, **k: 1 / 0
    try:
        r = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    finally:
        V.check_greeks = real
    f = json.loads((root / "validation/date=2026-09-29.json").read_text())
    ok &= check("a crash inside a check -> error, still written (never a stale pass)",
                r["status"] == "error" and f["status"] == "error" and "ZeroDivisionError" in f["error"])
    ok &= check("no temp files left", not list((root / "validation").glob(".tmp_*")))

    ok &= test_short_dated(src, root, fetch_ok)
    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1



def _poll_at(root, day, cut):
    """The latest live poll stored at or before `cut` (HHMMSS UTC) -- what the
    12:00 ET (16:00 UTC) run would have picked."""
    import pyarrow.parquet as pq
    for stamp, m, a in reversed(V.day_polls(root, "SPY", day)):
        if stamp[:6] > cut:
            continue
        meta = pq.read_table(m).to_pylist()[0]
        if meta.get("usable") and V._trade_date(meta.get("newest_trade_time")) == day:
            return stamp, meta, pq.read_table(a).to_pylist()
    return None


def _synthetic(mult=1.0, key="16:15", spot=700.0, iv=0.15):
    """0DTE / 1DTE / 3DTE OTM contracts whose gamma and delta ARE Black-Scholes
    at `mult` x the time to `key` -- the fit must recover `mult`."""
    meta = {"spot": spot, "newest_trade_time": "2026-10-05T12:00:00"}
    val = datetime.fromisoformat(meta["newest_trade_time"]).replace(tzinfo=V._NY)
    rows = []
    for exp in ("2026-10-05", "2026-10-06", "2026-10-08"):
        T = (V._expiry_closes(date.fromisoformat(exp))[key] - val).total_seconds() / (365 * 86400) * mult
        for k in range(-14, 15):
            K = spot + k * 1.0
            row = {"strike": K, "expiry": exp}
            for opt, pfx in V.SIDES:
                if (opt == "C" and K < spot) or (opt == "P" and K > spot):
                    row.update({pfx + "oi": 0.0, pfx + "gamma_oi": 0.0, pfx + "delta_oi": 0.0,
                                pfx + "iv": None, pfx + "bid": None, pfx + "ask": None})
                    continue
                g, d = V.bsm_gamma_delta(spot, K, T, iv, V.FIXED_R, V.FIXED_Q, opt)
                row.update({pfx + "oi": 100.0, pfx + "gamma_oi": g * 1e4, pfx + "delta_oi": d * 1e4,
                            pfx + "iv": iv, pfx + "bid": 1.00, pfx + "ask": 1.01})
            rows.append(row)
    return rows, meta


def test_short_dated(src, root, fetch_ok):
    print("short-dated greeks diagnostic (report-only, audit F12)")
    ok = True
    c = V._expiry_closes(date(2026, 11, 27))
    ok &= check("early close: 13:15 options close, 13:00 equity close",
                (c["16:15"].hour, c["16:15"].minute, c["16:00"].hour, c["16:00"].minute) == (13, 15, 13, 0))
    rows, meta = _synthetic(1.0, "16:15")
    r = V.check_short_dated(rows, meta)
    z = r["cohorts"]["0DTE"]
    ok &= check("BSM-exact contracts at the 16:15 time: consistent, error ~0 at 16:15, multiplier ~1",
                r["status"] == "consistent" and z["at_16_15"]["gamma_rel_err_median"] < 1e-9
                and abs(z["fitted"]["multiplier_of_16_15_T"] - 1) < 0.02, f"{z['fitted']}")
    r = V.check_short_dated(*_synthetic(1.0, "16:00"))
    z = r["cohorts"]["0DTE"]
    ok &= check("BSM-exact at the 16:00 time: the 16:00 benchmark wins, multiplier below 1",
                z["at_16_00"]["gamma_rel_err_median"] < z["at_16_15"]["gamma_rel_err_median"]
                and z["fitted"]["multiplier_of_16_15_T"] < 1, f"{z['fitted']['multiplier_of_16_15_T']:.3f}")
    r = V.check_short_dated(*_synthetic(3.0, "16:15"))
    ok &= check("greeks at 3x the time: flagged (multiplier outside the band)",
                r["status"] == "investigate" and any("multiplier" in f for f in r["flags"]),
                str(r["flags"])[:90])
    r = V.check_short_dated(*_synthetic(12.0, "16:15"))
    ok &= check("greeks at 12x the time: boundary hit reported and flagged",
                r["cohorts"]["0DTE"]["fitted"]["boundary_hit"] and r["status"] == "investigate")
    # Exclusions, one reason each, on a copy of the synthetic chain.
    rows, meta = _synthetic(1.0, "16:15")
    zero = [x for x in rows if x["expiry"] == "2026-10-05"]
    zero[14]["call_oi"] = 0.0                                  # ATM call: zero OI
    zero[16]["call_bid"], zero[16]["call_ask"] = 1.0, 1.5      # wide (and > 2 cents)
    zero[17]["call_bid"], zero[17]["call_ask"] = 0.01, 0.03    # 2 cents: allowed
    r = V.check_short_dated(rows, meta)
    ex = r["excluded"]
    ok &= check("exclusions counted by reason (zero OI, ITM, wide quote; 2-cent spread kept)",
                ex.get("zero OI", 0) >= 1 and ex.get("bad or wide quote") == 1 and ex.get("in the money", 0) == 0,
                str(ex)[:120])
    rows, meta = _synthetic(1.0, "16:15")
    for x in rows:                                             # v3: stored per-contract greeks win
        for _o, pfx in V.SIDES:
            if x[pfx + "oi"]:
                x[pfx + "gamma"] = 2 * x[pfx + "gamma_oi"] / (x[pfx + "oi"] * 100)
                x[pfx + "delta"] = x[pfx + "delta_oi"] / (x[pfx + "oi"] * 100)
    r = V.check_short_dated(rows, meta)
    ok &= check("v3 stored per-contract gamma is used in preference to the recovered one",
                r["cohorts"]["0DTE"]["at_16_15"]["gamma_rel_err_median"] > 0.4)
    r = V.check_short_dated(*_synthetic(12.0, "16:15"))
    f = r["cohorts"]["0DTE"]["fitted"]
    ok &= check("the refined search and the near-optimal range stay inside 0.2x-5x",
                f["multiplier_of_16_15_T"] <= 5.0 and max(f["near_optimal_range"]) <= 5.0 + 1e-12
                and min(f["near_optimal_range"]) >= 0.2 - 1e-12, f"{f['multiplier_of_16_15_T']:.3f}")
    rows, meta = _synthetic(1.0, "16:15")
    for x in rows:
        for _o, pfx in V.SIDES:
            if x[pfx + "oi"]:
                x[pfx + "gamma"] = x[pfx + "gamma_oi"] / (x[pfx + "oi"] * 100)
                x[pfx + "delta"] = x[pfx + "delta_oi"] / (x[pfx + "oi"] * 100)
    rows[14]["call_oi"] = float("nan")                         # v3 greeks present, OI broken
    r = V.check_short_dated(rows, meta)
    try:
        json.dumps(r, allow_nan=False)
        strict = True
    except ValueError:
        strict = False
    ok &= check("NaN OI: excluded and counted, the result stays strict JSON",
                strict and r["excluded"].get("missing or non-finite OI") == 1)
    only0 = [x for x in _synthetic()[0] if x["expiry"] == "2026-10-05"]
    r = V.check_short_dated(only0, _synthetic()[1])
    ok &= check("cohorts with no expiry listed (e.g. 1DTE on a Friday) are 'none listed', not failures",
                r["status"] == "consistent" and r["cohorts"]["1DTE"]["status"] == "none listed")
    thin = [x for x in _synthetic()[0] if not (x["expiry"] == "2026-10-06" and abs(x["strike"] - 700) > 2)]
    r = V.check_short_dated(thin, _synthetic()[1])
    far = [dict(x, expiry="2026-10-30") for x in _synthetic()[0]]          # nothing within 0-6 DTE
    ok &= check("no 0-6 DTE expiry at all -> not_run, never 'consistent' on zero contracts",
                V.check_short_dated(far, _synthetic()[1])["status"] == "not_run")
    ok &= check("a LISTED cohort with too few contracts -> incomplete (success is not claimed for it)",
                r["status"] == "incomplete" and r["insufficient_cohorts"] == ["1DTE"])
    real_csd = V.check_short_dated
    try:
        V.check_short_dated = lambda *a, **k: {"status": "consistent", "x": float("nan"), "cohorts": {}}
        rn = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    finally:
        V.check_short_dated = real_csd
    ok &= check("a non-JSON-safe diagnostic is isolated: its own error, the report is still written",
                rn["short_dated"]["status"] == "error" and rn["status"] != "error"
                and json.loads((root / "validation/date=2026-09-29.json").read_text())["status"] == rn["status"])
    ok &= check("pre-v2 poll (no IV) -> not_run",
                V.check_short_dated([{k: v for k, v in x.items() if not k.endswith("iv")} for x in _synthetic()[0]],
                                    _synthetic()[1])["status"] == "not_run")
    # Real noon snapshots: v2 (recovered greeks) and v3 (stored greeks).
    for day in (date(2026, 9, 29), date(2026, 9, 30)):
        got = _poll_at(src, day, "160000")
        if not got:
            ok &= check(f"{day}: noon poll present in the backup", False)
            continue
        r = V.check_short_dated(got[2], got[1])
        z = r["cohorts"]["0DTE"]
        ok &= check(f"{day} noon ({got[1]['newest_trade_time'][11:16]} ET): consistent; 0DTE n >= 8; "
                    f"closer to the 16:15 convention than 16:00",
                    r["status"] == "consistent" and z["n"] >= 8
                    and z["at_16_15"]["gamma_rel_err_median"] < z["at_16_00"]["gamma_rel_err_median"],
                    f"n={z['n']} err {z['at_16_15']['gamma_rel_err_median']:.1%} vs {z['at_16_00']['gamma_rel_err_median']:.1%}")
    # Report-only: neither a flag nor a crash can move the daily status.
    real = V.check_short_dated
    try:
        V.check_short_dated = lambda *a, **k: {"status": "investigate", "flags": ["forced"], "cohorts": {}}
        r1 = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
        V.check_short_dated = lambda *a, **k: 1 / 0
        r2 = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    finally:
        V.check_short_dated = real
    r0 = V.run(root, "SPY", DAY, now=NOON, fetch=fetch_ok)
    ok &= check("report-only: a flagged diagnostic leaves the daily status unchanged",
                r1["status"] == r0["status"] and r1["short_dated"]["status"] == "investigate", f"{r0['status']}")
    ok &= check("report-only: a crashing diagnostic is recorded as its own error, the report stands",
                r2["status"] == r0["status"] and r2["short_dated"]["status"] == "error"
                and "ZeroDivisionError" in r2["short_dated"]["error"])
    return ok

if __name__ == "__main__":
    sys.exit(main())
