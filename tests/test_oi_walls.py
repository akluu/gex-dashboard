"""Front-expiry open interest (gex/oiwalls.py, server.OiCache): GEXStream's OI
walls and Max-OI history, with the design corrections.

Checked, in order of how badly they could go wrong unnoticed:
  1. Real stored sessions give the numbers verified by hand before any code
     (2026-09-29: 379,111 total, 768/770/767) and an independent recompute.
  2. OI change is same-expiry vs the previous TRADING session: across Labor
     Day it compares with the Friday; after an uncaptured session it is
     unavailable, never zero and never silently across the gap.
  3. Closures are not sessions (2026-09-07); gaps stay visible (2026-09-23).
  4. The displayed session is anchored to the poll ON SCREEN and never uses a
     later or a stale poll; before a live poll exists, the last session with
     one is shown and flagged.
  5. Invalid OI is unavailable, not 0; ties are flagged; far-from-spot Max
     OI is kept with its distance; calendar labels, including a holiday-
     shifted monthly.
  6. Caching: settled sessions cached, an unreadable fallback not cached.

Usage:  python3 tests/test_oi_walls.py [DATA_ROOT]      (default: the VM backup)
"""
import json, sys, threading, urllib.request
from collections import defaultdict
from datetime import date
from http.server import ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import pyarrow.parquet as pq
import gex.server as server
from gex import oiwalls as ow

DEFAULT_ROOT = HERE.parent / "data/vm_backup/gex_data"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def r(k, e, c, p):
    return {"strike": k, "expiry": e, "call_oi": c, "put_oi": p}


def test_calendar():
    print("5a. trading calendar and expiry labels")
    ok = True
    D = date.fromisoformat
    ok &= check("previous trading day skips Labor Day", ow.previous_trading_day(D("2026-09-08")) == D("2026-09-04"))
    ok &= check("previous trading day skips the weekend", ow.previous_trading_day(D("2026-09-28")) == D("2026-09-25"))
    ok &= check("trading days exclude weekends and holidays",
                ow.trading_days(D("2026-09-03"), D("2026-09-09")) ==
                [D("2026-09-03"), D("2026-09-04"), D("2026-09-08"), D("2026-09-09")])
    cases = {
        "2026-09-18": ["monthly (3rd Fri)", "end of week"],
        "2026-09-30": ["quarter-end"],
        "2026-10-30": ["month-end", "end of week"],
        "2026-09-25": ["end of week"],
        "2026-09-29": ["daily"],
        # Juneteenth 2026-06-19 is the 3rd Friday: the monthly moves to Thursday.
        "2026-06-18": ["monthly (3rd Fri, holiday-shifted)", "end of week"],
        "2026-04-02": ["end of week"],           # Good Friday 04-03 closed
        # New Year's Day 2027 is a Friday: 12-31 ends the quarter AND the week.
        "2026-12-31": ["quarter-end", "end of week"],
    }
    for d, want in cases.items():
        got = ow.expiry_labels(D(d))
        ok &= check(f"labels {d}", got == want, repr(got))
    ok &= check("past the holiday table: unclassified",
                ow.expiry_labels(D("2029-01-05"))[0].startswith("unclassified"))
    return ok


def test_summarize():
    print("5b. summarize(): ranking, ties, validity, scope")
    ok = True
    S = date(2026, 9, 29)
    rows = [r(760, "2026-09-29", 10, 5), r(765, "2026-09-29", 3, 12), r(770, "2026-09-29", 15, 0),
            r(600, "2026-09-29", 50, 50),                       # far from spot, the global max
            r(765, "2026-09-30", 999, 999),                     # later expiry: not the front
            r(765, "2026-09-26", 5000, 0)]                      # expired: ignored
    s = ow.summarize(rows, 766.0, S)
    ok &= check("front = nearest unexpired expiry", s["front_expiry"] == "2026-09-29")
    ok &= check("totals over the front only", (s["total_oi"], s["call_oi"], s["put_oi"]) == (145, 78, 67),
                repr((s["total_oi"], s["call_oi"], s["put_oi"])))
    ok &= check("global Max OI kept far from spot, with its distance",
                s["top"][0]["strike"] == 600 and abs(s["top"][0]["dist"] - (600 / 766 - 1)) < 1e-12)
    # 760 (15), 765 (15), 770 (15) tie: nearer spot first (765 @1, then 770 @4 / 760 @6).
    ok &= check("ties broken nearer spot, flagged as ties",
                [t["strike"] for t in s["top"]] == [600, 765, 770] and s["top"][1]["tied"] and s["top"][2]["tied"]
                and not s["top"][0]["tied"], repr([(t["strike"], t["tied"]) for t in s["top"]]))
    ok &= check("near-spot top 3 excludes strikes outside +-12%",
                [t["strike"] for t in s["top_near"]] == [765, 770, 760])
    ok &= check("expiry totals cover every expiry (for a later baseline)",
                s["expiry_totals"]["2026-09-30"] == 1998 and s["expiry_totals"]["2026-09-26"] == 5000)
    eq = ow.summarize([r(764, "2026-09-29", 5, 0), r(768, "2026-09-29", 5, 0)], 766.0, S)
    ok &= check("equidistant exact tie: lower strike first", [t["strike"] for t in eq["top"]] == [764, 768])
    for bad, label in ((float("nan"), "NaN"), (-1.0, "negative"), (2.5, "fractional"), (None, "missing"),
                       (True, "bool")):
        b = ow.summarize(rows[:3] + [r(775, "2026-09-29", bad, 0)], 766.0, S)
        ok &= check(f"{label} OI in the front: unavailable, not 0",
                    b["total_oi"] is None and b["top"] == [] and b["problems"]
                    and b["expiry_totals"]["2026-09-29"] is None)
    b = ow.summarize(rows[:3] + [r(765, "2026-09-30", float("nan"), 1)], 766.0, S)
    ok &= check("invalid OI elsewhere: that expiry unavailable, the front unaffected",
                b["total_oi"] == 45 and b["expiry_totals"]["2026-09-30"] is None)
    ns = ow.summarize(rows[:3], None, S)
    ok &= check("no spot: distances unavailable, no near-spot list",
                ns["top"][0]["dist"] is None and ns["top_near"] == [])
    ok &= check("no unexpired expiry: stated", ow.summarize([r(1, "2026-09-01", 1, 1)], 766.0, S)["problems"])
    ok &= check("next stored expiry and its current OI (post-close note)",
                s["next_expiry"] == "2026-09-30" and s["next_total_oi"] == 1998)
    ok &= check("no later expiry: next unavailable", ow.summarize(rows[:3], 766.0, S)["next_expiry"] is None)
    # Reproduced case: a valid 30 plus a 300 with no expiry used to
    # return total 30 with no problem.
    u = ow.summarize([r(765, "2026-09-29", 10, 20), {"strike": 766, "expiry": None, "call_oi": 150, "put_oi": 150}],
                     766.0, S)
    ok &= check("a row with no expiry invalidates every total (scope unknown)",
                u["total_oi"] is None and u["problems"] and all(v is None for v in u["expiry_totals"].values()))
    # No strike but a known expiry: the scope IS known -- only that expiry.
    u = ow.summarize([r(765, "2026-09-29", 10, 20), r(None, "2026-09-30", 1, 1)], 766.0, S)
    ok &= check("no strike in another expiry: only that expiry unavailable",
                u["total_oi"] == 30 and u["expiry_totals"]["2026-09-30"] is None)
    u = ow.summarize([r(765, "2026-09-29", 10, 20), r(None, "2026-09-29", 1, 1)], 766.0, S)
    ok &= check("no strike in the front: the front unavailable", u["total_oi"] is None and u["problems"])
    z = ow.summarize([r(765, "2026-09-29", 0, 0), r(766, "2026-09-29", 0, 0)], 766.0, S)
    ok &= check("zero front OI: total 0, no 'wall' of zeros, stated",
                z["total_oi"] == 0 and z["top"] == [] and z["top_near"] == [] and z["problems"])
    return ok


def test_change():
    print("2a. same_expiry_change(): unavailable, never zero")
    ok = True
    cur = {"front_expiry": "2026-09-29", "total_oi": 300.0}
    base = {"expiry_totals": {"2026-09-29": 100.0, "2026-09-30": 0.0, "2026-10-01": None}}
    c = ow.same_expiry_change(cur, base, date(2026, 9, 28))
    ok &= check("normal: absolute and percentage", c["available"] and c["abs"] == 200 and c["pct"] == 2.0)
    c = ow.same_expiry_change(cur, None, date(2026, 9, 23))
    ok &= check("baseline session not captured", not c["available"] and "2026-09-23" in c["reason"])
    c = ow.same_expiry_change({**cur, "front_expiry": "2026-10-02"}, base, date(2026, 9, 28))
    ok &= check("expiry absent from the baseline", not c["available"] and "not in" in c["reason"])
    c = ow.same_expiry_change({**cur, "front_expiry": "2026-10-01"}, base, date(2026, 9, 28))
    ok &= check("invalid baseline", not c["available"] and "invalid" in c["reason"])
    c = ow.same_expiry_change({**cur, "front_expiry": "2026-09-30"}, base, date(2026, 9, 28))
    ok &= check("zero baseline: absolute kept, percentage unavailable",
                c["abs"] == 300 and c["pct"] is None and "zero baseline" in c["reason"])
    return ok


def by_date(out):
    return {s["date"]: s for s in out["sessions"]}


def test_store(root):
    ok = True
    reader = server.StoreReader(root, "SPY")
    newest = reader.newest()[0]
    if not newest.startswith("date=2026-09-29/"):
        print(f"  (newest stored poll is {newest}; the 09-29 anchor checks use explicit keys)")
    day29 = list(reader.complete("date=2026-09-29"))        # newest first
    last29 = day29[0][0]
    st = server.State()
    st.set({"poll_key": last29})
    cache = server.OiCache(st, reader)
    out = cache.get()
    S = by_date(out)

    print("1. real sessions: the numbers verified before any code")
    s29 = S["2026-09-29"]
    ok &= check("09-29 front expiry and total", s29["front_expiry"] == "2026-09-29" and s29["total_oi"] == 379111,
                f"{s29['front_expiry']} {s29['total_oi']}")
    ok &= check("09-29 top 3 (768 / 770 / 767)",
                [(t["strike"], t["oi"]) for t in s29["top"]] == [(768, 18731), (770, 17171), (767, 16959)])
    k, m, a = next(x for x in day29 if x[0] == s29["snapshot_key"])
    raw = pq.read_table(a, columns=["strike", "expiry", "call_oi", "put_oi"]).to_pylist()
    comb = defaultdict(float)
    for x in raw:
        if x["expiry"] == "2026-09-29":
            comb[x["strike"]] += x["call_oi"] + x["put_oi"]
    ok &= check("independent recompute of total and Max OI",
                sum(comb.values()) == s29["total_oi"] and max(comb, key=comb.get) == s29["top"][0]["strike"])
    s18 = S["2026-09-18"]
    ok &= check("09-18 monthly: global Max OI 520 kept at ~-32% from spot",
                s18["top"][0]["strike"] == 520 and s18["top"][0]["oi"] == 212149 and s18["top"][0]["dist"] < -0.30
                and s18["labels"] == ["monthly (3rd Fri)", "end of week"])
    ok &= check("09-18 near-spot top 3 all within +-12%",
                s18["top_near"] and all(abs(t["dist"]) <= 0.12 for t in s18["top_near"]))

    print("2b. change vs the previous TRADING session")
    c29 = s29["change"]
    ok &= check("09-29 vs 09-28: +205,746 (+118.7%)",
                c29["available"] and c29["baseline_session"] == "2026-09-28" and c29["abs"] == 205746
                and abs(c29["pct"] - 1.1868) < 1e-3, f"{c29['abs']} {c29['pct']}")
    ok &= check("09-08 compares with 09-04, across Labor Day", S["2026-09-08"]["change"]["baseline_session"] == "2026-09-04"
                and S["2026-09-08"]["change"]["available"])
    c24 = S["2026-09-24"]["change"]
    ok &= check("09-24: unavailable because 09-23 was not captured (not compared with 09-22)",
                not c24["available"] and c24["baseline_session"] == "2026-09-23" and "2026-09-23" in c24["reason"])
    ok &= check("first stored session: no baseline", not S["2026-09-04"]["change"]["available"])

    print("3. closures and gaps")
    ok &= check("09-07 Labor Day is a closure, not a session",
                "2026-09-07" not in S and any(c["date"] == "2026-09-07" and c["stored_polls"] == 49
                                              for c in out["closures"]))
    ok &= check("09-23 is a visible gap", S["2026-09-23"]["status"] == "gap")
    ok &= check("every weekday session from the first partition is a slot",
                [s["date"] for s in out["sessions"]] ==
                [d.isoformat() for d in ow.trading_days(date(2026, 9, 4), date(2026, 9, 29))])

    print("4. anchored to the poll on screen")
    mid = day29[len(day29) // 2][0]
    st.set({"poll_key": mid})
    out_mid = cache.get()
    ok &= check("the displayed session uses a poll at or before the one on screen",
                by_date(out_mid)["2026-09-29"]["snapshot_key"] <= mid)
    first_poll = day29[-1][0]            # saved before 09-29's first live trade (mixed-age)
    meta = pq.read_table(day29[-1][1]).to_pylist()[0]
    st.set({"poll_key": first_poll})
    out_first = cache.get()
    stale_first = str(meta.get("newest_trade_time"))[:10] < "2026-09-29"
    ok &= check("before a live poll: the session is a gap, the last live session is shown and flagged",
                (not stale_first) or (by_date(out_first)["2026-09-29"]["status"] == "gap"
                                      and out_first["current"]["date"] == "2026-09-28"
                                      and out_first["current"]["is_shown_session"] is False),
                f"first poll newest trade {meta.get('newest_trade_time')}")

    print("6. caching")
    st.set({"poll_key": last29})
    a1 = cache.get()
    ok &= check("same poll: served from cache", cache.get() is a1)
    ok &= check("settled sessions cached, the displayed one not",
                "2026-09-28" in cache._done and "2026-09-29" not in cache._done)
    ok &= check("a gap is never cached (it may be repaired)", "2026-09-23" not in cache._done)
    ok &= check("a result with a gap is retried even for an unchanged poll (not only on read errors)",
                a1["retry"] is True and a1["transient"] is False and cache._retry_at > 0)
    clean = server.OiCache(st, reader)
    real_build = clean._build
    clean._build = lambda key: {**real_build(key), "retry": False, "transient": False}
    import time as _t
    clean.get()
    ok &= check("even a clean result has a bounded life (rechecked, not served forever)",
                0 < clean._retry_at - _t.time() <= server.OI_REFRESH_SECONDS + 1)
    # The cached 09-28 summary used its newest live poll; if that poll is
    # later quarantined (complete() drops it), the entry must be rebuilt even
    # though the partition's newest-key signature might not change.
    used = cache._done["2026-09-28"][1]["snapshot_key"]
    real_complete = reader.complete
    reader.complete = lambda day, _rc=real_complete: (x for x in _rc(day) if x[0] != used)
    try:
        cache._key = None                    # force a rebuild of the outer result
        s28b = by_date(cache.get())["2026-09-28"]
    finally:
        reader.complete = real_complete
    ok &= check("a cached session whose snapshot was quarantined is rebuilt from another poll",
                s28b["snapshot_key"] != used and s28b["total_oi"] == S["2026-09-28"]["total_oi"])
    cache._key = None; cache.get()
    # A cached entry whose partition changed is rebuilt, not served.
    real28 = cache._done["2026-09-28"][1]["total_oi"]
    cache._done["2026-09-28"] = ("stale-signature", {**cache._done["2026-09-28"][1], "total_oi": 1.0})
    st.set({"poll_key": mid}); cache.get(); st.set({"poll_key": last29})
    ok &= check("a cached session whose source changed is rebuilt",
                by_date(cache.get())["2026-09-28"]["total_oi"] == real28)
    # An invalid summary carries a readable reason (the page printed "undefined").
    import pyarrow as pa
    inv = server.OiCache(st, reader)
    path27 = str(list(reader.complete("date=2026-09-25"))[0][2])
    real_read0 = server.pq.read_table

    class BadRowsPQ:
        ParquetFile = pq.ParquetFile      # schema checks (server._read_rows) stay real
        @staticmethod
        def read_table(path, *a, **k):
            t = real_read0(path, *a, **k)
            if str(path) == path27:
                rows = t.to_pylist() + [{"strike": 1.0, "expiry": None, "call_oi": 1.0, "put_oi": 1.0}]
                return pa.Table.from_pylist(rows)
            return t
    server.pq = BadRowsPQ
    try:
        oi = inv.get()
    finally:
        server.pq = pq
    s25 = by_date(oi)["2026-09-25"]
    ok &= check("invalid session: status, readable reason, not cached",
                s25["status"] == "invalid" and "scope" in (s25["reason"] or "") and "2026-09-25" not in inv._done,
                repr(s25.get("reason"))[:80])
    ok &= check("the next session's change names the invalid baseline",
                not by_date(oi)["2026-09-28"]["change"]["available"]
                and "invalid" in by_date(oi)["2026-09-28"]["change"]["reason"])
    # An unreadable newest poll on a past day: the next live poll is used,
    # disclosed, and the result is NOT cached for good.
    fresh = server.OiCache(st, reader)
    bad_path = str(list(reader.complete("date=2026-09-28"))[0][2])
    real_read = server.pq.read_table

    class FlakyPQ:
        ParquetFile = pq.ParquetFile      # schema checks (server._read_rows) stay real
        @staticmethod
        def read_table(path, *a, **k):
            if str(path) == bad_path:
                raise OSError("simulated unreadable file")
            return real_read(path, *a, **k)
    server.pq = FlakyPQ
    try:
        o = fresh.get()
    finally:
        server.pq = pq
    s28 = by_date(o)["2026-09-28"]
    ok &= check("unreadable newest poll: fell back, disclosed, marked transient",
                s28["status"] == "ok" and s28["unreadable_skipped"] == 1 and o["transient"]
                and "2026-09-28" not in fresh._done)
    ok &= check("the fallback gives the same OI (constant within a session)",
                s28["total_oi"] == S["2026-09-28"]["total_oi"])

    print("scheduled options close sent with the view (the page compares it with the clock)")
    k0, m0, a0 = day29[0]
    v29 = server.build_view(*reader.load(k0, m0, a0))
    ok &= check("regular day: 16:15 ET = 20:15 UTC in EDT", v29["options_close_utc"] == "2026-09-29T20:15:00+00:00",
                v29["options_close_utc"])
    ok &= check("early close (2026-11-27): 13:15 ET = 18:15 UTC in EST",
                server.build_view({"session_date": "2026-11-27", "spot": 700.0, "rows": []})["options_close_utc"]
                == "2026-11-27T18:15:00+00:00")
    ok &= check("unparseable session date: no close sent, no crash",
                server.build_view({"session_date": None, "spot": 700.0, "rows": []})["options_close_utc"] is None)

    print("route")
    server.Handler.bundles = {"SPY": server.bundle("SPY", state=st, oi=cache)}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        body = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{srv.server_address[1]}/api/oi", timeout=10).read())
        ok &= check("/api/oi serves the anchored result", body["poll_key"] == last29 and body["current"]["date"] == "2026-09-29"
                    and len(body["current"]["strikes"]) == s29["n_strikes"])
        ok &= check("sessions carry no per-strike arrays (only the current one does)",
                    all("strikes" not in s and "expiry_totals" not in s for s in body["sessions"]))
    finally:
        srv.shutdown()
    return ok


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    ok = test_calendar() & test_summarize() & test_change() & test_store(root)
    print()
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
