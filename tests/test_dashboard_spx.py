"""The dashboard on SPX (schema v4): one server, a bundle per symbol, and the
AM-settlement filter applied once at read time -- run against a store built
from a REAL archived SPX chain through the real aggregator and store.

The store: the 2026-08-26 SPX chain written as two sessions, 2026-09-17
(Thu) and 2026-09-18 (Fri) -- 09-18 is a real AM-settlement date with both
SPX (AM) and SPXW (PM) listed in that chain -- plus one real SPY checkpoint.
Same chain both days, so every OI change must be EXACTLY zero: any non-zero
change on Friday is the AM leg masquerading as an OI move.

Checked:
  1. settlement_time / poll_session: the clocks the filter and the panels use.
  2. The reader: on 09-18 the SPX-root 09-18 rows are dropped from the view
     and counted; on 09-17 nothing is; header net == an independent recompute
     over the eligible rows; an SPX file without `root` is refused; a pre-v4
     SPY file reads root 'SPY'.
  3. Every panel on the settlement day: tracks/heat nets, history (recomputed
     from eligible rows, not the stored metadata total), smile and term split
     by (expiry, root) with AM/PM labels and settlement instants, no AM series
     on its own day, term values == ivterm on that series' rows alone.
  4. OI walls keyed by series: Friday's front is "2026-09-18 PM", its change
     vs Thursday's SAME series is exactly 0 (a date-combined basket would
     show the AM OI as a drop); Thursday's next series is the AM one.
  5. HTTP with both symbols in one server: ?symbol= routing, the default,
     unknown -> 404, /api/symbols, SPX status carries NO validation report,
     /api/stream per symbol, and an empty configured store.

Usage: python3 tests/test_dashboard_spx.py [SPY_RAW_CHECKPOINT_DIR]
"""
import copy, gzip, json, math, sys, tempfile, threading, time, urllib.error, urllib.request
from datetime import date, datetime, time as dtime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import atexit as _atexit, shutil as _shutil, tempfile as _tempfile
_real_mkdtemp, _made = _tempfile.mkdtemp, []
def _mkdtemp(*a, **k):
    d = _real_mkdtemp(*a, **k)
    _made.append(d)
    return d
_tempfile.mkdtemp = _mkdtemp
_atexit.register(lambda: [_shutil.rmtree(d, ignore_errors=True) for d in _made])

import pyarrow as pa
import pyarrow.parquet as pq
from gex import gammatracks, ivterm, oiwalls, server, store
from gex.aggregate import aggregate, net_exposure
from gex.fetch import Snapshot
from gex.market_calendar import settlement_time

SPX_CHAIN = HERE.parent / "data/fixtures/spx_chains/SPX_2026-08-26T13-35-08Z.json"
DEFAULT_SPY_RAW = HERE.parent / "data/vm_backup/gex_data/raw/symbol=SPY"
THU, FRI = date(2026, 9, 17), date(2026, 9, 18)


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def as_of(payload, day):
    """The chain as if received on `day`: one trade stamped that morning, so
    the OI panel treats the poll as a live session (it requires a same-day
    newest trade)."""
    p = copy.deepcopy(payload)
    p["data"]["options"][0]["last_trade_time"] = f"{day.isoformat()}T10:00:00"
    return p


def write_poll(root, payload, symbol, day, hhmm):
    agg = aggregate(payload, today=day)
    t = datetime(day.year, day.month, day.day, *hhmm, tzinfo=timezone.utc)
    snap = Snapshot(payload=payload, symbol=symbol, received_at=t, fetch_seconds=1.0,
                    http_status=200, fingerprint=f"{symbol}{day}{hhmm}", raw_bytes=1)
    m = store.append_metadata(root, snap, agg, net_exposure(agg))
    a = store.append_aggregate(root, snap, agg)
    key = f"date={t.date().isoformat()}/{m.name[len('meta_'):]}"
    return key, m, a, agg


def build_store(spy_raw):
    root = Path(tempfile.mkdtemp())
    chain = json.loads(SPX_CHAIN.read_text())
    polls = {}
    for day in (THU, FRI):
        p = as_of(chain, day)
        polls[day] = [write_poll(root, p, "_SPX", day, (14, 0)), write_poll(root, p, "_SPX", day, (15, 0))]
    spy = sorted(spy_raw.glob("*.json.gz"))[-1]
    spy_day = date.fromisoformat(spy.name.split("_")[1][:10])
    write_poll(root, json.load(gzip.open(spy)), "SPY", spy_day, (15, 0))
    return root, polls


def test_clocks():
    print("1. settlement clocks and poll sessions")
    ok = True
    reg = dtime(16, 15)
    ok &= check("SPX (AM) settles at the 09:30 open", settlement_time("SPX", FRI, reg) == dtime(9, 30))
    ok &= check("SPXW (PM) at 16:00, 13:00 on an early close (2026-11-27), not SPY's 13:15",
                settlement_time("SPXW", FRI, reg) == dtime(16, 0)
                and settlement_time("SPXW", date(2026, 11, 27), reg) == dtime(13, 0))
    ok &= check("SPY / unknown roots keep options_close (16:15; 13:15 early)",
                settlement_time("SPY", FRI, reg) == dtime(16, 15)
                and settlement_time(None, date(2026, 11, 27), reg) == dtime(13, 15))
    ok &= check("poll_session: a 15:00 UTC poll is that NY date",
                server.poll_session("date=2026-09-18/150000_000000.parquet") == "2026-09-18")
    ok &= check("poll_session: a 02:30 UTC poll belongs to the PREVIOUS NY date",
                server.poll_session("date=2026-09-19/023000_000000.parquet") == "2026-09-18")
    return ok


def eligible_rows(agg_path, day):
    rows = pq.read_table(agg_path).to_pylist()
    return [r for r in rows if not (r["root"] == "SPX" and r["expiry"] == day.isoformat())], rows


def test_reader(root, polls):
    print("2. reader: the filter, the counts, the guards")
    ok = True
    rd = server.StoreReader(root, "_SPX")
    ok &= check("StoreReader normalises _SPX -> SPX", rd.symbol == "SPX" and rd.agg_dir.name == "symbol=SPX")
    k, m, a, agg = polls[FRI][1]
    view = server.build_view(*rd.load(k, m, a))
    kept, allr = eligible_rows(a, FRI)
    n_am = len(allr) - len(kept)
    ok &= check("Friday: AM 09-18 rows exist in storage (capture is lossless)", n_am > 0, f"{n_am} rows")
    ok &= check("Friday view: those rows dropped and counted, nothing else",
                view["n_rows"] == len(kept) and view["n_excluded_settled"] == n_am
                and view["n_contracts"] == agg["n_contracts"] and view["symbol"] == "SPX"
                and not any(r["expiry"] == "2026-09-18" and r.get("root") == "SPX" for r in view["rows"]))
    indep = net_exposure({"spot": agg["spot"], "rows": kept})
    ok &= check("header net GEX/DEX == independent recompute over eligible rows",
                math.isclose(view["net_gex_per_pct"], indep["net_gex"], rel_tol=1e-12)
                and math.isclose(view["net_dex"], indep["net_dex"], rel_tol=1e-12))
    ok &= check("... and differs from the stored all-row total (the filter matters)",
                not math.isclose(view["net_gex_per_pct"], net_exposure(agg)["net_gex"], rel_tol=1e-9))
    k2, m2, a2, _ = polls[THU][1]
    v2 = server.build_view(*rd.load(k2, m2, a2))
    ok &= check("Thursday: nothing excluded (the 09-18 AM leg is still live)",
                v2["n_excluded_settled"] == 0 and v2["n_rows"] == len(pq.read_table(a2)))
    # an SPX aggregate without a root column must be refused, not read unfiltered
    bad = Path(tempfile.mkdtemp()) / "agg_bad.parquet"
    t = pq.read_table(a)
    pq.write_table(t.drop(["root"]), bad)
    try:
        server._read_rows(bad, server.VIEW_COLS, k, "SPX")
        refused = False
    except ValueError:
        refused = True
    ok &= check("an SPX file without `root` is refused (would bypass the filter)", refused)
    rows = server._read_rows(bad, ("strike", "expiry", "root"), k, "SPY")
    ok &= check("a root-less SPY file reads root 'SPY' (keys match across v3/v4)",
                rows and all(r["root"] == "SPY" for r in rows))
    return ok


def test_panels(root, polls):
    print("3. every panel on the settlement day")
    ok = True
    rd = server.StoreReader(root, "SPX")
    k, m, a, agg = polls[FRI][1]
    st = server.State()
    view = server.build_view(*rd.load(k, m, a)); view["poll_key"] = k
    st.set(view)
    kept, _ = eligible_rows(a, FRI)

    tc = server.TracksCache(st, rd)
    tr, heat = tc.get(), tc.get_heat()
    rec_t = pq.read_table(m).to_pylist()[0]["received_at"]
    pt = [p for p in tr["series"] if p["t"] == rec_t][0]          # the anchor poll
    want = gammatracks.extrema(kept, agg["spot"], gammatracks.strike_nets(kept))
    ok &= check("tracks: extrema and ratio from eligible rows only",
                pt["status"] == "ok" and pt["max"]["strike"] == want["max"]["strike"]
                and pt["min"]["strike"] == want["min"]["strike"]
                and math.isclose(pt["ratio"], want["ratio"], rel_tol=1e-12))
    ok &= check("heat: served for the same poll", heat.get("poll_key") == k and heat.get("strikes"))

    hc = server.HistoryCache(st, rd).series()
    p_fri = [p for p in hc["series"]][-1]
    ok &= check("history: Friday points recomputed from eligible rows, not the metadata total",
                math.isclose(p_fri["gex"], view["net_gex_per_pct"], rel_tol=1e-12)
                and not math.isclose(p_fri["gex"], net_exposure(agg)["net_gex"], rel_tol=1e-9))
    st_thu = server.State()
    k2, m2, a2, agg2 = polls[THU][1]
    v2 = server.build_view(*rd.load(k2, m2, a2)); v2["poll_key"] = k2
    st_thu.set(v2)
    h2 = server.HistoryCache(st_thu, rd).series()
    ok &= check("history: an ordinary day still reads the stored metadata total",
                h2["series"][-1]["gex"] == pq.read_table(m2).to_pylist()[0]["net_gex_per_pct"])

    sm = server.SmileCache(st, rd).get()
    ids = [e["id"] for e in sm["expiries"]]
    ok &= check("smile: series ids unique", len(ids) == len(set(ids)))
    oct16 = {e["root"]: e for e in sm["expiries"] if e["expiry"] == "2026-10-16"}
    ok &= check("smile: 2026-10-16 is TWO series, AM and PM, settling 09:30 vs 16:00 ET",
                set(oct16) == {"SPX", "SPXW"} and oct16["SPX"]["settle"] == "AM"
                and oct16["SPXW"]["settle"] == "PM"
                and oct16["SPX"]["settles_utc"] == "2026-10-16T13:30:00+00:00"
                and oct16["SPXW"]["settles_utc"] == "2026-10-16T20:00:00+00:00")
    ok &= check("smile: no AM series for its own settlement day",
                not any(e["expiry"] == "2026-09-18" and e["root"] == "SPX" for e in sm["expiries"]))

    tm = server.TermCache(st, rd).get()
    t16 = {e["root"]: e for e in tm["expiries"] if e["expiry"] == "2026-10-16"}
    ok &= check("term: two series on 2026-10-16, AM placed BEFORE PM by fractional days",
                set(t16) == {"SPX", "SPXW"} and t16["SPX"]["days"] < t16["SPXW"]["days"]
                and math.isclose(t16["SPXW"]["days"] - t16["SPX"]["days"], 6.5 / 24, rel_tol=1e-9))
    rows = server._read_rows(a, server.TERM_COLS, k, "SPX")
    alone = ivterm.expiry_curve([r for r in rows if r["expiry"] == "2026-10-16" and r["root"] == "SPXW"],
                                agg["spot"], True)
    ok &= check("term: a series' values come from its own root's contracts only",
                t16["SPXW"]["values"] == alone["values"])
    ok &= check("term: no AM series on its settlement day; close_utc = settlement",
                not any(e["expiry"] == "2026-09-18" and e["root"] == "SPX" for e in tm["expiries"])
                and all(e["close_utc"] == e["settles_utc"] for e in tm["expiries"]))
    return ok


def test_oi(root, polls):
    print("4. OI walls keyed by (expiry, root)")
    ok = True
    rd = server.StoreReader(root, "SPX")
    st = server.State()
    k, m, a, agg = polls[FRI][1]
    view = server.build_view(*rd.load(k, m, a)); view["poll_key"] = k
    st.set(view)
    out = server.OiCache(st, rd).get()
    cur = out.get("current") or {}
    ok &= check("Friday's front is the PM series of 09-18", cur.get("front_label") == "2026-09-18 PM"
                and cur.get("front_root") == "SPXW", str(cur.get("front_label")))
    ch = cur.get("change") or {}
    ok &= check("its change vs Thursday's SAME series is exactly 0 (no fake AM drop)",
                ch.get("available") and ch.get("abs") == 0, json.dumps(ch)[:200])
    thu = [s for s in out["sessions"] if s.get("date") == "2026-09-17"]
    ok &= check("Thursday's next series is 09-18 AM (settles before 09-18 PM)",
                thu and thu[0].get("next_label") == "2026-09-18 AM", str(thu[0].get("next_label") if thu else None))
    rows = server._read_rows(a, server.OI_COLS, k, "SPX")
    s = oiwalls.summarize(rows, agg["spot"], FRI)
    pm = sum((r["call_oi"] or 0) + (r["put_oi"] or 0) for r in rows
             if r["expiry"] == "2026-09-18" and r["root"] == "SPXW")
    ok &= check("front total = the PM series' OI alone", s["total_oi"] == pm, f"{s['total_oi']} vs {pm}")
    return ok


def get(base, path):
    try:
        r = urllib.request.urlopen(base + path, timeout=10)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def test_http(root):
    print("5. HTTP: two symbols in one server")
    ok = True
    stop = threading.Event()
    empty = Path(tempfile.mkdtemp())
    server.Handler.bundles = {"SPY": server.make_bundle(root, "SPY", 0.2, stop),
                              "SPX": server.make_bundle(root, "_SPX", 0.2, stop),
                              "NDX": server.make_bundle(empty, "NDX", 0.2, stop)}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    for _ in range(100):
        if all(server.Handler.bundles[s].state.get()[0] for s in ("SPY", "SPX")):
            break
        time.sleep(0.1)
    try:
        c, d = get(base, "/api/symbols")
        ok &= check("/api/symbols lists them, SPY default", c == 200 and d == {"symbols": ["SPY", "SPX", "NDX"], "default": "SPY"})
        c1, spy = get(base, "/api/snapshot")
        c2, spx = get(base, "/api/snapshot?symbol=SPX")
        c3, spx2 = get(base, "/api/snapshot?symbol=_spx")
        ok &= check("no ?symbol= serves SPY (old URL unchanged); ?symbol=SPX / _spx serve SPX",
                    c1 == c2 == c3 == 200 and spy["symbol"] == "SPY" and spx["symbol"] == "SPX"
                    and spx2["poll_key"] == spx["poll_key"])
        ok &= check("SPX is the settlement-day view (filter applied over HTTP too)",
                    spx["session_date"] == "2026-09-18" and spx["n_excluded_settled"] > 0)
        c, d = get(base, "/api/snapshot?symbol=QQQ")
        ok &= check("unknown symbol -> 404 naming the configured ones", c == 404 and d.get("symbols") == ["SPY", "SPX", "NDX"])
        c, sx = get(base, "/api/status?symbol=SPX")
        c0, sy = get(base, "/api/status")
        ok &= check("SPX status: NO validation report attached, an explicit note instead",
                    c == 200 and sx["validation"] is None and "SPX" in (sx["validation_note"] or "")
                    and sx["symbol"] == "SPX")
        ok &= check("SPY status unchanged in shape (no note)", c0 == 200 and sy["validation_note"] is None)
        for path in ("/api/oi", "/api/tracks", "/api/heat", "/api/term", "/api/smile", "/api/history"):
            c, d = get(base, path + "?symbol=SPX")
            ok &= check(f"{path}?symbol=SPX answers for the SPX poll", c == 200 and d.get("poll_key") == spx["poll_key"])
        r = urllib.request.urlopen(base + "/api/stream?symbol=SPX", timeout=10)
        line = b""
        while not line.startswith(b"data: "):
            line = r.readline()
        ok &= check("/api/stream?symbol=SPX pushes the SPX snapshot",
                    json.loads(line[6:])["symbol"] == "SPX")
        r.close()
        c, d = get(base, "/api/snapshot?symbol=NDX")
        c2, s2 = get(base, "/api/status?symbol=NDX")
        ok &= check("an empty configured store: 503 'no snapshot yet', status says nothing stored",
                    c == 503 and c2 == 200 and s2["last_stored_at"] is None)
        sz = len(json.dumps(spx)) / 1e6
        ok &= check("SPX snapshot push size measured", sz > 0, f"{sz:.2f} MB vs SPY {len(json.dumps(spy)) / 1e6:.2f} MB")
    finally:
        stop.set()
        srv.shutdown()
    return ok


def test_review_fixes():
    print("6. Review fixes: per-poll trend filter, unreadable = gap, settlement clocks")
    ok = True
    root = Path(tempfile.mkdtemp())
    chain = as_of(json.loads(SPX_CHAIN.read_text()), FRI)
    no_am = copy.deepcopy(chain)
    no_am["data"]["options"] = [o for o in no_am["data"]["options"] if not o["option"].startswith("SPX260918")]
    p1 = write_poll(root, chain, "_SPX", FRI, (14, 0))
    p2 = write_poll(root, chain, "_SPX", FRI, (15, 0))
    p3 = write_poll(root, no_am, "_SPX", FRI, (16, 0))       # the AM leg vanished mid-session
    rd = server.StoreReader(root, "SPX")
    st = server.State()
    v = server.build_view(*rd.load(p3[0], p3[1], p3[2])); v["poll_key"] = p3[0]
    st.set(v)
    hc = server.HistoryCache(st, rd)
    h = hc.series()
    want = [net_exposure({"spot": p[3]["spot"], "rows": eligible_rows(p[2], FRI)[0]})["net_gex"] for p in (p1, p2)]
    ok &= check("AM leg gone from the NEWEST poll: earlier points still filtered (per-poll, not per-day)",
                len(h["series"]) == 3 and all(math.isclose(h["series"][i]["gex"], want[i], rel_tol=1e-12)
                                              for i in range(2)))
    ok &= check("... and the newest point equals its stored total (nothing to filter)",
                h["series"][2]["gex"] == pq.read_table(p3[1]).to_pylist()[0]["net_gex_per_pct"])
    # an unreadable aggregate: a gap, counted, NOT cached; recovers once readable
    hc2 = server.HistoryCache(st, rd)
    real = server.pq.ParquetFile
    class Boom:
        def __init__(self, path, *a, **k):
            if str(path) == str(p2[2]):
                raise OSError("simulated unreadable aggregate")
            self._f = real(path, *a, **k)
        def __getattr__(self, n): return getattr(self._f, n)
    server.pq.ParquetFile = Boom
    try:
        hb = hc2.series()
    finally:
        server.pq.ParquetFile = real
    gap = hb["series"][1]
    ok &= check("unreadable aggregate: an unavailable point (gex None), never the unfiltered total",
                gap.get("unavailable") and gap["gex"] is None and hb["n_unavailable"] == 1)
    ok &= check("... not cached: the next request recomputes and recovers",
                hc2._key is None and not hc2.series()["series"][1].get("unavailable"))

    # settlement clocks the page uses for its "ended" notes
    vf = server.build_view(*server.StoreReader(root, "SPX").load(p1[0], p1[1], p1[2]))
    ok &= check("SPX view: expiring (SPXW) settles 16:00 ET, before the 16:15 session close",
                vf["expiring_settles_utc"] == "2026-09-18T20:00:00+00:00"
                and vf["options_close_utc"] == "2026-09-18T20:15:00+00:00")
    early = server.build_view({"symbol": "SPX", "session_date": "2026-11-27", "rows": [], "spot": 1.0})
    ok &= check("SPX on an early close: 13:00 ET (not SPY's 13:15)",
                early["expiring_settles_utc"] == "2026-11-27T18:00:00+00:00")
    spy = server.build_view({"symbol": "SPY", "session_date": "2026-09-18", "rows": [], "spot": 1.0})
    ok &= check("SPY: the expiring series settles at its options close, as before",
                spy["expiring_settles_utc"] == spy["options_close_utc"])
    stf = server.State(); vv = dict(vf); vv["poll_key"] = p1[0]; stf.set(vv)
    cur = server.OiCache(stf, rd).get().get("current") or {}
    ok &= check("OI front series carries its own settlement (09-18 PM -> 20:00Z)",
                cur.get("front_settles_utc") == "2026-09-18T20:00:00+00:00", str(cur.get("front_settles_utc")))
    return ok


def main():
    spy_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SPY_RAW
    if not SPX_CHAIN.exists() or not list(spy_raw.glob("*.json.gz")):
        print(f"  FAIL  fixtures missing: {SPX_CHAIN} / {spy_raw}")
        return 1
    root, polls = build_store(spy_raw)
    ok = test_clocks()
    ok &= test_reader(root, polls)
    ok &= test_panels(root, polls)
    ok &= test_oi(root, polls)
    ok &= test_http(root)
    ok &= test_review_fixes()
    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
