"""The dashboard serves the collector's stored polls and NEVER contacts CBOE.

Built on real daily checkpoints written through the real store functions,
with all network access blocked for the whole run.

Usage:  python3 tests/test_server_store.py [RAW_CHECKPOINT_DIR] [V1_AGG_PARQUET]
Defaults: the local VM backup. The second argument is one real v1 aggregate
file (pre-schema-v2), to prove the reader handles the old layout.
"""
import gzip, json, socket, sys, tempfile, threading, time, urllib.request
from datetime import date, datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
import contextlib, io

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

# Block the network BEFORE importing anything from gex: any attempt to reach
# CBOE (or anything else) raises. The local HTTP test below uses loopback and
# is allowed explicitly.
_real_connect = socket.socket.connect
NET_ATTEMPTS = []


def _guard(self, addr):
    if isinstance(addr, tuple) and addr[0] in ("127.0.0.1", "::1", "localhost"):
        return _real_connect(self, addr)
    NET_ATTEMPTS.append(addr)
    raise OSError(f"network blocked in test: {addr}")


socket.socket.connect = _guard

# Every temp dir this test makes is removed at exit (they hold real snapshot
# copies; an earlier sibling test left 9.4 GB of them behind).
import atexit, shutil as _shutil
_real_mkdtemp = tempfile.mkdtemp
_made = []
def _mkdtemp(*a, **k):
    d = _real_mkdtemp(*a, **k)
    _made.append(d)
    return d
tempfile.mkdtemp = _mkdtemp
atexit.register(lambda: [_shutil.rmtree(d, ignore_errors=True) for d in _made])

import gex.server as server
from gex.aggregate import aggregate
from gex.fetch import Snapshot
from gex import store

import pyarrow as pa
import pyarrow.parquet as pq

BACKUP = HERE.parent / "data/vm_backup/gex_data"
DEFAULT_RAW = BACKUP / "raw/symbol=SPY"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def write_poll(root, payload, today, received_at, meta_only=False):
    agg = aggregate(payload, today=today)
    snap = Snapshot(payload=payload, symbol="SPY", received_at=received_at,
                    fetch_seconds=0.1, http_status=200, fingerprint="t", raw_bytes=1)
    from gex.aggregate import net_exposure
    store.append_metadata(root, snap, agg, net_exposure(agg))
    if not meta_only:
        store.append_aggregate(root, snap, agg)
    return agg, snap


def main():
    raw_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_RAW
    files = sorted(raw_dir.glob("*.json.gz"))
    if len(files) < 2:
        raise SystemExit(f"need >= 2 checkpoints in {raw_dir}")
    ok = True

    print("no CBOE client in the server")
    ok &= check("server module has no CboeClient / poll_loop",
                not hasattr(server, "CboeClient") and not hasattr(server, "poll_loop"))

    root = Path(tempfile.mkdtemp())
    p_old = json.load(gzip.open(files[-2]))
    p_new = json.load(gzip.open(files[-1]))
    d1, d2 = date(2026, 9, 28), date(2026, 9, 29)
    t1 = datetime(2026, 9, 28, 15, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 9, 29, 15, 0, tzinfo=timezone.utc)
    write_poll(root, p_old, d1, t1)
    agg_last_d1, _ = write_poll(root, p_new, d1, t1 + timedelta(minutes=1))
    reader = server.StoreReader(root, "SPY")

    print("reader picks the newest COMPLETE poll")
    key = reader.newest()[0]
    ok &= check("newest poll of the only day", key.startswith("date=2026-09-28/"), key)
    ok &= check("it is the later of the two", "150100" in key, key)
    # Day 2 has metadata but no aggregate yet (the collector writes metadata
    # first): the reader must NOT pick it, and must fall back to day 1.
    write_poll(root, p_new, d2, t2, meta_only=True)
    key = reader.newest()[0]
    ok &= check("metadata without its aggregate is skipped (falls back a day)",
                key.startswith("date=2026-09-28/") and "150100" in key, key)
    agg2, snap2 = write_poll(root, p_old, d2, t2 + timedelta(seconds=30))
    key = reader.newest()[0]
    ok &= check("once a complete poll lands on the new day, it wins",
                key.startswith("date=2026-09-29/") and "150030" in key, key)

    print("loaded view matches what the collector stored")
    loaded_agg, loaded_snap = reader.load(*reader.newest())
    view = server.build_view(loaded_agg, loaded_snap)
    want_rows = [{c: r[c] for c in server.VIEW_COLS} for r in agg2["rows"]]
    ok &= check("rows equal the stored aggregate, view columns only",
                view["rows"] == want_rows, f"{len(view['rows'])} rows")
    ok &= check("no quote columns sent (not drawn yet)",
                all("call_iv" not in r for r in view["rows"]))
    ok &= check("spot, contract count, session date carried through",
                view["spot"] == agg2["spot"] and view["n_contracts"] == agg2["n_contracts"]
                and view["session_date"] == "2026-09-29")
    ok &= check("feed clocks carried through",
                view["feed"]["newest_trade_time"] == snap2.newest_trade_time
                and view["feed"]["received_at"] == snap2.received_at.isoformat())
    ok &= check("net GEX recomputed from stored rows matches the collector's",
                abs(view["net_gex_per_pct"] - server.net_exposure(agg2)["net_gex"]) < 1e-3)
    ok &= check("source says collector store", view["source"] == "collector store")

    print("reads a real v1 (pre-IV) aggregate")
    v1_agg = Path(sys.argv[2]) if len(sys.argv) > 2 else next(
        iter(sorted((BACKUP / "aggregates/symbol=SPY/date=2026-09-28").glob("agg_*.parquet"))), None)
    if v1_agg is None:
        ok &= check("a v1 aggregate file is available", False)
    else:
        rows = pq.read_table(v1_agg, columns=list(server.VIEW_COLS)).to_pylist()
        ok &= check("v1 file reads with the view columns", len(rows) > 1000,
                    f"{v1_agg.name}: {len(rows)} rows")

    print("store loop pushes once per new poll, never re-sends")
    state = server.State()
    stop = threading.Event()
    th = threading.Thread(target=server.store_loop, args=(state, reader, 0.05, stop), daemon=True)
    th.start()
    time.sleep(0.5)
    _, v_a = state.get()
    ok &= check("one push for one poll", v_a == 1, f"version={v_a}")
    write_poll(root, p_new, d2, t2 + timedelta(minutes=2))
    time.sleep(0.5)
    payload, v_b = state.get()
    ok &= check("new poll -> exactly one more push", v_b == 2, f"version={v_b}")
    ok &= check("and it is the new poll", "15:02:00" in payload["feed"]["received_at"])

    # A corrupt aggregate must not blank the page.
    bad_t = t2 + timedelta(minutes=3)
    import io, contextlib
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        write_poll(root, p_new, d2, bad_t)
        bad = root / "aggregates/symbol=SPY/date=2026-09-29" / f"agg_{bad_t.strftime('%H%M%S_%f')}.parquet"
        bad.write_bytes(b"not parquet")
        time.sleep(0.5)                           # ~10 checks at 0.05 s
    payload, v_c = state.get()
    ok &= check("unreadable newest poll: last good view kept",
                v_c == 2 and "15:02:00" in payload["feed"]["received_at"], f"version={v_c}")
    n_err = captured.getvalue().count("stored poll rejected")
    ok &= check("the bad poll is logged once, not every check", n_err == 1, f"{n_err} log lines")
    write_poll(root, p_old, d2, bad_t + timedelta(minutes=1))
    time.sleep(0.5)
    payload, v_d = state.get()
    ok &= check("the next good poll is shown normally",
                v_d == 3 and "15:04:00" in payload["feed"]["received_at"], f"version={v_d}")

    print("cold start when the newest poll is unreadable")
    state2, stop2 = server.State(), threading.Event()
    root_cs = Path(tempfile.mkdtemp())
    write_poll(root_cs, p_old, d2, t2)
    t_bad = t2 + timedelta(minutes=1)
    write_poll(root_cs, p_new, d2, t_bad)
    (root_cs / "aggregates/symbol=SPY/date=2026-09-29" /
     f"agg_{t_bad.strftime('%H%M%S_%f')}.parquet").write_bytes(b"not parquet")
    with contextlib.redirect_stdout(io.StringIO()):
        threading.Thread(target=server.store_loop,
                         args=(state2, server.StoreReader(root_cs, "SPY"), 0.05, stop2),
                         daemon=True).start()
        time.sleep(0.5)
    pl2, v2 = state2.get()
    ok &= check("falls back to the newest READABLE poll instead of showing nothing",
                pl2 is not None and v2 == 1 and "15:00:00" in pl2["feed"]["received_at"],
                f"version={v2}")
    stop2.set()

    print("unusable metadata is rejected the same way")
    root_u = Path(tempfile.mkdtemp())
    write_poll(root_u, p_old, d2, t2)
    write_poll(root_u, p_new, d2, t2 + timedelta(minutes=1))
    mpath = root_u / "metadata/symbol=SPY/date=2026-09-29" / \
        f"meta_{(t2 + timedelta(minutes=1)).strftime('%H%M%S_%f')}.parquet"
    row = pq.read_table(mpath).to_pylist()[0]; row["usable"] = False
    pq.write_table(pa.Table.from_pylist([row]), mpath)
    state3, stop3 = server.State(), threading.Event()
    with contextlib.redirect_stdout(io.StringIO()):
        threading.Thread(target=server.store_loop,
                         args=(state3, server.StoreReader(root_u, "SPY"), 0.05, stop3),
                         daemon=True).start()
        time.sleep(0.5)
    pl3, _ = state3.get()
    ok &= check("unusable newest poll skipped, previous one shown",
                pl3 is not None and "15:00:00" in pl3["feed"]["received_at"])
    stop3.set()

    print("newest trade time: naive Eastern -> UTC")
    v_ = server.build_view(loaded_agg, SimpleNamespace(
        newest_trade_time="2026-09-28T16:14:59", snapshot_time=None,
        received_at=None, fetch_seconds=None, http_status=None))
    ok &= check("EDT 16:14:59 -> 20:14:59 UTC",
                v_["feed"]["newest_trade_utc"] == "2026-09-28T20:14:59+00:00",
                str(v_["feed"]["newest_trade_utc"]))
    v_ = server.build_view(loaded_agg, SimpleNamespace(
        newest_trade_time="2026-12-01T09:31:00", snapshot_time=None,
        received_at=None, fetch_seconds=None, http_status=None))
    ok &= check("EST 09:31 -> 14:31 UTC",
                v_["feed"]["newest_trade_utc"] == "2026-12-01T14:31:00+00:00")

    print("trend chart is anchored to the poll on screen")

    def run_loop(root_x, secs=0.5):
        st, sp = server.State(), threading.Event()
        rd = server.StoreReader(root_x, "SPY")
        with contextlib.redirect_stdout(io.StringIO()):
            threading.Thread(target=server.store_loop, args=(st, rd, 0.05, sp),
                             daemon=True).start()
            time.sleep(secs)
        sp.set()
        return st, rd

    root_h = Path(tempfile.mkdtemp())
    for k in range(3):
        write_poll(root_h, p_old, d1, t1 + timedelta(minutes=k))
    write_poll(root_h, p_old, d1, t1 + timedelta(minutes=3), meta_only=True)
    st_h, rd_h = run_loop(root_h)
    h = server.HistoryCache(st_h, rd_h).series()
    shown = st_h.get()[0]
    ok &= check("no poll today -> last session's series, same poll as the snapshot",
                h["session_date"] == "2026-09-28" == shown["session_date"]
                and h["poll_key"] == shown["poll_key"] and len(h["series"]) == 3,
                f"{h['session_date']} {len(h['series'])} points")
    ok &= check("metadata-only poll excluded (trend never ahead of profiles)",
                all("15:03:00" not in r["t"] for r in h["series"]))
    ok &= check("series in time order",
                [r["t"] for r in h["series"]] == sorted(r["t"] for r in h["series"]))

    # Scenario: yesterday valid, today's only complete pair corrupt.
    # Both panels must show YESTERDAY -- a first version showed yesterday's
    # profiles over today's trend.
    root_x = Path(tempfile.mkdtemp())
    write_poll(root_x, p_old, d1, t1)
    write_poll(root_x, p_old, d1, t1 + timedelta(minutes=1))
    write_poll(root_x, p_new, d2, t2)
    (root_x / "aggregates/symbol=SPY/date=2026-09-29" /
     f"agg_{t2.strftime('%H%M%S_%f')}.parquet").write_bytes(b"not parquet")
    st_x, rd_x = run_loop(root_x)
    sx = st_x.get()[0]
    hx = server.HistoryCache(st_x, rd_x).series()
    ok &= check("corrupt today + valid yesterday: snapshot AND trend both yesterday",
                sx["session_date"] == "2026-09-28" and hx["session_date"] == "2026-09-28"
                and hx["poll_key"] == sx["poll_key"] and len(hx["series"]) == 2,
                f"snapshot {sx['session_date']}, trend {hx['session_date']} "
                f"({len(hx['series'])} pts)")

    # Cutoff: the trend stops at the displayed poll even if newer ones exist.
    st_c = server.State()
    early = next(k for k, _m, _a in rd_h.complete("date=2026-09-28") if "150100" in k)
    st_c.set({"poll_key": early, "session_date": "2026-09-28"})
    hc = server.HistoryCache(st_c, rd_h).series()
    ok &= check("trend stops at the displayed poll", len(hc["series"]) == 2,
                f"{len(hc['series'])} points up to {early}")

    print("HTTP")
    server.Handler.bundles = {"SPY": server.bundle("SPY", state=state, reader=reader,
                                                   history=server.HistoryCache(state, reader))}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    body = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{srv.server_address[1]}/api/snapshot", timeout=5).read())
    ok &= check("/api/snapshot serves the stored view",
                body["spot"] == payload["spot"] and len(body["rows"]) == len(payload["rows"]))
    hist = json.loads(urllib.request.urlopen(
        f"http://127.0.0.1:{srv.server_address[1]}/api/history", timeout=5).read())
    ok &= check("/api/history is the displayed poll's day and trend",
                hist["session_date"] == "2026-09-29" and len(hist["series"]) >= 3
                and hist["poll_key"] == body["poll_key"],
                f"{hist['session_date']} {len(hist['series'])} points")
    srv.shutdown()
    stop.set()

    print("IV smile endpoint")
    sm_state = server.State()
    sm_reader = server.StoreReader(root, "SPY")
    good_key = next(k for k, _m, _a in sm_reader.complete("date=2026-09-29") if "150400" in k)
    agg_rows = {(r["expiry"], r["strike"]): r for r in pq.read_table(
        sm_reader.agg_dir / "date=2026-09-29" / ("agg_" + good_key.split("/")[1])).to_pylist()}
    spot = agg2["spot"]
    sm_state.set({"poll_key": good_key, "session_date": "2026-09-29", "spot": spot})
    sc = server.SmileCache(sm_state, sm_reader)
    sm = sc.get()
    pts = [(e["expiry"], p_) for e in sm["expiries"] for p_ in e["points"]]
    ok &= check("smile available with points", sm["available"] and len(pts) > 500, f"{len(pts)} points")
    ok &= check("OTM side only: puts below spot, calls at/above",
                all((p_[2] == "P") == (p_[0] < spot) for _e, p_ in pts))
    def src(e, p_):
        r = agg_rows[(e, p_[0])]; pre = "put_" if p_[2] == "P" else "call_"
        return r[pre + "iv"], r[pre + "bid"], r[pre + "ask"]
    ok &= check("every kept point: IV equals the stored IV, two-sided quote, spread <= 20%",
                all(abs(src(e, p_)[0] - p_[1]) < 1e-5 and src(e, p_)[1] > 0
                    and src(e, p_)[2] > src(e, p_)[1]
                    and (src(e, p_)[2] - src(e, p_)[1]) / ((src(e, p_)[2] + src(e, p_)[1]) / 2) <= 0.20
                    for e, p_ in pts))
    within = sum(1 for (e, k) in agg_rows if abs(k / spot - 1) <= 0.20)
    accounted = sum(e["retained"] + sum(e["excluded"].values()) for e in sm["expiries"])
    ok &= check("kept + excluded accounts for every in-range strike of the expiries shown",
                accounted <= within and accounted > 0.5 * within, f"{accounted} of {within}")
    ok &= check("points within +-20% of spot", all(abs(p_[0] / spot - 1) <= 0.20 for _e, p_ in pts))
    # gap flag is set exactly when a listed in-range strike was filtered out
    # between two kept points (not when the strike grid widens $1 -> $5)
    gap_ok, n_gaps, grid_steps = True, 0, 0
    for e in sm["expiries"]:
        listed = sorted(k for (x_, k) in agg_rows if x_ == e["expiry"] and abs(k / spot - 1) <= 0.20)
        kept = {p_[0] for p_ in e["points"]}
        for i, p_ in enumerate(e["points"]):
            if i == 0:
                gap_ok &= p_[3] == 0
                continue
            prev = e["points"][i - 1][0]
            between = [k for k in listed if prev < k < p_[0]]
            gap_ok &= p_[3] == (1 if between else 0)
            n_gaps += p_[3]
            grid_steps += (not between and p_[0] - prev > 1.0)
    ok &= check("gap flag = a listed strike was filtered in between; grid widening is not a gap",
                gap_ok and grid_steps > 0, f"{n_gaps} gaps, {grid_steps} wider grid steps drawn through")

    # An expiry whose quotes ALL fail is listed with 0 kept and its reasons.
    wide_dir = Path(tempfile.mkdtemp())
    wagg = wide_dir / "aggregates/symbol=SPY/date=2026-09-29/agg_150400_000000.parquet"
    wagg.parent.mkdir(parents=True)
    t = pq.read_table(sm_reader.agg_dir / "date=2026-09-29" / ("agg_" + good_key.split("/")[1]))
    first_exp = min(t.column("expiry").to_pylist())
    rows_w = t.to_pylist()
    for r in rows_w:
        if r["expiry"] == first_exp:
            for pre in ("call_", "put_"):
                if r[pre + "bid"]:
                    r[pre + "ask"] = r[pre + "bid"] * 3            # spread far too wide
    pq.write_table(pa.Table.from_pylist(rows_w, schema=t.schema), wagg)
    ww = server.SmileCache(sm_state, server.StoreReader(wide_dir, "SPY"))
    sm_state.set({"poll_key": "date=2026-09-29/150400_000000.parquet",
                  "session_date": "2026-09-29", "spot": spot})
    we = next((e for e in ww.get()["expiries"] if e["expiry"] == first_exp), None)
    ok &= check("an expiry whose quotes all fail stays listed: 0 kept, with reasons",
                we is not None and we["retained"] == 0 and we["points"] == []
                and we["excluded"].get("spread too wide", 0) > 0, str(we and we["excluded"]))

    # v1 snapshot: permanently unavailable, cached (no re-read per request)
    v1_state = server.State()
    v1_dir = Path(tempfile.mkdtemp())
    (v1_dir / "aggregates/symbol=SPY/date=2026-09-28").mkdir(parents=True)
    import shutil as _sh
    _sh.copy(sys.argv[2] if len(sys.argv) > 2 else next(iter(sorted(
        (BACKUP / "aggregates/symbol=SPY/date=2026-09-28").glob("agg_*.parquet")))),
        v1_dir / "aggregates/symbol=SPY/date=2026-09-28/agg_150000_000000.parquet")
    v1_state.set({"poll_key": "date=2026-09-28/150000_000000.parquet",
                  "session_date": "2026-09-28", "spot": spot})
    v1 = server.SmileCache(v1_state, server.StoreReader(v1_dir, "SPY")).get()
    ok &= check("pre-v2 snapshot: unavailable, says why, not transient",
                not v1["available"] and "schema v2" in v1["reason"] and not v1.get("transient"))

    # transient read failure: retried on the SAME poll, recovers
    bad_state = server.State()
    bdir = Path(tempfile.mkdtemp())
    bagg = bdir / "aggregates/symbol=SPY/date=2026-09-29/agg_150400_000000.parquet"
    bagg.parent.mkdir(parents=True)
    good_bytes = (sm_reader.agg_dir / "date=2026-09-29" / ("agg_" + good_key.split("/")[1])).read_bytes()
    bagg.write_bytes(b"not parquet")
    bad_state.set({"poll_key": "date=2026-09-29/150400_000000.parquet",
                   "session_date": "2026-09-29", "spot": spot})
    old_retry = server.SMILE_RETRY_SECONDS
    server.SMILE_RETRY_SECONDS = 0.2
    try:
        bc = server.SmileCache(bad_state, server.StoreReader(bdir, "SPY"))
        b1 = bc.get()
        bagg.write_bytes(good_bytes)
        b2 = bc.get()                      # within the retry window: still cached failure
        time.sleep(0.3)
        b3 = bc.get()
    finally:
        server.SMILE_RETRY_SECONDS = old_retry
    ok &= check("unreadable snapshot -> transient failure, not cached for good",
                b1.get("transient") and not b1["available"] and b2.get("transient")
                and b3["available"], f"{b1.get('reason')} -> {b3['available']}")
    for bad_spot in (None, 0, float("nan")):
        bad_state.set({"poll_key": "date=2026-09-29/150400_000000.parquet",
                       "session_date": "2026-09-29", "spot": bad_spot})
        r_ = server.SmileCache(bad_state, server.StoreReader(bdir, "SPY")).get()
        ok &= check(f"invalid spot {bad_spot!r} -> unavailable, JSON-safe",
                    not r_["available"] and r_["spot"] is None
                    and json.dumps(r_, allow_nan=False) is not None)

    print("status endpoint (feed-health panel)")
    import gex.validate as V
    rep = V.run(root, "SPY", d2, now=t2, fetch=lambda u: (503, b""))   # a report on disk
    stc = server.StatusCache(root, "SPY")
    stt = stc.get(now=t2 + timedelta(minutes=5))
    ok &= check("status carries market, collector, saved-count and report fields",
                set(stt) >= {"market", "collecting", "stored_last_10min", "last_stored_at",
                             "validation", "today_et"} and "open" in stt["market"])
    ok &= check("latest report summarised with its date and status",
                stt["validation"]["date"] == "2026-09-29"
                and stt["validation"]["status"] == rep["status"],
                f"{stt['validation']['status']} (fixtures carry older trade dates -> {rep['status']})")
    ok &= check("newest save time = newest COMPLETE poll (the metadata-only 15:00 one and the corrupt one are pairs too, but 15:04 is newest)",
                stt["last_stored_at"].startswith("2026-09-29T15:04:00"), stt["last_stored_at"])
    ok &= check("saved-in-last-10-min counts complete pairs only (15:00:30, 15:02, 15:03, 15:04)",
                stt["stored_last_10min"] == 4, str(stt["stored_last_10min"]))
    wk = stc.get(now=datetime(2026, 10, 3, 15, 0, tzinfo=timezone.utc))      # a Saturday
    ok &= check("on a weekend the last save is still found (not just today's folder)",
                wk["last_stored_at"].startswith("2026-09-29T15:04:00") and wk["stored_last_10min"] == 0
                and wk["market"]["open"] is False)
    qd = root / "quarantine" / "2026-09-29"
    qd.mkdir(parents=True)
    newest_stamp = server.StoreReader(root, "SPY").newest()[0].split("/")[1]
    (qd / f"meta_{newest_stamp}").write_bytes(b"")
    qq = server.StatusCache(root, "SPY").get(now=t2 + timedelta(minutes=5))
    ok &= check("a quarantined poll is not the 'last save'",
                not qq["last_stored_at"].startswith("2026-09-29T15:04:00"), qq["last_stored_at"])
    (qd / f"meta_{newest_stamp}").unlink()
    vj = root / "validation" / "date=2026-09-30.json"
    vj.write_text(json.dumps({"date": "2026-09-30", "status": "investigate",
                              "occ_oi": {"status": "match"}, "greeks": {"status": "consistent"},
                              "arithmetic": {"status": "reconciled"}, "unreadable_polls": [],
                              "previous_session_unreadable_polls": [{"stamp": "x"}]}))
    sv = server.StatusCache(root, "SPY").get(now=t2)["validation"]
    ok &= check("previous-session corruption is shown, not hidden behind three green checks",
                sv["status"] == "investigate" and sv["n_prev_unreadable_polls"] == 1)
    vj.unlink()
    from gex.collector import market_session
    ok &= check("market session excludes the collector's 45-min tail",
                market_session(datetime(2026, 9, 29, 20, 30, tzinfo=timezone.utc))[0] is False
                and market_session(datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc))[0] is True)

    ok &= check("ZERO network attempts during the whole run", not NET_ATTEMPTS, str(NET_ATTEMPTS))
    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
