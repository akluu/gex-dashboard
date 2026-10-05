"""Intraday max/min gamma strikes and the intraday GEX ratio
(gex/gammatracks.py, server.TracksCache), with the design rules.

Checked, in order of how badly they could go wrong unnoticed:
  1. Real sessions: one point per usable stored poll up to the one on
     screen; the last point's ratio equals the header's EXACTLY; extrema
     agree with an independent recompute and are scale-invariant.
  2. Definition: all-expiry netting per strike, positive-only max /
     negative-only min, "none of this sign" distinct from invalid data,
     ties nearer spot then lower strike, runner-up gap and exact-tie flag,
     missing spot keeps raw extrema and ratio.
  3. Unreadable aggregate -> a TIMESTAMPED unavailable entry (never bridged),
     unreadable metadata -> counted apart; neither cached; retried.
  4. Anchoring, incremental reads, quarantine, single-flight builds, no
     publication for an anchor that is no longer on screen, bounded life.

Usage:  python3 tests/test_gamma_tracks.py [DATA_ROOT]     (default: the VM backup)
"""
import json, math, sys, threading, time, urllib.request
from collections import defaultdict
from http.server import ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import pyarrow.parquet as pq
import gex.server as server
from gex.aggregate import gex_ratio
from gex.gammatracks import extrema

DEFAULT_ROOT = HERE.parent / "data/vm_backup/gex_data"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def close(a, b, rel=1e-9):
    return a is not None and b is not None and abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def g(k, cg=0.0, pg=0.0):
    return {"strike": k, "call_gamma_oi": cg, "put_gamma_oi": pg}


def test_definition():
    print("2. definition (hand cases)")
    ok = True
    # 100 nets +10 (one expiry) - 4 (another) = +6; 101: -9; 102: +6.5; 99: -2.
    rows = [g(100, cg=10), g(100, pg=4), g(101, pg=9), g(102, cg=6.5), g(99, pg=2)]
    e = extrema(rows, 100.4)
    ok &= check("nets per strike across expiries; max = most positive",
                e["ok"] and e["max"]["strike"] == 102 and e["max"]["raw"] == 6.5)
    ok &= check("runner-up and gap of the same sign",
                e["max"]["runner_up"] == 100 and close(e["max"]["gap"], 0.5 / 6.5) and not e["max"]["tied"])
    ok &= check("min = most negative, its runner-up and gap",
                e["min"]["strike"] == 101 and e["min"]["runner_up"] == 99 and close(e["min"]["gap"], 7 / 9))
    ok &= check("ratio is gex_ratio() of the same rows", e["ratio"] == gex_ratio(rows) and close(e["ratio"], 12.5 / 23.5))
    ok &= check("distance from that poll's spot", close(e["max"]["dist"], 102 / 100.4 - 1))
    t = extrema([g(98, cg=5), g(103, cg=5)], 101.0)
    ok &= check("exact tie: nearer spot wins, flagged", t["max"]["strike"] == 103 and t["max"]["tied"])
    t = extrema([g(98, cg=5), g(103, cg=5)], 100.5)
    ok &= check("equidistant tie: lower strike", t["max"]["strike"] == 98 and t["max"]["tied"])
    t = extrema([g(98, cg=5), g(103, cg=5), g(90, pg=1)], None)
    ok &= check("no spot: lower strike, distance unavailable, raw extrema and ratio kept",
                t["ok"] and t["max"]["strike"] == 98 and t["max"]["dist"] is None and t["min"]["strike"] == 90
                and t["ratio"] is not None and t["spot_valid"] is False)
    p = extrema([g(100, cg=1), g(101, cg=2)], 100.0)
    ok &= check("no negative strike: min is 'none', not invalid",
                p["ok"] and p["min"] is None and "negative" in p["min_none"] and p["max"]["strike"] == 101)
    n = extrema([g(100, pg=1), g(101, pg=2)], 100.0)
    ok &= check("no positive strike: max is 'none'", n["ok"] and n["max"] is None and n["min"]["strike"] == 101)
    z = extrema([g(100, cg=3, pg=3)], 100.0)
    ok &= check("all strikes net zero: both 'none', ratio unavailable",
                z["ok"] and z["max"] is None and z["min"] is None and z["ratio"] is None)
    for bad, label in ((None, "missing"), (float("nan"), "NaN"), (float("inf"), "inf"), (True, "bool")):
        b = extrema(rows + [g(105, cg=bad)], 100.0)
        ok &= check(f"{label} gamma: the whole poll unavailable, not computed from the rest",
                    not b["ok"] and b["max"] is None and b["ratio"] is None and "unavailable" in b["reason"])
    b = extrema(rows + [{"strike": None, "call_gamma_oi": 1.0, "put_gamma_oi": 0.0}], 100.0)
    ok &= check("missing strike: unavailable", not b["ok"])
    ok &= check("no rows: unavailable", not extrema([], 100.0)["ok"])
    S = 765.15
    scaled = [{**r, "call_gamma_oi": r["call_gamma_oi"] * S * S * 0.01, "put_gamma_oi": r["put_gamma_oi"] * S * S * 0.01}
              for r in rows]
    es = extrema(scaled, 100.4)
    ok &= check("scale-invariant: same strikes and gaps in dollar units",
                (es["max"]["strike"], es["min"]["strike"]) == (102, 101) and close(es["max"]["gap"], e["max"]["gap"])
                and close(es["ratio"], e["ratio"]))
    return ok


def usable_keys(reader, day, upto):
    out = []
    for k, m, a in reader.complete(day):
        if k <= upto and pq.read_table(m).to_pylist()[0].get("usable"):
            out.append(k)
    return sorted(out)


def test_store(root):
    ok = True
    reader = server.StoreReader(root, "SPY")
    day = "date=2026-09-29"
    polls = list(reader.complete(day))                  # newest first
    last = polls[0][0]
    st = server.State()
    st.set({"poll_key": last})
    cache = server.TracksCache(st, reader)
    out = cache.get()
    s = out["series"]

    print("1. real session 2026-09-29")
    want = usable_keys(reader, day, last)
    ok &= check("one point per usable stored poll up to the one on screen", len(s) == len(want), f"{len(s)} points")
    ok &= check("all ok, nothing transient", all(p["status"] == "ok" for p in s) and not out["transient"])
    k, m, a = polls[0]
    view = server.build_view(*reader.load(k, m, a))
    ok &= check("last point's ratio == the header's GEX ratio, exactly", s[-1]["ratio"] == view["gex_ratio"],
                f"{s[-1]['ratio']}")
    ok &= check("every max > 0, every min < 0, every ratio in [0, 1]",
                all(p["max"]["raw"] > 0 and p["min"]["raw"] < 0 and 0 <= p["ratio"] <= 1 for p in s))
    by_t = {p["t"]: p for p in s}
    agree = scale_ok = True
    for k, m, a in (polls[0], polls[len(polls) // 2], polls[-1]):
        meta = pq.read_table(m).to_pylist()[0]
        rows = pq.read_table(a, columns=["strike", "call_gamma_oi", "put_gamma_oi"]).to_pylist()
        nets = defaultdict(list)
        for x in rows:
            nets[x["strike"]].append(x["call_gamma_oi"] - x["put_gamma_oi"])
        n = {kk: math.fsum(v) for kk, v in nets.items()}
        mx, mn = max(n, key=n.get), min(n, key=n.get)
        p = by_t[meta["received_at"]]
        agree &= p["max"]["strike"] == mx and p["min"]["strike"] == mn and close(p["max"]["raw"], n[mx])
        S = meta["spot"]
        sc = [{**x, "call_gamma_oi": x["call_gamma_oi"] * S * S * 0.01, "put_gamma_oi": x["put_gamma_oi"] * S * S * 0.01}
              for x in rows]
        es = extrema(sc, S)
        scale_ok &= es["max"]["strike"] == p["max"]["strike"] and es["min"]["strike"] == p["min"]["strike"]
    ok &= check("independent recompute (fsum, separate code) agrees on 3 polls", agree)
    ok &= check("scale-invariant on real polls: same strikes in dollar units", scale_ok)

    print("4. anchoring, incremental reads, quarantine, bounded life")
    ok &= check("a clean result has a bounded life (<= 60 s)",
                0 < cache._retry_at - time.time() <= server.OI_REFRESH_SECONDS + 1)
    mid = polls[len(polls) // 2][0]
    calls = []
    real_one = cache._one
    cache._one = lambda k, m, a: (calls.append(k), real_one(k, m, a))[1]
    st.set({"poll_key": mid})
    om = cache.get()
    ok &= check("anchored: nothing after the poll on screen", len(om["series"]) == len(usable_keys(reader, day, mid)))
    ok &= check("incremental: moving the anchor back re-reads nothing", calls == [])
    fresh = server.TracksCache(st, reader)
    fresh.get()                                          # built up to mid
    calls2 = []
    real_one2 = fresh._one
    fresh._one = lambda k, m, a: (calls2.append(k), real_one2(k, m, a))[1]
    nxt = min(k for k, _, _ in polls if k > mid)
    st.set({"poll_key": nxt})
    fresh.get()
    ok &= check("incremental: one new poll -> exactly one new read", calls2 == [nxt], repr(calls2))

    st.set({"poll_key": last})
    gone = polls[5][0]
    real_complete = reader.complete
    reader.complete = lambda d, _rc=real_complete: (x for x in _rc(d) if x[0] != gone)
    try:
        cache._key = None
        oq = cache.get()
    finally:
        reader.complete = real_complete
    ok &= check("a quarantined poll drops out of the series and the cache",
                len(oq["series"]) == len(s) - 1 and gone not in cache._polls)

    print("3. unreadable files")
    flaky = server.TracksCache(st, reader)
    bad_agg, bad_meta = str(polls[10][2]), str(polls[20][1])
    real_read = server.pq.read_table

    class FlakyPQ:
        ParquetFile = pq.ParquetFile      # schema checks (server._read_rows) stay real
        @staticmethod
        def read_table(path, *a, **k):
            if str(path) in (bad_agg, bad_meta):
                raise OSError("simulated unreadable file")
            return real_read(path, *a, **k)
    server.pq = FlakyPQ
    try:
        of = flaky.get()
    finally:
        server.pq = pq
    un = [p for p in of["series"] if p["status"] == "unreadable"]
    ok &= check("unreadable aggregate: a timestamped unavailable entry, not dropped or bridged",
                len(un) == 1 and un[0]["t"] and un[0].get("max") is None and un[0].get("ratio") is None)
    ok &= check("unreadable metadata: not placed, counted apart", of["n_unplaceable"] == 1 and of["n_unreadable"] == 1)
    ok &= check("neither cached; the result is transient and retried soon",
                polls[10][0] not in flaky._polls and polls[20][0] not in flaky._polls and of["transient"]
                and flaky._retry_at - time.time() <= server.OI_RETRY_SECONDS + 1)
    flaky._retry_at = 0
    orr = flaky.get()
    ok &= check("once readable again, both recover", not orr["transient"] and len(orr["series"]) == len(s))

    print("3b. bad metadata values")
    import pyarrow as pa
    odd = server.TracksCache(st, reader)
    nan_meta, null_t_meta = str(polls[30][1]), str(polls[40][1])
    real_read2 = server.pq.read_table

    class OddMetaPQ:
        ParquetFile = pq.ParquetFile      # schema checks (server._read_rows) stay real
        @staticmethod
        def read_table(path, *a, **k):
            t = real_read2(path, *a, **k)
            if str(path) == nan_meta:
                return pa.Table.from_pylist([{**t.to_pylist()[0], "spot": float("nan")}])
            if str(path) == null_t_meta:
                return pa.Table.from_pylist([{**t.to_pylist()[0], "received_at": None}])
            return t
    server.pq = OddMetaPQ
    try:
        oo = odd.get()
    finally:
        server.pq = pq
    try:
        json.dumps(oo, allow_nan=False)
        strict = True
    except ValueError:
        strict = False
    nanpt = [p for p in oo["series"] if p["t"] == pq.read_table(polls[30][1]).to_pylist()[0]["received_at"]]
    ok &= check("NaN spot in metadata: strict JSON, spot None, raw extrema and ratio kept",
                strict and nanpt and nanpt[0]["spot"] is None and nanpt[0]["max"]["dist"] is None
                and nanpt[0]["ratio"] is not None)
    ok &= check("null timestamp: counted unplaceable, not cached, retried",
                oo["n_unplaceable"] == 1 and polls[40][0] not in odd._polls and oo["transient"])

    print("4b. single-flight and obsolete anchors")
    rounds_ok, worst = True, 0
    for _ in range(20):
        sf = server.TracksCache(st, reader)
        sf._polls = dict(cache._polls); sf._day = cache._day      # warm, so each round is quick
        real_build = sf._build
        def slow(key, _rb=real_build):
            time.sleep(0.05)
            return _rb(key)
        sf._build = slow
        res = []
        th = [threading.Thread(target=lambda: res.append(sf.get())) for _ in range(5)]
        [t.start() for t in th]; [t.join() for t in th]
        worst = max(worst, sf.builds)
        rounds_ok &= sf.builds == 1 and len(res) == 5 and all(r["poll_key"] == last for r in res)
    ok &= check("20 rounds x 5 concurrent requests -> exactly one build each", rounds_ok, f"max builds={worst}")
    ob = server.TracksCache(st, reader)
    real_build_ob = ob._build
    def moving(key):
        out = real_build_ob(key)
        st.set({"poll_key": mid})                        # the screen moved on mid-build
        return out
    ob._build = moving
    r1 = ob.get()
    ok &= check("a result whose anchor left the screen is returned but not published",
                r1["poll_key"] == last and ob._key is None)
    st.set({"poll_key": last})

    print("measured data lag (audit F1)")
    ml = server.median_lag_seconds
    pts = [{"t": "2026-09-29T14:00:00+00:00", "newest_trade": "2026-09-29T09:44:00"},   # EDT: 13:44Z -> 16 min
           {"t": "2026-09-29T14:01:00+00:00", "newest_trade": "2026-09-29T09:45:00"},   # 16 min
           {"t": "2026-09-29T14:02:00+00:00", "newest_trade": "2026-09-29T09:47:00"},   # 15 min
           {"t": "2026-09-29T13:31:00+00:00", "newest_trade": "2026-09-28T16:00:00"},   # stale: excluded
           {"t": "2026-09-29T14:03:00+00:00", "newest_trade": "garbage"},               # malformed: excluded
           {"t": None, "newest_trade": "2026-09-29T09:50:00"}]
    ok &= check("median over LIVE points only (naive Eastern newest trade)", ml(pts, "2026-09-29") == 960.0,
                repr(ml(pts, "2026-09-29")))
    ok &= check("even count: mean of the middle two", ml(pts[:1] + pts[2:3], "2026-09-29") == 930.0)
    ok &= check("no live point: None, not a guess", ml(pts[3:], "2026-09-29") is None)
    ok &= check("real session: ~16 min (the audit measured 15.8 min median)",
                out.get("median_lag_s") and 900 < out["median_lag_s"] < 1020, f"{out.get('median_lag_s')}")

    print("route")
    server.Handler.bundles = {"SPY": server.bundle("SPY", state=st, tracks=cache)}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        cache._key = None
        body = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{srv.server_address[1]}/api/tracks", timeout=30).read())
        ok &= check("/api/tracks serves the anchored series", body["poll_key"] == last and len(body["series"]) == len(s))
        ok &= check("only the fields the page reads", set().union(*[p.keys() for p in body["series"]]) <= set(server.TRACKS_OUT))
    finally:
        srv.shutdown()
    return ok


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    ok = test_definition() & test_store(root)
    print()
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
