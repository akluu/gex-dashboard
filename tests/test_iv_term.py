"""IV term structure at fixed deltas and the 25Δ skew (gex/ivterm.py,
server.TermCache), with the bracket safeguards.

Usage:  python3 tests/test_iv_term.py [DATA_ROOT]      (default: the VM backup)
"""
import json, sys, threading, urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import pyarrow.parquet as pq
import gex.server as server
from gex import ivterm as I

DEFAULT_ROOT = HERE.parent / "data/vm_backup/gex_data"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def close(a, b, tol=1e-12):
    return a is not None and b is not None and abs(a - b) <= tol


def test_brackets():
    print("bracket safeguards (hand cases)")
    ok = True
    pts = [(100, 0.70, 0.20), (101, 0.55, 0.18), (102, 0.40, 0.17), (103, 0.20, 0.16), (104, 0.08, 0.17)]
    r = I.at_delta(pts, 0.50, 100.0)
    ok &= check("linear in delta between the bracketing pair", close(r["iv"], 0.18 + (0.55 - 0.50) / 0.15 * (0.17 - 0.18))
                and r["strikes"] == [101, 102], repr(r))
    ok &= check("exact match used as is", I.at_delta(pts, 0.40, 100.0)["iv"] == 0.17)
    ok &= check("no extrapolation past the last observation", I.at_delta(pts, 0.05, 100.0)["iv"] is None)
    dup = pts + [(102, 0.40, 0.19)]
    ok &= check("duplicate deltas with different IVs -> ambiguous", "ambiguous" in I.at_delta(sorted(dup), 0.40, 100.0)["reason"])
    bumpy = [(100, 0.60, 0.2), (101, 0.45, 0.2), (102, 0.55, 0.2), (103, 0.40, 0.2)]
    ok &= check("non-monotonic deltas -> ambiguous (two brackets)", "ambiguous" in I.at_delta(bumpy, 0.50, 100.0)["reason"])
    wide = [(100, 0.60, 0.2), (101, 0.30, 0.2)]
    ok &= check("bracket wider than 0.15 delta -> refused", "too wide" in I.at_delta(wide, 0.50, 100.0)["reason"])
    far = [(100, 0.55, 0.2), (104, 0.45, 0.2)]
    ok &= check("bracket wider than 3% of spot -> refused", "too wide" in I.at_delta(far, 0.50, 100.0)["reason"])
    rows = [{"strike": 100 + i, "call_iv": 0.2, "call_bid": 1.0, "call_ask": 1.1, "call_delta": d,
             "put_iv": 0.2, "put_bid": 1.0, "put_ask": 1.1, "put_delta": d - 1}
            for i, d in enumerate((0.9, 0.7, 0.5, 0.3, 0.1))]
    rows[1]["call_ask"] = 2.0                                   # wide quote
    rows[3]["call_delta"] = 1.2                                 # outside (0, 1)
    pts, ex = I.side_points(rows, "C", True)
    ok &= check("quote-valid and side-range filters, counted", len(pts) == 3 and ex.get("spread > 20% of mid") == 1
                and ex.get("delta outside the side's range") == 1, repr(ex))
    v2 = [{"strike": 100, "call_iv": 0.2, "call_bid": 1.0, "call_ask": 1.05, "call_oi": 10.0, "call_delta_oi": 500.0},
          {"strike": 101, "call_iv": 0.2, "call_bid": 1.0, "call_ask": 1.05, "call_oi": 0.0, "call_delta_oi": 0.0}]
    pts, ex = I.side_points(v2, "C", False)
    ok &= check("pre-v3: delta recovered where OI > 0, zero-OI unavailable",
                pts == [(100, 0.5, 0.2)] and ex.get("no delta") == 1)
    ok &= check("provenance comes from the schema: a v2 expiry with only zero-OI contracts still says 'recovered'",
                I.expiry_curve(v2[1:], 100.0, False)["delta_source"].startswith("recovered"))
    v3gap = [dict(v2[0], call_delta=None)]
    ok &= check("v3: a missing stored delta is unavailable, never recovered from the sums",
                I.side_points(v3gap, "C", True)[0] == [])
    ok &= check("an ambiguous delta at a bracket ENDPOINT -> ambiguous (a reproduced case)",
                "ambiguous" in I.at_delta([(100, .30, .20), (101, .30, .40), (102, .20, .20)], .25, 100.0)["reason"])
    c = I.expiry_curve(rows, 100.0, True)
    ok &= check("skew needs both 25Δ wings; else unavailable with a reason",
                (c["skew_25"]["vol_pts"] is None) == (c["values"]["25Δ put"]["iv"] is None or c["values"]["25Δ call"]["iv"] is None)
                and (c["skew_25"]["vol_pts"] is not None or c["skew_25"]["reason"]))
    return ok


def test_store(root):
    ok = True
    r = server.StoreReader(root, "SPY")

    def poll(day, cut):
        for k, m, a in r.complete(f"date={day}"):
            if k.split("/")[1][:6] <= cut:
                return k, m, a

    def term_at(k, m, a):
        v = server.build_view(*r.load(k, m, a)); v["poll_key"] = k
        st = server.State(); st.set(v)
        return server.TermCache(st, r).get(), v, st

    print("real snapshots")
    k, m, a = poll("2026-09-30", "160000")
    out, view, st = term_at(k, m, a)
    exps = out["expiries"]
    ok &= check("v3 noon: every expiry from the session on, stored deltas",
                out["available"] and exps[0]["expiry"] == "2026-09-30" and exps[0]["dte"] == 0
                and all(e["delta_source"].startswith("stored") for e in exps), f"{len(exps)} expiries")
    vals = [x["iv"] for e in exps for x in e["values"].values() if x["iv"] is not None]
    n_all = sum(len(e["values"]) for e in exps)
    ok &= check("values plausible for SPY (5%-80%) and mostly available",
                all(0.05 < x < 0.8 for x in vals) and len(vals) >= 0.9 * n_all, f"{len(vals)}/{n_all}")
    # Independent recompute of one value: 25Δ call of the 7th expiry, separate code.
    e7 = exps[6]
    rows = [x for x in pq.read_table(a).to_pylist() if x["expiry"] == e7["expiry"]]
    good = sorted((x["strike"], x["call_delta"], x["call_iv"]) for x in rows
                  if x["call_iv"] and x["call_iv"] > 0 and x["call_bid"] and x["call_bid"] > 0 and x["call_ask"] > x["call_bid"]
                  and (x["call_ask"] - x["call_bid"]) / ((x["call_ask"] + x["call_bid"]) / 2) <= 0.2
                  and x["call_delta"] is not None and 0 < x["call_delta"] < 1)
    lo = max((p for p in good if p[1] > 0.25), key=lambda p: p[0])
    hi = min((p for p in good if p[1] < 0.25), key=lambda p: p[0])
    indep = lo[2] + (lo[1] - 0.25) / (lo[1] - hi[1]) * (hi[2] - lo[2])
    got = e7["values"]["25Δ call"]
    ok &= check("independent recompute of a 25Δ call value agrees", close(got["iv"], indep, 1e-12)
                and got["strikes"] == [lo[0], hi[0]], f"{e7['expiry']}: {got['iv']:.5f} vs {indep:.5f}")
    ok &= check("skew = 25Δ put - 25Δ call in vol points, same expiry",
                close(e7["skew_25"]["vol_pts"], (e7["values"]["25Δ put"]["iv"] - e7["values"]["25Δ call"]["iv"]) * 100, 1e-9))
    ok &= check("each expiry carries its own scheduled close (16:15 ET -> 20:15 UTC in EDT)",
                exps[0]["close_utc"] == "2026-09-30T20:15:00+00:00"
                and all(e["close_utc"].endswith("+00:00") for e in exps))
    k2, m2, a2 = poll("2026-09-29", "160000")
    out2, _v, _s = term_at(k2, m2, a2)
    ok &= check("v2 day: deltas recovered (zero-OI unavailable), disclosed",
                out2["available"] and any(e["delta_source"].startswith("recovered") for e in out2["expiries"]))
    ok &= check("the result carries its own snapshot times (for the mixed-age warning)",
                out["received_at"] == view["feed"]["received_at"] and out["newest_trade"] == view["feed"]["newest_trade_time"])
    import time as _t
    tc = server.TermCache(st, r); tc.get()
    ok &= check("bounded life: a clean result is rechecked within 60 s", 0 < tc._retry_at - _t.time() <= server.OI_REFRESH_SECONDS + 1)
    r.rejected.add(k)
    try:
        tc._key = None
        rej = tc.get()
    finally:
        r.rejected.discard(k)
    ok &= check("a rejected source is not served", not rej["available"] and rej.get("transient"))
    k1, m1, a1 = poll("2026-09-24", "190000")
    out1, _v, _s = term_at(k1, m1, a1)
    ok &= check("v1 day: unavailable, says why", not out1["available"] and "v2" in out1["reason"])

    print("route")
    server.Handler.bundles = {"SPY": server.bundle("SPY", state=st, term=server.TermCache(st, r))}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        body = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{srv.server_address[1]}/api/term", timeout=10).read())
        ok &= check("/api/term serves the anchored result, no IV-ratio field (deferred)",
                    body["poll_key"] == k and body["available"] and "front_ratio" not in body)
    finally:
        srv.shutdown()
    return ok


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    ok = test_brackets() & test_store(root)
    print()
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
