"""Intraday GEX heatmap (gex/heatmap.py, server.TracksCache -> /api/heat,
web/gexmath.js), with the design rules (2026-10-01).

Checked, in order of how badly they could go wrong unnoticed:
  1. Real sessions: one column per tracks point, same times and order; every
     cell equals an independent fsum recompute from the stored aggregate; the
     UNWINDOWED per-strike nets agree with the tracks' extrema (max value and
     strike, min value and strike) and sum to the header's net gamma; an
     in-window extremum sits at its row with its value, and no in-window cell
     beats it.
  2. /api/tracks is byte-identical to the pre-heatmap code (commit 0fee3ed)
     on the same store -- the shared cache must not change it.
  3. Definition: absent strike -> None (never 0); invalid poll -> no values;
     rows = union of strikes within the window of ANY valid spot; an
     off-window global winner has no row; values rounded to 4 decimals only.
  4. A heat-assembly failure leaves /api/tracks intact and is transient.
  5. Page maths under node: ramp lightness monotone from the grey midpoint to
     each pole; one cap (nearest-rank p99) over all transmitted cells with
     each column's OWN spot; spans capped at the gap and never extended past
     the last snapshot.

Usage:  python3 tests/test_heatmap.py [DATA_ROOT]     (default: the VM backup)
Needs git and node (Mac only), like test_gex_ratio.py.
"""
import json, math, shutil, subprocess, sys, tempfile, threading, urllib.request
from collections import defaultdict
from http.server import ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import pyarrow.parquet as pq
import gex.server as server
from gex import heatmap
from gex.gammatracks import strike_nets

DEFAULT_ROOT = HERE.parent / "data/vm_backup/gex_data"
PRE_HEATMAP = "0fee3ed"          # last commit before step 5


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def close(a, b, rel=1e-9):
    return a is not None and b is not None and abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def g(k, cg=0.0, pg=0.0):
    return {"strike": k, "call_gamma_oi": cg, "put_gamma_oi": pg}


def col(t, spot, rows, status="ok"):
    nets, bad = strike_nets(rows)
    return {"t": t, "spot": spot, "newest_trade": None, "age_s": None, "status": status, "reason": None,
            "nets": None if bad or not nets else nets}


def test_definition():
    print("3. definition (hand cases)")
    ok = True
    a = col("t1", 100.0, [g(100, cg=10), g(100, pg=4), g(101, pg=9), g(150, cg=99)])
    b = col("t2", 101.0, [g(100, cg=1), g(102, cg=2.123456789)])
    bad = col("t3", 100.0, [g(100, cg=1), g(101, cg=float("nan"))], status="invalid")
    out = heatmap.grid([a, b, bad], window=0.05)
    ok &= check("rows = union of strikes within [min spot x 0.95, max spot x 1.05]; 150 is outside",
                out["strikes"] == [100.0, 101.0, 102.0] and out["reason"] is None)
    v1, v2, v3 = (c["values"] for c in out["columns"])
    ok &= check("nets across expiries: 100 = +10 - 4", v1[0] == 6.0 and v1[1] == -9.0)
    ok &= check("a strike absent from a poll is None, never 0", v1[2] is None and v2[1] is None)
    ok &= check("values rounded to 4 decimals only", v2[2] == 2.1235)
    ok &= check("invalid poll: no values at all (not computed from the other rows)", v3 is None and bad["nets"] is None)
    ok &= check("column order and fields kept", [c["t"] for c in out["columns"]] == ["t1", "t2", "t3"]
                and set(out["columns"][0]) == {"t", "spot", "newest_trade", "age_s", "status", "reason", "values"})
    nospot = col("t4", None, [g(100, cg=1), g(400, cg=1)])
    o2 = heatmap.grid([a, nospot], window=0.05)
    ok &= check("a column without a spot keeps its raw values; its strikes don't widen the window",
                o2["strikes"] == [100.0, 101.0] and o2["columns"][1]["values"] == [1.0, None])
    o3 = heatmap.grid([nospot], window=0.05)
    ok &= check("no valid spot anywhere: no rows, a reason, columns still listed",
                o3["strikes"] == [] and o3["reason"] and len(o3["columns"]) == 1)
    # Off-window winner: the global max (150, +99) has no row; the in-window max is 100.
    vals = dict(zip(out["strikes"], v1))
    ok &= check("off-window global max has no row; the visible max is the in-window one",
                150.0 not in out["strikes"] and max(v for v in vals.values() if v is not None) == 6.0)
    return ok


def fsum_nets(agg_path):
    rows = pq.read_table(agg_path, columns=["strike", "call_gamma_oi", "put_gamma_oi"]).to_pylist()
    nets = defaultdict(list)
    for x in rows:
        nets[x["strike"]].append(x["call_gamma_oi"] - x["put_gamma_oi"])
    return {k: math.fsum(v) for k, v in nets.items()}


def test_store(root):
    ok = True
    for day in ("date=2026-09-29", "date=2026-09-30"):
        print(f"1. real session {day[5:]}")
        reader = server.StoreReader(root, "SPY")
        polls = list(reader.complete(day))                   # newest first
        last = polls[0][0]
        st = server.State(); st.set({"poll_key": last})
        cache = server.TracksCache(st, reader)
        tr, ht = cache.get(), cache.get_heat()
        s, cols, ks = tr["series"], ht["columns"], ht["strikes"]
        ok &= check("same anchor, one column per tracks point, same times in the same order",
                    ht["poll_key"] == tr["poll_key"] == last and [c["t"] for c in cols] == [p["t"] for p in s],
                    f"{len(cols)} columns x {len(ks)} strikes")
        ok &= check("rows ascending, unique, within +-12% of the session's spot range",
                    ks == sorted(set(ks)) and min(ks) >= min(p["spot"] for p in s) * 0.88 - 1e-9
                    and max(ks) <= max(p["spot"] for p in s) * 1.12 + 1e-9)
        ok &= check("every value is rounded to 4 decimals",
                    all(v == round(v, 4) for c in cols if c["values"] for v in c["values"] if v is not None))
        recs = {cache._polls[k]["t"]: cache._polls[k] for k in cache._polls}
        agree = place = beat = summ = True
        n_place = 0
        for p, c in zip(s, cols):
            if p["status"] != "ok":
                agree &= c["values"] is None
                continue
            nets = recs[p["t"]]["nets"]                      # UNWINDOWED, as cached
            pos = {k: v for k, v in nets.items() if v > 0}
            neg = {k: v for k, v in nets.items() if v < 0}
            if p["max"]:
                agree &= p["max"]["raw"] == max(pos.values()) and nets[p["max"]["strike"]] == p["max"]["raw"]
            if p["min"]:
                agree &= p["min"]["raw"] == min(neg.values()) and nets[p["min"]["strike"]] == p["min"]["raw"]
            row = dict(zip(ks, c["values"]))
            if p["max"] and p["max"]["strike"] in row:       # in-window winner: at its row, nothing beats it
                n_place += 1
                place &= row[p["max"]["strike"]] == round(p["max"]["raw"], 4)
                beat &= max(v for v in row.values() if v is not None) == round(p["max"]["raw"], 4)
            elif p["max"]:                                   # off-window winner: every visible value is smaller
                beat &= all(v is None or v <= p["max"]["raw"] for v in row.values())
        ok &= check("unwindowed nets agree with the tracks' max/min (value and strike) on every usable poll", agree)
        ok &= check("an in-window max-gamma strike is at its row with its value, and no in-window cell beats it",
                    place and beat, f"{n_place} in-window winners")
        # Unwindowed column sum == the header's net gamma for the poll on screen.
        k, m, a = polls[0]
        view = server.build_view(*reader.load(k, m, a))
        tot = math.fsum(recs[s[-1]["t"]]["nets"].values())
        ok &= check("unwindowed column sum == the header's net_gamma_raw (1e-12: summation order)",
                    close(tot, view["net_gamma_raw"], 1e-12), f"{tot:.4f} vs {view['net_gamma_raw']:.4f}")
        # Independent recompute from the raw aggregate files.
        indep = True
        by_t = {pq.read_table(mm).to_pylist()[0]["received_at"]: aa for kk, mm, aa in (polls[0], polls[len(polls) // 2], polls[-1])}
        for c in cols:
            if c["t"] in by_t:
                f = fsum_nets(by_t[c["t"]])
                indep &= all(v is None and kk not in f or v == round(f[kk], 4) or close(v, f[kk], 1e-12)
                             for kk, v in zip(ks, c["values"]))
        ok &= check("first, middle and last columns == an independent fsum recompute", indep)
        n_abs = sum(v is None for c in cols if c["values"] for v in c["values"])
        print(f"     (absent cells in the window: {n_abs})")
    return ok


def test_tracks_unchanged(root):
    print("2. /api/tracks byte-identical to the pre-heatmap code")
    tmp = Path(tempfile.mkdtemp())
    try:
        arc = subprocess.run(["git", "-C", str(HERE.parent), "archive", PRE_HEATMAP, "gex"],
                             capture_output=True, check=True).stdout
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=arc, check=True)
        (tmp / "gex").rename(tmp / "gexold")
        sys.path.insert(0, str(tmp))
        import gexold.server as old
        ok = True
        for day in ("date=2026-09-29", "date=2026-09-30"):
            outs = []
            for mod in (old, server):
                r = mod.StoreReader(root, "SPY"); last = list(r.complete(day))[0][0]
                st = mod.State(); st.set({"poll_key": last})
                outs.append(json.dumps(mod.TracksCache(st, r).get()))
            ok &= check(f"{day[5:]}: identical bytes", outs[0] == outs[1], f"{len(outs[1])} bytes")
        return ok
    finally:
        sys.path.remove(str(tmp)) if str(tmp) in sys.path else None
        shutil.rmtree(tmp, ignore_errors=True)


def test_route_and_isolation(root):
    print("4. route, strict JSON, and a heat failure isolated from the tracks")
    ok = True
    reader = server.StoreReader(root, "SPY")
    day = "date=2026-09-30"
    last = list(reader.complete(day))[0][0]
    st = server.State(); st.set({"poll_key": last})
    cache = server.TracksCache(st, reader)
    server.Handler.bundles = {"SPY": server.bundle("SPY", state=st, tracks=cache)}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}"
    strict = lambda raw: json.loads(raw, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
    real_grid = heatmap.grid
    try:
        body = strict(urllib.request.urlopen(url + "/api/heat", timeout=60).read())
        ok &= check("/api/heat serves the anchored grid as strict JSON",
                    body["poll_key"] == last and body["strikes"] and len(body["columns"]) > 100)
        cache._key = None
        heatmap.grid = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        tr = strict(urllib.request.urlopen(url + "/api/tracks", timeout=60).read())
        ht = strict(urllib.request.urlopen(url + "/api/heat", timeout=60).read())
        ok &= check("assembly failure: tracks still served in full", tr["poll_key"] == last and len(tr["series"]) > 100
                    and not tr["transient"])
        ok &= check("... and the heat answer is empty, transient, with the reason",
                    ht["strikes"] == [] and ht["transient"] and "heatmap assembly failed" in ht["reason"])
    finally:
        heatmap.grid = real_grid
        srv.shutdown()
    return ok


JS = r"""
const G = require(process.argv[2]);
const out = {};
const r = G.heatRamp(), L = h => G.hexToOklab(h)[0];
out.mono = ["pos", "neg"].every(a => r[a].every((h, i) => i === 0 || L(h) >= L(r[a][i - 1]) - 1e-9));
out.ends = r.pos[0] === G.HEAT.mid && r.neg[0] === G.HEAT.mid && r.pos[r.steps] === G.HEAT.pos && r.neg[r.steps] === G.HEAT.neg;
out.colors = [G.heatColor(5, 10, r) === r.pos[16], G.heatColor(-50, 10, r) === r.neg[r.steps],
              G.heatColor(0, 10, r) === r.pos[0], G.heatColor(null, 10, r) === null, G.heatColor(3, 0, r) === r.pos[0]];
const cols = [{values: [1, -2, null, 3], spot: 100}, {values: null, spot: 100}, {values: [4], spot: null}];
out.capRaw = G.heatCap(cols, "raw", 0.5);                 // [1,2,3,4] -> nearest rank 2
out.capUsd = G.heatCap(cols, "usd", 1.0);                 // spot-less column excluded: 3 * 100^2 * 0.01
out.capOwnSpot = G.heatCap([{values: [1], spot: 100}, {values: [1], spot: 200}], "usd", 1.0);
out.capNone = G.heatCap([{values: null, spot: 1}], "raw");
out.capZero = G.heatCap([{values: [0, 0], spot: 1}], "raw");
// Edge case: 100 zeros and one positive -> the p99 is 0; fall back to the max, and the value is NOT painted as zero.
const sparse = G.heatCap([{values: [...Array(100).fill(0), 7], spot: 1}], "raw");
out.capSparse = [sparse, G.heatColor(7, sparse.cap, r) === r.pos[r.steps]];
// Geometry: 501 snapshots one second apart on a 400 px plot (a reproduced case).
const ms = Array.from({length: 501}, (_, i) => i * 1000);
const spans = G.heatSpans(ms, 180000), xOf = t => 10 + t / 500000 * 400;
const geo = G.heatGeometry(spans.map(([a, b]) => [xOf(a), xOf(b)]), 2, 10, 410);
out.denseLast = G.heatOwner(geo, 409.5);                  // the final painted pixel belongs to the LAST snapshot
const paint = new Array(420).fill(-1);                    // paint in time order, as the page does
geo.forEach(([a, b], i) => { for (let p = a; p < b; p++) paint[p] = i; });
out.paintEqualsHover = paint.every((o, p) => o === -1 ? G.heatOwner(geo, p + 0.5) === -1 : G.heatOwner(geo, p + 0.5) === o);
out.hidden = G.heatHidden(geo);
out.inBounds = geo.every(([a, b]) => a >= 10 && b <= 410 && b - a >= 2);
// Sparse: every column gets whole pixels, the last is 2 px at the right edge, a gap stays empty.
const g2 = G.heatGeometry(G.heatSpans([0, 60000, 120000, 600000], 180000).map(([a, b]) => [10 + a / 600000 * 400, 10 + b / 600000 * 400]), 2, 10, 410);
out.sparseGeo = g2;
// Rows: fractional band edges (strikes 100..140, $1 apart, on 157.3 px) partition the pixels; paint owner == hover owner.
const rowsJ = Array.from({length: 41}, (_, j) => ({k: 100 + j, lo: 99.5 + j, hi: 100.5 + j}));
const yOf = p => 20.3 + (140.5 - p) / 41 * 157.3;
G.heatRowPixels(rowsJ, yOf);
const rpaint = new Array(200).fill(null);
for (const r of rowsJ) for (let p = r.top; p < r.bottom; p++) rpaint[p] = (rpaint[p] === null ? r.k : "OVERLAP");
out.rowsNoOverlap = !rpaint.includes("OVERLAP") && rowsJ.every((r, j) => j === 0 || r.bottom === rowsJ[j - 1].top);
out.rowsPaintEqHover = rpaint.every((k, p) => { const h = G.heatRowAt(rowsJ, p + 0.6); return k === null ? h === null : h && h.k === k; });
out.visible = [...G.heatVisible([[0, 4], [2, 6], [3, 5]])].sort();     // column 1 is covered by 2 except [5, 6)
out.spans = G.heatSpans([0, 60, 120, 500, 560], 180);
out.median = [G.median([3, 1, 2]), G.median([4, 1, 3, 2]), G.median([])];
console.log(JSON.stringify(out));
"""


def test_js():
    print("5. page maths (web/gexmath.js under node)")
    if not shutil.which("node"):
        return check("node available", False)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(JS)
    try:
        o = json.loads(subprocess.run(["node", f.name, str(HERE.parent / "web/gexmath.js")],
                                      capture_output=True, text=True, check=True).stdout)
    finally:
        Path(f.name).unlink()
    ok = True
    ok &= check("ramp lightness rises monotonically from the grey midpoint to each pole", o["mono"])
    ok &= check("ramp ends are exactly the midpoint and the two poles", o["ends"])
    ok &= check("linear intensity, saturation at the cap, zero = midpoint, null = no colour", all(o["colors"]))
    ok &= check("cap = nearest-rank quantile over all valid cells", o["capRaw"] == {"cap": 2, "rule": "quantile"})
    ok &= check("dollar cap uses each column's own spot; a spot-less column has no dollar values",
                close(o["capUsd"]["cap"], 300.0) and close(o["capOwnSpot"]["cap"], 400.0))
    ok &= check("cap rules: no valid cell -> none; all zero -> 0",
                o["capNone"] == {"cap": None, "rule": "none"} and o["capZero"] == {"cap": 0, "rule": "all zero"})
    ok &= check("sparse non-zero grid (p99 = 0): falls back to the max, value painted at full colour, not as zero",
                o["capSparse"][0] == {"cap": 7, "rule": "max"} and o["capSparse"][1])
    ok &= check("dense columns: the last painted pixel's tooltip is the LAST snapshot (the 501-at-400px case)",
                o["denseLast"] == 500)
    ok &= check("painting in time order and hovering give the same owner at every pixel", o["paintEqualsHover"])
    ok &= check("all columns whole-pixel, >= 2 px, inside the plot; hidden ones counted",
                o["inBounds"] and o["hidden"] > 0, f"{o['hidden']} hidden of 501")
    ok &= check("sparse geometry: hold to next, gap left empty, last column 2 px at the right edge",
                o["sparseGeo"] == [[10, 50], [50, 90], [90, 92], [408, 410]])
    ok &= check("strike rows partition the pixels at fractional edges (no overlap, shared boundaries)", o["rowsNoOverlap"])
    ok &= check("row painted at a pixel == row the tooltip picks there", o["rowsPaintEqHover"])
    ok &= check("visible columns = those owning a pixel", o["visible"] == [0, 1, 2])
    ok &= check("spans: hold to the next snapshot, gap beyond the cap, last not extended",
                o["spans"] == [[0, 60], [60, 120], [120, 120], [500, 560], [560, 560]])
    ok &= check("median", o["median"] == [2, 2.5, None])
    return ok


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    ok = test_definition() & test_store(root) & test_tracks_unchanged(root) & test_route_and_isolation(root) & test_js()
    print()
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
