"""GEX Ratio, raw units (gamma x OI x 100, delta shares) and the units toggle's
math -- Python (gex/aggregate.py) and the page's own JS (web/gexmath.js, run
under node), checked against hand-worked cases and real stored snapshots.

Five things, in order of how badly they could go wrong unnoticed:

  1. Stored numbers did not move: net_exposure()'s pre-existing fields equal,
     to the bit, both the metadata the collector stored and the pre-change
     net_exposure() read from git (the reproduce-before-trusting rule).
  2. The ratio nets calls against puts PER STRIKE across the selected
     expiries (hand cases where the gross formula gives a different answer),
     and is unavailable -- never 0 -- when it should be.
  3. The page's JS gives the same ratio as Python on real snapshots, for all
     buckets and for each bucket alone, and one case is recomputed by a
     third, independent route.
  4. Unit conversions reproduce the server's dollar figures exactly; the
     trend converts each point with its OWN spot and refuses a bad one.
  5. The server serves web/gexmath.js at /gexmath.js and nothing else new.

Usage:  python3 tests/test_gex_ratio.py [DATA_ROOT]
Default DATA_ROOT is the local VM backup. Needs node and git.
"""
import importlib.util, json, math, shutil, subprocess, sys, tempfile, threading, urllib.request
from collections import defaultdict
from http.server import ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import atexit
_made = []
atexit.register(lambda: [shutil.rmtree(d, ignore_errors=True) for d in _made])

import pyarrow.parquet as pq
import gex.server as server
from gex.aggregate import gex_ratio, net_exposure

DEFAULT_ROOT = ROOT / "data/vm_backup/gex_data"
BEFORE_COMMIT = "05b4ed6"      # net_exposure() before the raw units were added
BUCKETS = ["0DTE", "1D", "2-7D", "8-30D", "30D+"]
# Since Python 3.12, sum() over floats is compensated (Neumaier), so the SAME
# code on the SAME rows differs in the last bits between the collector's 3.10
# (where the metadata was computed) and a 3.12 Mac: measured ~1e-15 relative
# on every stored day. Exact against stored metadata only under < 3.12; the
# new-vs-old comparison runs both in this interpreter and is always exact.
SAME_SUM_AS_COLLECTOR = sys.version_info < (3, 12)
JSMATH = ROOT / "web/gexmath.js"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def close(a, b, rel=1e-12):
    return a is not None and b is not None and abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def tmpdir():
    d = tempfile.mkdtemp(prefix="gex_test_ratio_")
    _made.append(d)
    return Path(d)


def load_before():
    src = subprocess.run(["git", "show", f"{BEFORE_COMMIT}:gex_tool/gex/aggregate.py"],
                         cwd=HERE, capture_output=True, text=True, check=True).stdout
    f = tmpdir() / "aggregate_before.py"
    f.write_text(src)
    spec = importlib.util.spec_from_file_location("aggregate_before", f)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["aggregate_before"] = mod        # @dataclass looks its module up here
    spec.loader.exec_module(mod)
    return mod.net_exposure


# The JS side: requires the SAME file the page loads, runs every case, prints JSON.
NODE_DRIVER = r"""
const G = require(process.argv[2]);
const inp = JSON.parse(require("fs").readFileSync(0, "utf8"));
const out = {cases: inp.cases.map(c => G.gexRatio(c.rows, c.buckets)), snaps: []};
for (const s of inp.snaps) {
  const r = {ratio: {}, gexUsd: G.gexIn(s.net_gamma_raw, s.spot, "usd"),
             dexUsd: G.dexIn(s.net_delta_shares, s.spot, "usd"),
             gexRaw: G.gexIn(s.net_gamma_raw, s.spot, "raw"),
             trendRaw: G.trendGex({gex: s.meta_gex, spot: s.spot}, "raw"),
             trendUsd: G.trendGex({gex: s.meta_gex, spot: s.spot}, "usd")};
  r.ratio.all = G.gexRatio(s.rows, s.buckets_all);
  for (const b of s.buckets_all) r.ratio[b] = G.gexRatio(s.rows, [b]);
  out.snaps.push(r);
}
out.trend = [G.trendGex({gex: 1e9, spot: null}, "raw"), G.trendGex({gex: 1e9, spot: 0}, "raw"),
             G.trendGex({gex: 1e9, spot: -5}, "raw"), G.trendGex({gex: null, spot: 600}, "raw"),
             G.trendGex({gex: 1e9, spot: NaN}, "raw"), G.trendGex({gex: 1e9, spot: null}, "usd"),
             G.trendGex({gex: 6e9, spot: 600}, "raw")];
out.gexIn = [G.gexIn(5, null, "usd"), G.gexIn(5, 0, "usd"), G.gexIn(5, null, "raw"),
             G.dexIn(5, null, "usd"), G.dexIn(null, 600, "raw"), G.gexIn(NaN, 600, "raw")];
out.fmt = [G.fmt(-9.223e9, "usd", 3), G.fmt(2.0123e6, "raw"), G.fmt(17.663e9, "usd"),
           G.fmt(-512.5, "raw", 1), G.fmt(null, "usd"), G.fmt(NaN, "raw"), G.fmt(0, "usd"),
           G.fmtRatio(1/3), G.fmtRatio(null), G.fmtRatio(NaN)];
out.nanRow = G.gexRatio([{strike: 1, bucket: "0DTE", call_gamma_oi: NaN, put_gamma_oi: 0}], ["0DTE"]);
console.log(JSON.stringify(out));
"""


def run_node(payload):
    drv = tmpdir() / "driver.js"
    drv.write_text(NODE_DRIVER)
    res = subprocess.run(["node", str(drv), str(JSMATH)], input=json.dumps(payload),
                         capture_output=True, text=True)
    if res.returncode:
        raise SystemExit(f"node failed:\n{res.stderr}")
    return json.loads(res.stdout)


def row(k, bucket, cg=0.0, pg=0.0):
    return {"strike": k, "bucket": bucket, "call_gamma_oi": cg, "put_gamma_oi": pg,
            "call_delta_oi": 0.0, "put_delta_oi": 0.0}


# Hand-worked cases: (name, rows, selected buckets, expected ratio or None).
A, B = "0DTE", "1D"
HAND = [
    # Worked example: +10 (A) and -9 (B) at 100, -2 at 101. Per-strike nets
    # +1 and -2 -> 1/3. The gross formula would give 10/21.
    ("nets per strike across expiries", [row(100, A, cg=10), row(100, B, pg=9), row(101, A, pg=2)],
     [A, B], 1 / 3),
    ("bucket subset A only: +10 and -2 -> 10/12",
     [row(100, A, cg=10), row(100, B, pg=9), row(101, A, pg=2)], [A], 10 / 12),
    ("bucket subset B only: -9 -> 0", [row(100, A, cg=10), row(100, B, pg=9), row(101, A, pg=2)],
     [B], 0.0),
    ("every strike net positive -> 1", [row(100, A, cg=3, pg=1), row(101, B, cg=2)], [A, B], 1.0),
    ("call and put cancel within a strike", [row(100, A, cg=5, pg=5), row(101, A, cg=1)], [A], 1.0),
    ("empty selection -> unavailable", [row(100, A, cg=10)], [], None),
    ("no rows -> unavailable", [], [A, B], None),
    ("zero denominator -> unavailable (not 0, not 0.5)",
     [row(100, A, cg=4, pg=4), row(101, B, cg=2, pg=2)], [A, B], None),
    ("missing gamma in a selected row -> unavailable",
     [row(100, A, cg=10), {**row(101, A), "put_gamma_oi": None}], [A], None),
    ("missing strike -> unavailable", [row(100, A, cg=10), {**row(101, A, pg=1), "strike": None}],
     [A], None),
    ("a bad row in an UNSELECTED bucket does not poison the ratio",
     [row(100, A, cg=10), row(101, A, pg=10), {**row(102, B), "call_gamma_oi": None}], [A], 0.5),
]


def py_ratio(rows, buckets):
    return gex_ratio([r for r in rows if r["bucket"] in set(buckets)])


def independent_ratio(rows):
    """Third route, sharing no code with either implementation."""
    by = defaultdict(list)
    for r in rows:
        by[r["strike"]].append(r["call_gamma_oi"] - r["put_gamma_oi"])
    nets = [math.fsum(v) for v in by.values()]
    return math.fsum(max(n, 0.0) for n in nets) / math.fsum(abs(n) for n in nets)


def sample_polls(reader):
    """Newest poll of every stored day, plus every 40th poll of the newest day."""
    out = []
    days = reader._days()
    for i, day in enumerate(days):
        polls = list(reader.complete(day))
        if not polls:
            continue
        out.append(polls[0])
        if i == 0:
            out.extend(polls[1::40])
    return out


def main():
    data_root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    ok = True
    before = load_before()

    print("2. hand-worked cases, Python")
    for name, rows, sel, want in HAND:
        got = py_ratio(rows, sel)
        ok &= check(name, (got is None and want is None) or close(got, want), f"{got!r}")
    ok &= check("NaN gamma -> unavailable", gex_ratio([row(1, A, cg=float("nan"))]) is None)
    ok &= check("inf gamma -> unavailable", gex_ratio([row(1, A, cg=float("inf"))]) is None)
    ok &= check("bool is not a number", gex_ratio([{**row(1, A), "call_gamma_oi": True}]) is None)

    # Real snapshots.
    reader = server.StoreReader(data_root, "SPY")
    polls = sample_polls(reader)
    if len(polls) < 5:
        raise SystemExit(f"need stored polls under {data_root}; found {len(polls)}")
    snaps, pys = [], []
    n_bits = n_rows = 0
    print(f"1. stored numbers unchanged -- {len(polls)} real polls")
    bits_ok = before_ok = raw_ok = True
    for key, meta_path, agg_path in polls:
        loaded = reader.load(key, meta_path, agg_path)
        if loaded is None:
            continue
        agg, _snap = loaded
        meta = pq.read_table(meta_path).to_pylist()[0]
        new, old = net_exposure(agg), before(agg)
        # Against what the collector stored: bit-for-bit under the collector's
        # summation (see SAME_SUM_AS_COLLECTOR), else within 1e-12 relative.
        # Every pre-existing key against the pre-change function: always ==.
        same = (lambda a, b: a == b) if SAME_SUM_AS_COLLECTOR else close
        bits_ok &= same(new["net_gex"], meta["net_gex_per_pct"]) and same(new["net_dex"], meta["net_dex"])
        before_ok &= all(new[k] == old[k] for k in old)
        S = agg["spot"]
        raw_ok &= (new["net_gamma_raw"] * (S * S * 0.01) == new["net_gex"]
                   and new["net_delta_shares"] * S == new["net_dex"])
        n_bits += 1
        n_rows += len(agg["rows"])
        view = server.build_view(agg, _snap)
        pys.append((key, agg, new, view, meta))
        snaps.append({"rows": view["rows"], "spot": S, "buckets_all": BUCKETS,
                      "net_gamma_raw": new["net_gamma_raw"],
                      "net_delta_shares": new["net_delta_shares"],
                      "meta_gex": meta["net_gex_per_pct"]})
    ok &= check("net GEX / net DEX == the stored metadata, " +
                ("to the bit" if SAME_SUM_AS_COLLECTOR else "within 1e-12 (Python >= 3.12 sums differently)"), bits_ok,
                f"{n_bits} polls, {n_rows:,} rows")
    ok &= check(f"every pre-existing field == net_exposure() at {BEFORE_COMMIT}", before_ok)
    ok &= check("raw gamma x (S*S*0.01) == net GEX and delta shares x S == net DEX, exactly", raw_ok)
    days = sorted({k.split("/")[0] for k, *_ in pys})
    ok &= check("sample spans v1 and v2 partitions",
                "date=2026-09-28" in days and "date=2026-09-29" in days, f"{days[0]} .. {days[-1]}")

    print("no-spot path")
    agg0 = pys[0][1]
    nospot = net_exposure({**agg0, "spot": None})
    ok &= check("dollar figures refused without spot",
                nospot["net_gex"] is None and nospot["net_dex"] is None and nospot["unit"] == "unavailable")
    ok &= check("raw figures and the ratio still computed without spot",
                nospot["net_gamma_raw"] == pys[0][2]["net_gamma_raw"]
                and nospot["net_delta_shares"] == pys[0][2]["net_delta_shares"]
                and nospot["gex_ratio"] == pys[0][2]["gex_ratio"])
    bad_rows = agg0["rows"][:3] + [{**agg0["rows"][3], "put_gamma_oi": None}]
    try:
        r = net_exposure({"spot": None, "rows": bad_rows})
        ok &= check("malformed row without spot: raw unavailable, no exception",
                    r["net_gamma_raw"] is None and r["gex_ratio"] is None)
    except Exception as e:                                    # pragma: no cover
        ok &= check("malformed row without spot: raw unavailable, no exception", False, repr(e))
    raised = []
    for fn in (net_exposure, before):
        try:
            fn({"spot": 600.0, "rows": bad_rows})
        except TypeError:
            raised.append(True)
    ok &= check("malformed row WITH spot still raises, exactly as before", raised == [True, True])

    print("3. the page's JS against Python")
    js = run_node({"cases": [{"rows": r, "buckets": s} for _, r, s, _ in HAND], "snaps": snaps})
    for (name, _r, _s, want), got in zip(HAND, js["cases"]):
        ok &= check(f"JS: {name}", (got is None and want is None) or close(got, want), f"{got!r}")
    ok &= check("JS: NaN gamma -> unavailable", js["nanRow"] is None)
    all_ok = bucket_ok = view_ok = in_range = True
    n_bucket = 0
    for (key, agg, new, view, meta), j in zip(pys, js["snaps"]):
        all_ok &= close(j["ratio"]["all"], new["gex_ratio"])
        view_ok &= view["gex_ratio"] == new["gex_ratio"] and view["net_gamma_raw"] == new["net_gamma_raw"]
        in_range &= 0.0 <= new["gex_ratio"] <= 1.0
        for b in BUCKETS:
            want = py_ratio(view["rows"], [b])
            got = j["ratio"][b]
            bucket_ok &= (got is None and want is None) or close(got, want)
            n_bucket += want is not None
    ok &= check("all buckets: JS ratio == Python gex_ratio()", all_ok, f"{len(pys)} polls")
    ok &= check("each bucket alone: JS == Python", bucket_ok, f"{n_bucket} available cells")
    ok &= check("the header's ratio and raw GEX come from net_exposure()", view_ok)
    ok &= check("every real ratio lies in [0, 1]", in_range)
    key, agg, new, *_ = pys[-1]
    indep = independent_ratio(agg["rows"])
    ok &= check("an independent recomputation agrees", close(indep, new["gex_ratio"], 1e-9),
                f"{key}: {new['gex_ratio']:.6f} vs {indep:.6f}")
    gross = (sum(r["call_gamma_oi"] for r in agg["rows"])
             / sum(r["call_gamma_oi"] + r["put_gamma_oi"] for r in agg["rows"]))
    print(f"        (for scale: gross call/total gamma on that poll is {gross:.6f})")

    print("4. unit conversions and the trend")
    conv_ok = trend_ok = True
    for (key, agg, new, view, meta), j in zip(pys, js["snaps"]):
        conv_ok &= (j["gexUsd"] == new["net_gex"] and j["dexUsd"] == new["net_dex"]
                    and j["gexRaw"] == new["net_gamma_raw"])
        trend_ok &= close(j["trendRaw"], new["net_gamma_raw"]) and j["trendUsd"] == meta["net_gex_per_pct"]
    ok &= check("JS dollar conversion == the server's net GEX / DEX, exactly", conv_ok)
    ok &= check("trend: stored $ GEX / (own spot^2 x 0.01) == raw gamma", trend_ok)
    t = js["trend"]
    ok &= check("trend refuses null / 0 / negative / NaN spot and a null GEX",
                t[:5] == [None] * 5, repr(t[:5]))
    ok &= check("trend in dollars needs no spot", t[5] == 1e9)
    ok &= check("trend raw arithmetic: 6e9 at spot 600 -> 1,666,666.67",
                close(t[6], 6e9 / (600 * 600 * 0.01)), repr(t[6]))
    ok &= check("conversions refuse a missing spot in dollars only, and missing/NaN input",
                js["gexIn"] == [None, None, 5, None, None, None], repr(js["gexIn"]))
    want_fmt = ["−$9.223Bn", "+2.01M", "+$17.66Bn", "−512.5", "—", "—", "+$0.00",
                "0.333", "N/A", "N/A"]
    ok &= check("formatter: signs, $ only in dollars, magnitude suffixes, unavailable",
                js["fmt"] == want_fmt, repr(js["fmt"]))

    print("5. the server serves the shared math file, and only that")
    server.Handler.bundles = {"SPY": server.bundle("SPY", state=server.State())}
    server.Handler.default_symbol = "SPY"
    srv = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        r = urllib.request.urlopen(base + "/gexmath.js", timeout=5)
        ok &= check("/gexmath.js is the file, as JavaScript",
                    r.read() == JSMATH.read_bytes() and r.headers["Content-Type"].startswith("text/javascript"))
        page = urllib.request.urlopen(base + "/", timeout=5).read().decode()
        ok &= check("the page loads it before its own script",
                    page.index('<script src="gexmath.js"></script>') < page.index("<script>\n"))
        for path in ("/web/gexmath.js", "/../gex/server.py", "/gex/server.py", "/gexmath.js/../index.html"):
            try:
                urllib.request.urlopen(base + path, timeout=5)
                ok &= check(f"{path} -> 404", False, "served")
            except urllib.error.HTTPError as e:
                ok &= check(f"{path} -> 404", e.code == 404, str(e.code))
    finally:
        srv.shutdown()

    print()
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
