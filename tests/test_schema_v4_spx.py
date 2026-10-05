"""Schema v4 (the OCC root in the row key) and the raw-every-poll window -- run
against the REAL archived SPX chains before the first SPX poll is stored.

Checked, in order of how badly each could go wrong unnoticed:

  1. On every archived SPX chain: no collisions (~3,096 per poll before v4);
     both roots present; every quote/greek equals the contract it came from;
     per-root OI/volume/exposure equal an independent fsum over the raw chain.
  2. v4 against the pre-v4 aggregator (git V3_COMMIT): summed over roots, every
     summed column of every (strike, expiry) equals what v3 stored (to float
     reassociation); quotes on single-root keys are identical; on two-root keys
     v3 had blanked them and v4 keeps the source values. net_exposure agrees.
     On a real SPY checkpoint v4 == v3 + root 'SPY', EXACTLY (one root, same
     summation order).
  3. Storage: v4 file has exactly AGG_SCHEMA and round-trips every value incl.
     root; a v3 + v4 scan with the explicit schema reads v3 rows as root null;
     metadata keeps n_am_expiring_missing_greeks.
  4. The AM-settlement-day allowance: SPX root expiring today with OI and no
     greeks is a warning (poll usable); the same for SPXW, or for SPX on any
     other day, stays fatal.
  5. Raw-every-poll: the exact bytes and a receipt are kept BEFORE aggregation,
     for unusable payloads too; nothing outside the window; a failing save
     never fails the cycle; fetch() survives JSON that is not an object.

Usage:  python3 tests/test_schema_v4_spx.py [SPY_RAW_CHECKPOINT_DIR]
The pre-v4 aggregator is read from git at V3_COMMIT, or from $GEX_V3_AGGREGATE
(the VM has no git checkout). SPX chains: data/fixtures/spx_chains/.
"""
import gzip, importlib.util, json, math, os, subprocess, sys, tempfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

# Every temp dir this test makes is removed at exit.
import atexit as _atexit, shutil as _shutil, tempfile as _tempfile
_real_mkdtemp, _made = _tempfile.mkdtemp, []
def _mkdtemp(*a, **k):
    d = _real_mkdtemp(*a, **k)
    _made.append(d)
    return d
_tempfile.mkdtemp = _mkdtemp
_atexit.register(lambda: [_shutil.rmtree(d, ignore_errors=True) for d in _made])

from gex.aggregate import aggregate, net_exposure, parse_occ, bucket_for, _num
from gex import store, fetch
import gex.collector as col

import pyarrow.dataset as ds
import pyarrow.parquet as pq

V3_COMMIT = "0fee3ed"          # last commit with the pre-v4 (rootless-key) aggregator
SPX_DIR = HERE.parent / "data/fixtures/spx_chains"
DEFAULT_SPY_RAW = HERE.parent / "data/vm_backup/gex_data/raw/symbol=SPY"
SUMMED = ["call_gamma_oi", "put_gamma_oi", "call_delta_oi", "put_delta_oi",
          "call_oi", "put_oi", "call_volume", "put_volume"]
PER_CONTRACT = ("iv", "bid", "ask", "gamma", "delta")
QUOTE_COLS = [f"{s}_{f}" for s in ("call", "put") for f in PER_CONTRACT]


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def close(a, b, rel=1e-12, abs_=1e-9):
    return math.isclose(a, b, rel_tol=rel, abs_tol=abs_)


def load_v3_aggregate():
    src_path = os.environ.get("GEX_V3_AGGREGATE")
    if src_path:
        src = Path(src_path).read_text()
    else:
        try:
            src = subprocess.run(["git", "show", f"{V3_COMMIT}:gex_tool/gex/aggregate.py"],
                                 cwd=HERE, capture_output=True, text=True, check=True).stdout
        except (OSError, subprocess.CalledProcessError):
            return None
    tmp = Path(tempfile.mkdtemp()) / "aggregate_v3.py"
    tmp.write_text(src)
    spec = importlib.util.spec_from_file_location("aggregate_v3", tmp)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod          # @dataclass looks its module up here
    spec.loader.exec_module(mod)
    return mod


def spx_files():
    return sorted(SPX_DIR.glob("SPX_*.json"))


def file_day(p):
    return date.fromisoformat(p.name.split("_")[1][:10])


def test_spx_chains(v3):
    print("1+2. real archived SPX chains: no collisions, fidelity, per-root sums, v3 equivalence")
    ok = True
    files = spx_files()
    ok &= check("archived SPX chains found", len(files) >= 3, f"{len(files)} in {SPX_DIR}")
    for f in files:
        payload = json.loads(f.read_text())
        today = file_day(f)
        agg = aggregate(payload, today=today)
        rows = agg["rows"]
        by = {(r["strike"], r["expiry"], r["root"]): r for r in rows}
        ok &= check(f"{f.name}: usable, unique (strike, expiry, root) keys",
                    agg["usable"] and len(by) == len(rows), str(agg["problems"]))
        ok &= check(f"{f.name}: no collisions (was ~3,096 before v4)", agg["n_quote_collisions"] == 0)
        ok &= check(f"{f.name}: both roots present", {r["root"] for r in rows} == {"SPX", "SPXW"})

        # Every unexpired contract's quote/greek equals the source, in ITS root's row.
        sums = defaultdict(lambda: defaultdict(list))      # root -> column -> terms
        mism = checked = 0
        for o in payload["data"]["options"]:
            root, exp, cp, k = parse_occ(o["option"])
            if bucket_for(exp, today) == "expired":
                continue
            r = by[(k, exp.isoformat(), root)]
            side = "call_" if cp == "C" else "put_"
            for fld in PER_CONTRACT:
                checked += 1
                if r[side + fld] != _num(o.get(fld)):
                    mism += 1
            oi = o.get("open_interest") or 0.0
            sums[root][side + "volume"].append(o.get("volume") or 0.0)
            if oi:
                sums[root][side + "oi"].append(oi)
                sums[root][side + "gamma_oi"].append((o.get("gamma") or 0.0) * oi * 100.0)
                sums[root][side + "delta_oi"].append((o.get("delta") or 0.0) * oi * 100.0)
        ok &= check(f"{f.name}: every quote/greek equals its source contract",
                    checked > 0 and mism == 0, f"{checked:,} values, {mism} mismatched")
        bad = [(root, c) for root in sums for c in SUMMED
               if not close(sum(r[c] for r in rows if r["root"] == root), math.fsum(sums[root][c]))]
        ok &= check(f"{f.name}: per-root OI/volume/exposure = independent fsum", not bad, str(bad[:4]))

        if v3 is None:
            continue
        old = v3.aggregate(payload, today=today)
        old_by = {(r["strike"], r["expiry"]): r for r in old["rows"]}
        new_by = defaultdict(list)
        for r in rows:
            new_by[(r["strike"], r["expiry"])].append(r)
        ok &= check(f"{f.name}: same (strike, expiry) set as v3", set(old_by) == set(new_by))
        sum_bad = [(key, c) for key, rs in new_by.items() for c in SUMMED
                   if not close(sum(r[c] for r in rs), old_by[key][c])]
        ok &= check(f"{f.name}: summed over roots == v3 for all 8 summed columns", not sum_bad,
                    str(sum_bad[:3]))
        one = [key for key, rs in new_by.items() if len(rs) == 1]
        two = [key for key, rs in new_by.items() if len(rs) == 2]
        ok &= check(f"{f.name}: single-root keys: quotes identical to v3",
                    all(new_by[key][0][c] == old_by[key][c] for key in one for c in QUOTE_COLS),
                    f"{len(one):,} keys")
        ok &= check(f"{f.name}: two-root keys: v3 blanked, v4 keeps both roots' values",
                    len(two) > 0 and all(old_by[key]["call_iv"] is None and old_by[key]["put_iv"] is None
                                         for key in two)
                    and sum(1 for key in two for r in new_by[key] if r["call_iv"] is not None) > len(two),
                    f"{len(two):,} keys")
        n_new, n_old = net_exposure(agg), v3.net_exposure(old)
        ok &= check(f"{f.name}: net GEX / DEX equal v3", close(n_new["net_gex"], n_old["net_gex"])
                    and close(n_new["net_dex"], n_old["net_dex"]))
    return ok


def test_spy_unchanged(v3, raw_dir):
    print("2b. a real SPY checkpoint: v4 == v3 + root 'SPY', exactly")
    files = sorted(raw_dir.glob("*.json.gz"))
    if v3 is None or not files:
        return check("pre-v4 aggregator and an SPY checkpoint available", False,
                     "no git/$GEX_V3_AGGREGATE" if v3 is None else f"none in {raw_dir}")
    ok = True
    for f in files[-2:]:
        payload = json.load(gzip.open(f))
        today = date.fromisoformat(f.name.split("_")[1][:10])
        new, old = aggregate(payload, today=today), v3.aggregate(payload, today=today)
        ok &= check(f"{f.name}: every row == v3 row + root 'SPY' (exact ==)",
                    len(new["rows"]) == len(old["rows"]) and all(
                        n == {**o, "root": "SPY"} for n, o in zip(new["rows"], old["rows"])),
                    f"{len(new['rows']):,} rows")
        ok &= check(f"{f.name}: net exposure bit-identical to v3",
                    net_exposure(new) == v3.net_exposure(old))
    return ok


def _fake_snap(symbol, when, body=b"{}", status=200, warnings=None):
    try:
        payload = json.loads(body) if status == 200 else None
    except ValueError:
        payload = None
    return SimpleNamespace(symbol=symbol, received_at=when, snapshot_time=None, newest_trade_time=None,
                           payload=payload,
                           fetch_seconds=0.5, http_status=status, raw_bytes=len(body),
                           fingerprint="fp", http_date="d", http_age="3", retry_after=None,
                           warnings=list(warnings or []), raw_body=body)


def test_storage():
    print("3. storage: v4 schema, round-trip incl. root, v3+v4 scan, metadata field")
    ok = True
    f = spx_files()[-1]
    payload = json.loads(f.read_text())
    agg = aggregate(payload, today=file_day(f))
    root = Path(tempfile.mkdtemp())
    snap = _fake_snap("_SPX", datetime(2026, 8, 26, 13, 35, 8, 123456, tzinfo=timezone.utc))
    p4 = store.append_aggregate(root, snap, agg)
    ok &= check("partition is symbol=SPX", "symbol=SPX" in str(p4))
    t = pq.read_table(p4)
    ok &= check("v4 file has exactly AGG_SCHEMA", t.schema.equals(store.AGG_SCHEMA))
    back = t.to_pylist()
    ok &= check("schema_version 4 on every row", {r["schema_version"] for r in back} == {4})
    fields = ["strike", "expiry", "root", "bucket"] + SUMMED + QUOTE_COLS
    ok &= check("every value round-trips, root included",
                len(back) == len(agg["rows"]) and all(
                    all(b[c] == a[c] for c in fields) for b, a in zip(back, agg["rows"])),
                f"{len(back):,} rows")
    # A v3-shaped file (no root column) next to it, read with the explicit schema.
    v3_rows = [{c: v for c, v in r.items() if c != "root"} for r in back[:5]]
    p3 = root / "agg_v3.parquet"
    import pyarrow as pa
    pq.write_table(pa.Table.from_pylist(
        [{**r, "schema_version": 3} for r in v3_rows],
        schema=pa.schema([fld for fld in store.AGG_SCHEMA if fld.name != "root"])), p3)
    both = ds.dataset([str(p3), str(p4)], schema=store.AGG_SCHEMA).to_table().to_pylist()
    ok &= check("v3+v4 scan with AGG_SCHEMA: v3 rows read root null, v4 rows keep theirs",
                [r["root"] for r in both[:5]] == [None] * 5
                and {r["root"] for r in both[5:]} == {"SPX", "SPXW"})
    m = store.append_metadata(root, snap, {**agg, "n_am_expiring_missing_greeks": 7},
                              net_exposure(agg))
    ok &= check("metadata keeps n_am_expiring_missing_greeks",
                pq.read_table(m).to_pylist()[0]["n_am_expiring_missing_greeks"] == 7)
    return ok


def _c(sym, **kw):
    base = {"option": sym, "open_interest": 10.0, "volume": 1.0,
            "gamma": 0.01, "delta": 0.5, "iv": 0.2, "bid": 1.0, "ask": 1.1}
    base.update(kw)
    return base


def _chain(extra):
    filler = [_c(f"SPXW261218C{(9000 + i) * 1000:08d}") for i in range(2100)]
    return {"data": {"current_price": 6700.0, "options": filler + extra}}


def test_am_allowance():
    print("4. AM-settlement-day missing greeks: warning for SPX expiring today only")
    ok = True
    fri = date(2026, 10, 16)
    agg = aggregate(_chain([_c("SPX261016C06700000", gamma=None, delta=None),
                            _c("SPX261016P06700000", gamma=None, delta=None)]), today=fri)
    r = next(r for r in agg["rows"] if r["root"] == "SPX" and r["strike"] == 6700.0)
    ok &= check("SPX expiring today, OI without greeks: poll stays usable",
                agg["usable"], str(agg["problems"]))
    ok &= check("... counted as a warning", agg["n_am_expiring_missing_greeks"] == 2)
    ok &= check("... per-contract greeks stored null, weighted exposure 0, OI kept",
                r["call_gamma"] is None and r["put_delta"] is None
                and r["call_gamma_oi"] == 0.0 and r["call_oi"] == 10.0)
    for kw, what in (({"gamma": None}, "gamma"), ({"delta": None}, "delta")):
        agg = aggregate(_chain([_c("SPX261016C06700000", **kw)]), today=fri)
        r = next(r for r in agg["rows"] if r["root"] == "SPX")
        ok &= check(f"only {what} missing: usable, warned, BOTH weighted exposures 0, the other greek stored",
                    agg["usable"] and agg["n_am_expiring_missing_greeks"] == 1
                    and r["call_gamma_oi"] == 0.0 and r["call_delta_oi"] == 0.0 and r["call_oi"] == 10.0
                    and (r["call_delta"] == 0.5 if what == "gamma" else r["call_gamma"] == 0.01))
    agg = aggregate(_chain([_c("SPXW261016C06700000", gamma=None)]), today=fri)
    ok &= check("SPXW (PM) expiring today without greeks: still fatal",
                not agg["usable"] and agg["n_am_expiring_missing_greeks"] == 0)
    agg = aggregate(_chain([_c("SPX261120C06700000", gamma=None)]), today=fri)
    ok &= check("SPX expiring on ANOTHER day without greeks: still fatal", not agg["usable"])
    agg = aggregate(_chain([_c("SPX261016C06700000", gamma=None)]), today=date(2026, 10, 15))
    ok &= check("SPX the day BEFORE its expiry without greeks: still fatal", not agg["usable"])
    return ok


def _collector(root, client, raw_until):
    c = col.Collector.__new__(col.Collector)           # no signal handlers
    c.symbol, c.root, c.interval, c.idle_interval, c.always = "_SPX", Path(root), 60.0, 900.0, False
    c._stop, c._fail_streak, c._bad_streak, c._was_active, c._calendar_warned = False, 0, 0, True, None
    c._http_refusal, c._retry_at, c._bad_since, c._stall_alerted = False, None, None, False
    c.raw_until, c._raw_window_closed_logged = raw_until, False
    c._now = lambda: 1_000_000.0
    c._in_session = lambda: True
    c._stall_was_in, c._last_ok = True, 1_000_000.0
    c.client = client
    c.logs = []
    c.log = lambda msg, **kw: c.logs.append(msg)
    return c


def test_raw_window():
    print("5. raw-every-poll window")
    ok = True
    root = Path(tempfile.mkdtemp())
    when = datetime(2026, 10, 16, 14, 0, 0, 654321, tzinfo=timezone.utc)
    body = json.dumps({"data": {"current_price": 6700.0, "options": [_c("SPX261016C06700000")]}}).encode()
    snap = _fake_snap("_SPX", when, body=body, warnings=["w1"])
    p = store.save_raw_poll(root, snap, date(2026, 10, 16))
    side = p.with_name(p.name.replace(".json.gz", ".json"))
    rec = json.loads(side.read_text())
    ok &= check("body gunzips to the EXACT bytes received", gzip.decompress(p.read_bytes()) == body)
    ok &= check("receipt: microsecond receipt time, session date, provenance",
                rec["received_at"] == when.isoformat() and rec["session_date"] == "2026-10-16"
                and rec["http_status"] == 200 and rec["fetch_warnings"] == ["w1"] and rec["body"] == p.name)
    ok &= check("layout raw_polls/symbol=SPX/date=2026-10-16, no .tmp_ leftovers",
                p.parent == root / "raw_polls/symbol=SPX/date=2026-10-16"
                and not list(p.parent.glob(".tmp_*")))

    # Through the REAL cycle: a payload aggregate() rejects (5-contract chain) is
    # still kept raw, while nothing reaches metadata/aggregates.
    class Client:
        def __init__(self, snaps): self.snaps = snaps
        def fetch(self): return self.snaps.pop(0)
        def mark_stored(self, fp): pass
    r2 = Path(tempfile.mkdtemp())
    c = _collector(r2, Client([_fake_snap("_SPX", when, body=body)]), date(2026, 10, 23))
    stored = c.cycle()
    ok &= check("unusable payload: raw kept, nothing stored, rejected as usual",
                not stored and len(list((r2 / "raw_polls").rglob("*.json.gz"))) == 1
                and not (r2 / "aggregates").exists() and c._bad_streak == 1)
    c = _collector(r2, Client([_fake_snap("_SPX", when, body=b"<html>403</html>", status=403)]),
                   date(2026, 10, 23))
    c.cycle()
    ok &= check("an HTTP 403 body is kept too, with its status",
                any(json.loads(s.read_text())["http_status"] == 403
                    for s in (r2 / "raw_polls").rglob("*.json")))
    r4 = Path(tempfile.mkdtemp())
    c = _collector(r4, Client([_fake_snap("_SPX", when, body=body)]), date(2026, 10, 16))
    c.cycle()
    ok &= check("the window's LAST session date is inside it (inclusive)",
                len(list((r4 / "raw_polls").rglob("*.json.gz"))) == 1)
    late = datetime(2026, 10, 17, 0, 30, tzinfo=timezone.utc)     # 20:30 ET on the 16th
    c = _collector(r4, Client([_fake_snap("_SPX", late, body=body)]), date(2026, 10, 16))
    c.cycle()
    ok &= check("... judged by the New York session date, not the UTC date",
                len(list((r4 / "raw_polls").rglob("*.json.gz"))) == 2)
    r3 = Path(tempfile.mkdtemp())
    c = _collector(r3, Client([_fake_snap("_SPX", when, body=body)]), date(2026, 10, 15))
    c.cycle()
    ok &= check("outside the window: nothing kept, logged once",
                not (r3 / "raw_polls").exists() and c.logs.count("raw-every-poll window ended") == 1)
    c = _collector(r3, Client([_fake_snap("_SPX", when, body=body)]), None)
    c.cycle()
    ok &= check("no window (SPY's unit): nothing kept", not (r3 / "raw_polls").exists())
    blocked = Path(tempfile.mkdtemp()) / "file_not_dir"
    blocked.write_text("x")
    c = _collector(blocked, Client([_fake_snap("_SPX", when, body=body)]), date(2026, 10, 23))
    c.cycle()
    ok &= check("a failing raw save is logged and the cycle carries on to aggregation",
                "raw poll save FAILED" in c.logs and c._bad_streak == 1 and c._fail_streak == 0)

    # fetch() on valid JSON that is not an object: no exception, body kept.
    class Resp:
        def __init__(self, content): self.content, self.status_code, self.headers = content, 200, {}
    for raw, why in ((b"[1, 2, 3]", "a JSON list"), (b'{"data": null}', "data null")):
        cl = fetch.CboeClient("_SPX")
        cl._session = SimpleNamespace(get=lambda url, timeout: Resp(raw))
        try:
            s = cl.fetch()
            good = s.payload == {} and s.raw_body == raw and s.warnings and s.newest_trade_time is None
        except Exception as e:
            good, s = False, e
        ok &= check(f"fetch() on {why}: empty payload + warning, raw bytes kept", good)
    return ok


def main():
    spy_raw = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SPY_RAW
    v3 = load_v3_aggregate()
    if v3 is None:
        print("  NOTE  pre-v4 aggregator unavailable (no git, no $GEX_V3_AGGREGATE): v3 comparisons FAIL")
    ok = test_spx_chains(v3)
    ok &= test_spy_unchanged(v3, spy_raw)
    ok &= test_storage()
    ok &= test_am_allowance()
    ok &= test_raw_window()
    ok &= check("pre-v4 aggregator was available", v3 is not None)
    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
