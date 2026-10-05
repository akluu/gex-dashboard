"""Schema v2 (per-side IV and bid/ask) and v3 (per-contract gamma and delta)
-- run against the REAL daily checkpoints.

Four things are checked, in order of how badly they could go wrong unnoticed:

  1. The v1 columns are EXACTLY EQUAL (Python ==) to the pre-v2 aggregator on every real
     checkpoint (the project's reproduce-before-trusting rule). A quote change
     must not move a single exposure number.
  2. Every quote the aggregator emits equals the contract it came from -- including the
     zero-open-interest wings, which the exposure path skips.
  3. Source fidelity at the edges: explicit 0.0 stays 0.0, an absent key is
     None, and a duplicate contract blanks that side instead of keeping either.
  4. The v1/v2 file boundary reads correctly with an explicit schema, and the
     pitfall the store docstring warns about is real.

Usage:  python3 tests/test_schema_v2.py [RAW_CHECKPOINT_DIR]
Default dir is the local VM backup. Check 1 needs the pre-v2 aggregator; it
is read from git at the commit below, or from $GEX_V1_AGGREGATE if set (the
VM has no git checkout).
"""
import gzip, importlib.util, json, os, subprocess, sys, tempfile
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
from gex.aggregate import aggregate, parse_occ, bucket_for
from gex import store

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

V1_COMMIT = "2d0f3ee"          # last commit with the v1 aggregator
V1_COLS = ["strike", "expiry", "bucket", "call_gamma_oi", "put_gamma_oi",
           "call_delta_oi", "put_delta_oi", "call_oi", "put_oi",
           "call_volume", "put_volume"]
QUOTE_COLS = ["call_iv", "put_iv", "call_bid", "call_ask", "put_bid", "put_ask",
              # schema v3: per-contract greeks, same assignment rules
              "call_gamma", "put_gamma", "call_delta", "put_delta"]
PER_CONTRACT = ("iv", "bid", "ask", "gamma", "delta")
DEFAULT_RAW = HERE.parent / "data/vm_backup/gex_data/raw/symbol=SPY"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def load_v1_aggregate():
    src_path = os.environ.get("GEX_V1_AGGREGATE")
    if src_path:
        src = Path(src_path).read_text()
    else:
        try:
            src = subprocess.run(
                ["git", "show", f"{V1_COMMIT}:gex_tool/gex/aggregate.py"],
                cwd=HERE, capture_output=True, text=True, check=True).stdout
        except (OSError, subprocess.CalledProcessError):
            return None
    tmp = Path(tempfile.mkdtemp()) / "aggregate_v1.py"
    tmp.write_text(src)
    spec = importlib.util.spec_from_file_location("aggregate_v1", tmp)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["aggregate_v1"] = mod     # @dataclass looks its module up here
    spec.loader.exec_module(mod)
    return mod.aggregate


def checkpoint_date(path: Path) -> date:
    # SPY_2026-09-28T13-36-40Z.json.gz -> the date the poll was received
    return date.fromisoformat(path.name.split("_")[1][:10])


def test_v1_equivalence(files, agg_v1):
    print("1. v1 columns exactly equal (==) to the pre-v2 aggregator")
    if agg_v1 is None:
        return check("pre-v2 aggregator available", False,
                     "no git and no $GEX_V1_AGGREGATE -- cannot verify")
    ok = True
    total_rows = 0
    for f in files:
        payload = json.load(gzip.open(f))
        today = checkpoint_date(f)
        old, new = agg_v1(payload, today=today), aggregate(payload, today=today)
        same_meta = all(old[k] == new[k] for k in
                        ("spot", "n_contracts", "n_rows", "n_unparseable",
                         "problems", "usable", "session_date"))
        same_rows = (len(old["rows"]) == len(new["rows"]) and all(
            {k: a[k] for k in V1_COLS} == {k: b[k] for k in V1_COLS}
            for a, b in zip(old["rows"], new["rows"])))
        total_rows += len(new["rows"])
        ok &= check(f"{f.name}", same_meta and same_rows,
                    f"{len(new['rows'])} rows" + ("" if same_rows else "  ROWS DIFFER"))
    ok &= check("all checkpoints compared", len(files) > 0,
                f"{len(files)} files, {total_rows:,} rows")
    return ok


def test_quote_fidelity(files):
    print("2. every aggregated quote equals its source contract")
    ok = True
    for f in files[-3:]:
        payload = json.load(gzip.open(f))
        today = checkpoint_date(f)
        src = {}
        for o in payload["data"]["options"]:
            p = parse_occ(o["option"])
            if p and bucket_for(p[1], today) != "expired":
                src[(p[3], p[1].isoformat(), p[2])] = o
        agg = aggregate(payload, today=today)
        checked = mismatched = zero_oi_quoted = zero_oi_gamma = 0
        for r in agg["rows"]:
            for side, pfx in (("C", "call_"), ("P", "put_")):
                o = src.get((r["strike"], r["expiry"], side))
                if o is None:
                    ok &= check("no quote on a side with no contract",
                                all(r[pfx + k] is None for k in PER_CONTRACT))
                    continue
                checked += 1
                if tuple(r[pfx + k] for k in PER_CONTRACT) != tuple(
                        float(o[k]) for k in PER_CONTRACT):
                    mismatched += 1
                if not (o.get("open_interest") or 0) and (o.get("bid") or 0):
                    zero_oi_quoted += 1
                if not (o.get("open_interest") or 0) and (o.get("gamma") or 0):
                    zero_oi_gamma += 1
        ok &= check(f"{f.name}: zero-OI contracts keep their gamma (v3)", zero_oi_gamma > 0,
                    f"{zero_oi_gamma:,} zero-OI sides with gamma")
        ok &= check(f"{f.name}: quotes + greeks match source", checked > 0 and mismatched == 0,
                    f"{checked:,} sides checked, {mismatched} mismatched")
        ok &= check(f"{f.name}: zero-OI wings still quoted", zero_oi_quoted > 0,
                    f"{zero_oi_quoted:,} zero-OI sides with a bid")
        ok &= check(f"{f.name}: no collisions on real SPY data",
                    agg["n_quote_collisions"] == 0)
    return ok


def _contract(sym, **kw):
    base = {"option": sym, "open_interest": 10.0, "volume": 1.0,
            "gamma": 0.01, "delta": 0.5, "iv": 0.2, "bid": 1.0, "ask": 1.1}
    base.update(kw)
    return base


def _synthetic(extra):
    # Enough filler contracts to pass MIN_PLAUSIBLE_CONTRACTS and the greek
    # coverage guard, on strikes far away from the ones under test.
    filler = [_contract(f"SPY261218C{(1000 + i) * 1000:08d}") for i in range(2100)]
    return {"data": {"current_price": 500.0, "options": filler + extra}}


def test_edges():
    print("3. source fidelity at the edges")
    ok = True
    today = date(2026, 9, 29)
    k = lambda rows, strike: next(r for r in rows if r["strike"] == strike)

    agg = aggregate(_synthetic([
        _contract("SPY261218C00400000", iv=0.0, bid=0.0, ask=0.05),
        _contract("SPY261218P00400000"),
    ]), today=today)
    r = k(agg["rows"], 400.0)
    ok &= check("explicit 0.0 IV stored as 0.0, not None", r["call_iv"] == 0.0)
    ok &= check("explicit 0.0 bid stored as 0.0", r["call_bid"] == 0.0)

    c = _contract("SPY261218C00410000"); del c["iv"]; c["bid"] = None
    agg = aggregate(_synthetic([c]), today=today)
    r = k(agg["rows"], 410.0)
    ok &= check("absent key and null both stored as None",
                r["call_iv"] is None and r["call_bid"] is None)
    ok &= check("present field alongside them kept", r["call_ask"] == 1.1)
    ok &= check("side with no contract is None", r["put_iv"] is None)

    agg = aggregate(_synthetic([
        _contract("SPY261218C00420000", iv=0.21, open_interest=0.0),
    ]), today=today)
    r = k(agg["rows"], 420.0)
    ok &= check("zero-OI contract still quoted", r["call_iv"] == 0.21)
    ok &= check("zero-OI contract keeps its gamma and delta (v3)",
                r["call_gamma"] == 0.01 and r["call_delta"] == 0.5)
    ok &= check("... while its OI-weighted exposure is 0, as before",
                r["call_gamma_oi"] == 0.0)
    agg = aggregate(_synthetic([
        _contract("SPY261218C00440000", gamma=0.0, delta=None),
    ]), today=today)
    r = k(agg["rows"], 440.0)
    ok &= check("explicit 0.0 gamma stays 0.0; null delta stays None",
                r["call_gamma"] == 0.0 and r["call_delta"] is None)

    # Two roots on one (strike, expiry, side): an adjusted root after a
    # corporate action (or SPX vs SPXW). Before schema v4 this was a collision
    # (quotes blanked, exposure summed); from v4 the root is part of the key,
    # so each root keeps its own row and its own quotes, and a reader summing
    # over roots gets the same exposure v1-v3 stored.
    agg = aggregate(_synthetic([
        _contract("SPY261218C00430000", iv=0.30),
        _contract("SPY1261218C00430000", iv=0.99, open_interest=5.0),
        _contract("SPY261218P00430000", iv=0.25),
    ]), today=today)
    rs = {r["root"]: r for r in agg["rows"] if r["strike"] == 430.0}
    ok &= check("v4: two roots are two rows, no collision",
                set(rs) == {"SPY", "SPY1"} and agg["n_quote_collisions"] == 0)
    ok &= check("v4: each root keeps its own quotes",
                rs["SPY"]["call_iv"] == 0.30 and rs["SPY1"]["call_iv"] == 0.99
                and rs["SPY"]["put_iv"] == 0.25 and rs["SPY1"]["put_iv"] is None)
    ok &= check("v4: exposure summed over roots = what v1-v3 stored for the key",
                rs["SPY"]["call_oi"] + rs["SPY1"]["call_oi"] == 15.0)
    ok &= check("snapshot still usable", agg["usable"], str(agg["problems"]))

    # A TRUE collision since v4: CBOE repeating the same OCC symbol. Quotes are
    # ambiguous -> blanked; exposure sums both (likely double-counted, flagged).
    agg = aggregate(_synthetic([
        _contract("SPY261218C00430000", iv=0.30),
        _contract("SPY261218C00430000", iv=0.99, open_interest=5.0),
        _contract("SPY261218P00430000", iv=0.25),
    ]), today=today)
    r = k(agg["rows"], 430.0)
    ok &= check("collision counted", agg["n_quote_collisions"] == 1)
    ok &= check("colliding side blanked (quotes AND greeks)",
                all(r["call_" + f] is None for f in PER_CONTRACT))
    ok &= check("other side untouched", r["put_iv"] == 0.25)
    ok &= check("exposure still sums both records", r["call_oi"] == 15.0)
    ok &= check("snapshot still usable", agg["usable"], str(agg["problems"]))
    return ok


def test_storage(files):
    print("4. storage: v1/v2 boundary, explicit schema, size")
    ok = True
    payload = json.load(gzip.open(files[-1]))
    today = checkpoint_date(files[-1])
    agg = aggregate(payload, today=today)
    root = Path(tempfile.mkdtemp())
    snap = SimpleNamespace(symbol="SPY",
                           received_at=datetime(2026, 9, 29, 14, 0, tzinfo=timezone.utc))
    v2 = store.append_aggregate(root, snap, agg)
    t2 = pq.read_table(v2)
    ok &= check("v2 file has exactly AGG_SCHEMA", t2.schema.equals(store.AGG_SCHEMA))
    ok &= check(f"schema_version column = {store.SCHEMA_VERSION}",
                set(t2.column("schema_version").to_pylist()) == {store.SCHEMA_VERSION})
    # Values, not just presence: a swapped or dropped mapping in
    # append_aggregate would otherwise pass, because the explicit schema
    # silently supplies any missing field as null.
    want = {c: [r[c] for r in agg["rows"]] for c in V1_COLS + QUOTE_COLS}
    ok &= check("direct read: every v1 and quote column round-trips",
                all(t2.column(c).to_pylist() == want[c] for c in want),
                ", ".join(c for c in want if t2.column(c).to_pylist() != want[c]))

    # A v1 file exactly as the deployed collector wrote it: inferred schema,
    # v1 columns only.
    v1_rows = [{"received_at": "2026-09-28T14:00:00+00:00",
                **{c: r[c] for c in V1_COLS}} for r in agg["rows"]]
    v1 = root / "agg_v1.parquet"
    pq.write_table(pa.Table.from_pylist(v1_rows), v1, compression="zstd")
    ok &= check("v1 columns of AGG_SCHEMA match the inferred v1 types",
                pq.read_schema(v1).equals(pa.schema(
                    [store.AGG_SCHEMA.field(n) for n in pq.read_schema(v1).names])))

    both = ds.dataset([str(v1), str(v2)], schema=store.AGG_SCHEMA).to_table()
    n1 = len(v1_rows)
    sv = both.column("schema_version").to_pylist()
    ok &= check("explicit schema: v1 rows read with null quotes",
                sv.count(None) == n1 and both.column("call_iv").to_pylist()[:n1].count(None) == n1)
    ok &= check("explicit schema: v2 rows keep their quotes",
                sv.count(store.SCHEMA_VERSION) == len(agg["rows"]) and all(
                    both.column(c).to_pylist()[n1:] == want[c] for c in QUOTE_COLS))
    ok &= check("explicit schema: v1 rows' v1 columns intact",
                all(both.column(c).to_pylist()[:n1] == want[c] for c in V1_COLS))
    naive = ds.dataset([str(v1), str(v2)]).to_table()
    ok &= check("PITFALL is real: v1-first inferred scan drops v2 columns",
                "call_iv" not in naive.column_names)
    promoted = pa.concat_tables([pq.read_table(v1), pq.read_table(v2)],
                                promote_options="default")
    ok &= check("per-file read + promote keeps v2 columns",
                promoted.num_rows == 2 * n1 and all(
                    promoted.column(c).to_pylist()[n1:] == want[c] for c in QUOTE_COLS))

    # A poll where a whole quote column is null (calls only here). Without
    # the explicit schema pyarrow would infer a `null`-typed put_iv, and the
    # schema would drift file to file -- real SPY data never shows this, so
    # only a synthetic chain can catch the explicit schema being dropped.
    calls_only = aggregate(_synthetic([]), today=date(2026, 9, 29))
    v2b = store.append_aggregate(root, SimpleNamespace(
        symbol="SPY", received_at=datetime(2026, 9, 29, 14, 1, tzinfo=timezone.utc)),
        calls_only)
    t2b = pq.read_table(v2b)
    ok &= check("all-null case: quote values round-trip (puts null, calls kept)",
                all(t2b.column(c).to_pylist() == [r[c] for r in calls_only["rows"]]
                    for c in QUOTE_COLS)
                and set(t2b.column("put_iv").to_pylist()) == {None}
                and None not in t2b.column("call_iv").to_pylist())
    ok &= check("all-null quote column still typed float64",
                pq.read_schema(v2b).equals(store.AGG_SCHEMA),
                str(pq.read_schema(v2b).field("put_iv").type))

    s1, s2 = v1.stat().st_size, v2.stat().st_size
    ok &= check("size measured", s2 > s1,
                f"v1 {s1/1024:.0f} KB -> v2 {s2/1024:.0f} KB per poll "
                f"(+{(s2 - s1)/1024:.0f} KB, x{s2/s1:.2f})")
    return ok


def main():
    raw_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_RAW
    files = sorted(raw_dir.glob("*.json.gz"))
    if not files:
        raise SystemExit(f"no checkpoints in {raw_dir}")
    ok = True
    ok &= test_v1_equivalence(files, load_v1_aggregate())
    ok &= test_quote_fidelity(files)
    ok &= test_edges()
    ok &= test_storage(files)
    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
