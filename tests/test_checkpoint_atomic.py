"""The daily raw checkpoint is published atomically, and a crash mid-write
neither leaves a truncated checkpoint nor suppresses the day's retry.

Before 2026-09-29 `save_raw` wrote straight to the final name (the "rename" in
daily_checkpoint was a no-op), so both of those failure modes were live.

Usage:  python3 tests/test_checkpoint_atomic.py
"""
import gzip, json, sys, tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Every temp dir this test makes is removed at exit.
import atexit as _atexit, shutil as _shutil, tempfile as _tempfile
_real_mkdtemp, _made = _tempfile.mkdtemp, []
def _mkdtemp(*a, **k):
    d = _real_mkdtemp(*a, **k)
    _made.append(d)
    return d
_tempfile.mkdtemp = _mkdtemp
_atexit.register(lambda: [_shutil.rmtree(d, ignore_errors=True) for d in _made])
import gex.fetch as fetch
from gex import store


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def _snap(minute=0):
    payload = {"timestamp": "2026-09-29 14:00:00",
               "data": {"current_price": 500.0,
                        "options": [{"option": f"SPY261218C{i:08d}", "iv": 0.2}
                                    for i in range(5000)]}}
    return SimpleNamespace(symbol="SPY", payload=payload,
                           received_at=datetime(2026, 9, 29, 14, minute,
                                                tzinfo=timezone.utc))


def main():
    ok = True
    root = Path(tempfile.mkdtemp())
    raw = root / "raw" / "symbol=SPY"

    print("normal write")
    snap = _snap()
    p = store.daily_checkpoint(root, snap)
    ok &= check("checkpoint written under its final name",
                p is not None and p.exists() and p.name.startswith("SPY_2026-09-29"))
    ok &= check("content round-trips", p is not None and p.exists()
                and json.load(gzip.open(p)) == snap.payload)
    ok &= check("no .tmp_ file left behind", not list(raw.glob(".tmp_*")))
    ok &= check("second call the same day is a no-op",
                store.daily_checkpoint(root, _snap(5)) is None)

    print("crash mid-write")
    root2 = Path(tempfile.mkdtemp())
    raw2 = root2 / "raw" / "symbol=SPY"
    real_json = fetch.json

    def partial_then_crash(obj, fh):
        fh.write('{"timestamp": "2026-09-29 14:00:00", "data": {"opt')
        raise OSError("simulated crash mid-write")

    fetch.json = SimpleNamespace(dump=partial_then_crash, loads=real_json.loads)
    raised = False
    try:
        store.daily_checkpoint(root2, _snap())
    except OSError:
        raised = True
    finally:
        fetch.json = real_json
    ok &= check("the failure propagates (collector logs it)", raised)
    ok &= check("no checkpoint under the final name",
                not [f for f in raw2.iterdir() if not f.name.startswith(".")])
    ok &= check("no .tmp_ file left behind", not list(raw2.glob(".tmp_*")))
    p2 = store.daily_checkpoint(root2, _snap(1))
    ok &= check("the day's retry is NOT suppressed",
                p2 is not None and json.load(gzip.open(p2)) == _snap(1).payload)

    print("a stray .tmp_ file does not count as today's checkpoint")
    root3 = Path(tempfile.mkdtemp())
    raw3 = root3 / "raw" / "symbol=SPY"
    raw3.mkdir(parents=True)
    (raw3 / ".tmp_abc.json.gz").write_bytes(b"\x1f\x8b truncated")
    p3 = store.daily_checkpoint(root3, _snap())
    ok &= check("checkpoint still written", p3 is not None and p3.exists())

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
