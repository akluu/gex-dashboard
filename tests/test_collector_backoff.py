"""Collector backoff, unusable-payload grace, stall alert and stored-fingerprint
dedupe (gex/collector.py, gex/fetch.py): the 2026-10-01 change.

That morning ~65 minutes of the session were never recorded. CBOE's daily
greeks-zeroed handover (three unusable payloads) shared the backoff streak
with real timeouts, so the timeouts that followed waited 8 and then 16
minutes. Fix: unusable payloads get a 15-minute grace at the
normal cadence and then back off like a failure; a source that did not answer
backs off with a 4 x cap in session; an HTTP error response keeps the 16 x cap
and its Retry-After; "unchanged" is judged against the last STORED body; an
ALERT line marks 10 minutes without a usable result.

Checked:
  1. next_delay()/retry_after_seconds(): every cap, Retry-After floor and
     ceiling, and the outside-session formula identical to the old one.
  2. 2026-10-01 replayed through the real cycle() with a simulated clock that
     advances by the delay run() would sleep: unusable payloads wait 60 s;
     the timeouts after them wait 2, 4, 4 min (were 8, 16, 16).
  3. A lasting unusable source: normal cadence for 15 minutes, then backoff.
  4. Every failure kind backs off; HTTP refusals with the 16 x cap and their
     Retry-After; 401/403 logged as ACCESS DENIED.
  5. Stall alert: one ALERT after 10 minutes without a usable result, one
     "recovered" line, none while healthy.
  6. Real CboeClient (fake HTTP session): "unchanged" only against a body
     marked stored; a body whose storage failed is stored on the next
     identical response, not skipped.

Usage:  python3 tests/test_collector_backoff.py
"""
import io, json, sys
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import gex.collector as col
import gex.fetch as fetch


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    return bool(cond)


def old_delay(active, s, interval, idle, to_open):
    """The pre-change formula, verbatim, for the outside-session comparison."""
    base = interval if active else idle
    delay = base * min(2 ** s, 16) if s else base
    if not active:
        delay = min(delay, max(5.0, to_open))
    else:
        delay = min(delay, interval * 16)
    return delay


def test_delay_rules():
    print("1. next_delay / retry_after_seconds")
    ok = True
    nd = col.next_delay
    ok &= check("in session, no answer: 60, 120, 240, then capped at 240",
                [nd(True, s, 60.0, 900.0) for s in range(7)] == [60, 120, 240, 240, 240, 240, 240])
    ok &= check("in session, HTTP refusal: the old 16 x cap (960 s)",
                [nd(True, s, 60.0, 900.0, http_refusal=True) for s in range(7)] == [60, 120, 240, 480, 960, 960, 960])
    same = all(nd(False, s, 60.0, 900.0, t) == old_delay(False, s, 60.0, 900.0, t)
               for s in range(10) for t in (1.0, 4.0, 300.0, 3600.0, 50000.0, 2e5))
    ok &= check("outside the session: identical to the old formula (16 x, never past the open)", same)
    now = datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)
    ra = col.retry_after_seconds
    ok &= check("Retry-After: seconds, an HTTP date, garbage, missing",
                ra("120", now) == 120.0 and abs(ra(format_datetime(now + timedelta(seconds=90), usegmt=True), now) - 90) < 1
                and ra("soon", now) is None and ra(None, now) is None and ra("-5", now) == 0.0)
    return ok


class FakeClient:
    def __init__(self, events):
        self.events = list(events)

    def mark_stored(self, fp):
        pass

    def fetch(self):
        kind = self.events.pop(0)
        if kind == "timeout":
            raise TimeoutError("Read timed out. (read timeout=20)")
        if kind == "conn":
            raise ConnectionError("connection refused")
        status = {"http503": 503, "http429": 429, "http403": 403}.get(kind, 200)
        payload = None if kind == "empty" or status != 200 else {"kind": kind}
        return SimpleNamespace(http_status=status, payload=payload, retry_after="600" if kind == "http429" else None,
                               warnings=[fetch.UNCHANGED] if kind == "unchanged" else [],
                               fingerprint="f", snapshot_time="t", fetch_seconds=0.1, newest_trade_time=None,
                               received_at=datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc))


def fake_env(store_fail=0):
    """Patch aggregate/net/store so a cycle touches nothing real. store_fail:
    how many append_aggregate calls raise before they succeed."""
    calls = {"aggregate": 0, "fail_left": store_fail}

    def append_aggregate(*a):
        calls["aggregate"] += 1
        if calls["fail_left"] > 0:
            calls["fail_left"] -= 1
            raise OSError("disk full")

    col.aggregate = lambda p: (_ for _ in ()).throw(RuntimeError("boom")) if p.get("kind") == "explode" else \
        {"usable": p.get("kind") != "unusable", "problems": ["greeks not populated"] if p.get("kind") == "unusable" else [],
         "spot": 1.0, "n_rows": 1, "n_contracts": 1}
    col.net_exposure = lambda agg, per_pct=True: {"net_gex": 0.0, "net_dex": 0.0}
    col.store = SimpleNamespace(append_metadata=lambda *a: None, append_aggregate=append_aggregate,
                                daily_checkpoint=lambda *a: None)
    return calls


def collector(client):
    c = col.Collector.__new__(col.Collector)           # no signal handlers, no real client
    c.symbol, c.root, c.interval, c.idle_interval, c.always = "SPY", Path("/nonexistent"), 60.0, 900.0, False
    c._stop, c._fail_streak, c._bad_streak, c._was_active, c._calendar_warned = False, 0, 0, True, None
    c._http_refusal, c._retry_at, c._bad_since, c._stall_alerted = False, None, None, False
    c.raw_until, c._raw_window_closed_logged = None, False      # v4 raw window off
    clock = [1_000_000.0]
    c._now = lambda: clock[0]
    c._in_session = lambda: True
    c._stall_was_in = True
    c._last_ok = clock[0]
    c.client = client
    return c, clock


def replay(events):
    """As run() does in session: cycle, check for a stall, sleep _delay(True).
    Returns [(event, fail streak, bad streak, wait s)] and the parsed log."""
    c, clock = collector(FakeClient(events))
    out, buf = [], io.StringIO()
    with redirect_stdout(buf):
        for e in events:
            c.cycle()
            c._check_stall()
            d = c._delay(True)
            out.append((e, c._fail_streak, c._bad_streak, d))
            clock[0] += d
    return out, [json.loads(l) for l in buf.getvalue().splitlines()]


def test_replay():
    print("2. 2026-10-01 replayed through the real cycle()")
    ok = True
    fake_env()
    seq = ["timeout", "timeout", "ok", "ok", "unchanged", "ok", "unusable", "unusable", "unusable",
           "timeout", "timeout", "timeout", "ok"]
    out, log = replay(seq)
    waits = [d for *_, d in out]
    ok &= check("unusable payloads (inside the grace): transport streak 0, wait 60 s",
                all(f == 0 and d == 60 for e, f, _, d in out if e == "unusable"))
    ok &= check("... their own streak 1, 2, 3 in the log, not backing off",
                [(r["bad_payload_streak"], r["backing_off"]) for r in log if r["msg"].startswith("payload not usable")]
                == [(1, False), (2, False), (3, False)])
    ok &= check("the timeouts after them wait 2, 4, 4 min (were 8, 16, 16)", waits[9:12] == [120, 240, 240], f"{waits[9:12]}")
    ok &= check("a usable result resets both streaks", out[-1][1:3] == (0, 0) and out[2][1:3] == (0, 0))
    print(f"     (waits: {[int(w) for w in waits]})")
    return ok


def test_lasting_unusable():
    print("3. a lasting unusable source: 15 min at the normal cadence, then backoff")
    ok = True
    fake_env()
    out, log = replay(["unusable"] * 24 + ["unchanged"])
    recs = [r for r in log if r["msg"].startswith("payload not usable")]
    first_off = next(i for i, r in enumerate(recs) if r["backing_off"])
    ok &= check("no backoff while bad for <= 900 s, then it starts",
                all(r["bad_for_s"] <= 900 for r in recs[:first_off]) and recs[first_off]["bad_for_s"] > 900
                and all(d == 60 for *_, d in out[:first_off]), f"backs off from poll {first_off + 1}")
    ok &= check("after the grace: 120, 240, 240 ... (the in-session cap)",
                [d for *_, d in out[first_off:first_off + 4]] == [120, 240, 240, 240])
    ok &= check("bad streak keeps counting; an unchanged USABLE poll ends it",
                recs[-1]["bad_payload_streak"] == 24 and out[-1][1:3] == (0, 0))
    return ok


def test_failure_kinds():
    print("4. failure kinds")
    ok = True
    fake_env()
    for kind in ("timeout", "conn", "empty", "explode"):
        out, _ = replay([kind] * 4)
        ok &= check(f"{kind}: no answer -> 120, 240, 240, 240", [d for *_, d in out] == [120, 240, 240, 240])
    out, log = replay(["http503"] * 5)
    ok &= check("HTTP 503: refusal keeps the 16 x cap -> 120, 240, 480, 960, 960", [d for *_, d in out] == [120, 240, 480, 960, 960])
    out, log = replay(["http429"])
    ok &= check("HTTP 429 with Retry-After: 600 -> a no-request deadline 600 s ahead (run() honours it, section 7)",
                log[0]["retry_after_s"] == 600.0 and log[0]["no_request_until"])
    out, log = replay(["http403"])
    ok &= check("HTTP 403: logged as ACCESS DENIED", log[0]["msg"] == "ACCESS DENIED by the source")
    out, _ = replay(["timeout", "unusable", "timeout"])
    ok &= check("a timeout after an answered (unusable) poll starts a fresh streak", [f for _, f, _, _ in out] == [1, 0, 1])
    out, _ = replay(["http503", "http503", "timeout"])
    ok &= check("a timeout after refusals is a no-answer failure again (4 x cap)", out[-1][3] == 240 and out[-1][1] == 3)
    return ok


def test_stall_alert():
    print("5. stall alert")
    ok = True
    fake_env()
    out, log = replay(["timeout"] * 7 + ["ok", "ok"])
    msgs = [r["msg"] for r in log]
    ok &= check("one ALERT after 10 minutes without a usable result, one 'recovered'",
                msgs.count("ALERT: no usable snapshot in session") == 1 and msgs.count("recovered from stall") == 1)
    al = next(r for r in log if r["msg"].startswith("ALERT"))
    ok &= check("the ALERT reports >= 10 minutes", al["minutes"] >= 10, f"{al['minutes']} min")
    out, log = replay(["ok"] * 12 + ["unchanged"] * 12)
    ok &= check("no alert while healthy (stored or unchanged)", not any(r["msg"].startswith("ALERT") for r in log))
    return ok


class FakeResp:
    def __init__(self, body: bytes, status=200, headers=None):
        self.content, self.status_code, self.headers = body, status, headers or {}


class FakeSession:
    def __init__(self, resps):
        self.resps = list(resps)

    def get(self, url, timeout=None):
        return self.resps.pop(0)


def test_fingerprint():
    print("6. 'unchanged' is judged against the last STORED body")
    ok = True
    body = json.dumps({"timestamp": "2026-10-01 14:00:00", "data": {"options": [{"option": "x"}]}}).encode()
    cl = fetch.CboeClient("SPY")
    cl._session = FakeSession([FakeResp(body), FakeResp(body), FakeResp(body), FakeResp(b"", 429, {"Retry-After": "120"})])
    a = cl.fetch(); b = cl.fetch()
    ok &= check("identical body, never stored: NOT unchanged", fetch.UNCHANGED not in b.warnings)
    cl.mark_stored(b.fingerprint)
    c = cl.fetch()
    ok &= check("identical body after mark_stored: unchanged", fetch.UNCHANGED in c.warnings)
    d = cl.fetch()
    ok &= check("Retry-After header kept on the snapshot", d.http_status == 429 and d.retry_after == "120")
    # Through the collector: the first store fails, the identical next body is stored, the third is unchanged.
    calls = fake_env(store_fail=1)
    col.aggregate = lambda p: {"usable": True, "problems": [], "spot": 1.0, "n_rows": 1, "n_contracts": 1}
    cl = fetch.CboeClient("SPY")
    cl._session = FakeSession([FakeResp(body)] * 3)
    cc, clock = collector(cl)
    buf = io.StringIO()
    with redirect_stdout(buf):
        res = [cc.cycle() for _ in range(3)]
    msgs = [json.loads(l)["msg"] for l in buf.getvalue().splitlines()]
    ok &= check("storage fails -> the identical next body is stored, not skipped; then unchanged",
                res == [False, True, False] and msgs == ["cycle failed", "stored", "unchanged, not stored"]
                and calls["aggregate"] == 2, f"{msgs}")
    return ok


class RunClient(FakeClient):
    """FakeClient that records the (simulated) time of every request and stops
    the collector once its events are used up."""
    def __init__(self, events, clock, coll_ref, retry_after="600"):
        super().__init__(events)
        self.clock, self.coll_ref, self.times, self.ra = clock, coll_ref, [], retry_after

    def fetch(self):
        self.times.append(self.clock[0])
        if len(self.events) == 1:
            self.coll_ref[0]._stop = True
        snap = super().fetch()
        if getattr(snap, "http_status", 200) == 429:
            snap.retry_after = self.ra
        return snap


def run_sim(events, session=lambda t: True, retry_after="600", start=1_000_000.0, to_open=None):
    """The REAL run() loop on a simulated clock: time.sleep advances it, the
    session is session(t). Returns request times (s after start) and the log."""
    fake_env()
    clock = [start]
    ref = [None]
    c, _ = collector(None)
    c._now = lambda: clock[0]
    c._in_session = lambda: session(clock[0])
    c._was_active, c._last_ok, c._stall_was_in = False, None, False
    c.client = RunClient(events, clock, ref, retry_after)
    ref[0] = c
    saved = (col.time.sleep, col.closed_reason, col.seconds_to_next_open)
    col.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
    col.closed_reason = lambda: None if session(clock[0]) else "after close"
    col.seconds_to_next_open = (lambda: to_open(clock[0])) if to_open else \
        (lambda: next((k for k in range(1, 200000, 1) if session(clock[0] + k)), 900.0))
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            c.run()
    finally:
        col.time.sleep, col.closed_reason, col.seconds_to_next_open = saved
    return [t - start for t in c.client.times], [json.loads(l) for l in buf.getvalue().splitlines()]


def test_run_loop():
    print("7. the real run() loop: Retry-After deadline, stall alerts while waiting")
    ok = True
    t, log = run_sim(["http429", "ok"], retry_after="600")
    ok &= check("Retry-After 600 s: no request until 600 s later", t == [0, 600], f"{t}")
    t, log = run_sim(["http429", "ok"], retry_after=str(24 * 3600))
    ok &= check("Retry-After 24 h is honoured, not shortened to an hour", t == [0, 86400], f"{t}")
    msgs = [r["msg"] for r in log]
    ok &= check("... and the outage raises ONE alert while waiting, then 'recovered'",
                msgs.count("ALERT: no usable snapshot in session") == 1 and msgs.count("recovered from stall") == 1)
    al = next(r for r in log if r["msg"].startswith("ALERT"))
    ok &= check("... on time (10-10.5 min in), not after the wait", 10 <= al["minutes"] <= 10.5, f"{al['minutes']} min")
    # A 2 h Retry-After received in session; the session closes at 30 min and reopens at 60 min,
    # inside the deadline. The close/open bookkeeping must not clear or shorten it.
    sess = lambda x: not (1_000_000.0 + 1800 <= x < 1_000_000.0 + 3600)
    t, log = run_sim(["http429", "ok"], session=sess, retry_after=str(2 * 3600))
    ok &= check("a Retry-After deadline survives a close and a re-open (not shortened to the open)",
                t == [0, 7200], f"{t}")
    t, log = run_sim(["timeout"] * 8 + ["ok"])
    ok &= check("timeouts in run(): requests 120 s, then 240 s apart",
                [b - a for a, b in zip(t, t[1:])][:4] == [120, 240, 240, 240], f"{[b - a for a, b in zip(t, t[1:])][:4]}")
    return ok


def test_overnight():
    print("9. overnight: the stall clock restarts at the open")
    ok = True
    S, DAY, OPEN_LEN = 1_000_000.0, 86400.0, 6.5 * 3600
    sess = lambda x: ((x - S) % DAY) < OPEN_LEN
    nxt = lambda x: max(1.0, DAY - ((x - S) % DAY)) if not sess(x) else 0.0
    late = S + OPEN_LEN - 30                     # the first poll 30 s before the close
    t, log = run_sim(["ok", "ok"], session=sess, to_open=nxt, start=late)
    msgs = [r["msg"] for r in log]
    ok &= check("usable poll, overnight close, usable poll at the next open: no alert, no recovery",
                t[-1] == DAY - OPEN_LEN + 30 and not any(m.startswith("ALERT") or m.startswith("recovered") for m in msgs),
                f"second poll {t[-1] / 3600:.2f} h later")
    t, log = run_sim(["ok"] + ["timeout"] * 8 + ["ok"], session=sess, to_open=nxt, start=late)
    ok &= check("... the failures really are in the next session", t[1] == DAY - OPEN_LEN + 30, f"{t[1] / 3600:.2f} h")
    al = [r for r in log if r["msg"].startswith("ALERT")]
    ok &= check("failures from the next open: the first alert is ~10 min into the NEW session",
                len(al) == 1 and 10 <= al[0]["minutes"] <= 10.5, f"{[a['minutes'] for a in al]}")
    return ok


def test_checkpoint_retry():
    print("8. daily checkpoint: separate from availability, retried on unchanged polls")
    ok = True
    body = json.dumps({"timestamp": "2026-10-01 14:00:00", "data": {"options": [{"option": "x"}]}}).encode()
    fake_env()
    col.aggregate = lambda p: {"usable": True, "problems": [], "spot": 1.0, "n_rows": 1, "n_contracts": 1}
    tries = {"n": 0}
    def ckpt(*a):
        tries["n"] += 1
        if tries["n"] == 1:
            raise OSError("disk full")
        return Path("/x") if tries["n"] == 2 else None          # written once, then a no-op
    col.store.daily_checkpoint = ckpt
    cl = fetch.CboeClient("SPY")
    cl._session = FakeSession([FakeResp(body)] * 3)
    cc, clock = collector(cl)
    buf = io.StringIO()
    with redirect_stdout(buf):
        res = [cc.cycle() for _ in range(3)]
    log = [json.loads(l) for l in buf.getvalue().splitlines()]
    msgs = [r["msg"] for r in log]
    ok &= check("a failed checkpoint does not fail the cycle or feed the backoff",
                res[0] is True and msgs[0].startswith("daily checkpoint FAILED") and msgs[1] == "stored"
                and cc._fail_streak == 0)
    ok &= check("the identical next body is 'unchanged' (the snapshot WAS stored) and retries the checkpoint",
                msgs[2] == "unchanged, not stored" and tries["n"] == 3)
    return ok


def main():
    ok = (test_delay_rules() & test_replay() & test_lasting_unusable() & test_failure_kinds()
          & test_stall_alert() & test_fingerprint() & test_run_loop() & test_checkpoint_retry() & test_overnight())
    print()
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
