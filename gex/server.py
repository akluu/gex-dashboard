"""Local dashboard server: reads the COLLECTOR's stored snapshots and pushes
them to the browser. **It never contacts CBOE.**

Until 2026-09-29 this polled CBOE itself, so running it alongside the systemd
collector hit the source TWICE -- against terms that prohibit auto-extraction
and name IP blocking as the remedy. Now the collector is the only thing that
fetches; this process only reads `<data-root>/metadata` + `aggregates`, so it
costs nothing against the source and shows exactly what was stored. The price
is up to `--check-every` seconds of extra lag on top of the collector's
60 s poll and CBOE's ~16 min delay.

The collector writes metadata BEFORE the aggregate, each atomically, so the
newest metadata file can briefly have no aggregate yet. The reader only takes
a poll whose pair is complete, and otherwise keeps showing the previous one.

WHY SSE RATHER THAN WEBSOCKET: the payload is one-way and small enough to send
whole (~1.6 MB of JSON per SPY snapshot, ~2.5 MB for SPX, once a minute;
measured at v1 it was ~75 KB for ~800 strikes). A delta protocol
would add reconciliation complexity to save bytes that are not the bottleneck
-- the network fetch from CBOE is ~770 ms of a ~900 ms cycle.

BINDS 127.0.0.1 ONLY. Cboe's terms prohibit redistributing their delayed data
externally; this is reached over an SSH tunnel, never exposed.
"""
from __future__ import annotations

import argparse
import json
import math
import threading
import time
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs

from .aggregate import _NY, net_exposure, session_date
from .collector import RTH_CLOSE, in_session, market_session
from .market_calendar import AM_SETTLED_ROOTS, options_close, settlement_time
from . import gammatracks, heatmap, ivterm, oiwalls

try:
    import pyarrow.parquet as pq
except ImportError:                                   # pragma: no cover
    pq = None

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


class State:
    """Newest snapshot, guarded. Written by one poller, read by many handlers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._payload: dict | None = None
        self._version = 0

    def set(self, obj: dict) -> None:
        with self._lock:
            self._payload = obj
            self._version += 1

    def get(self) -> tuple[dict | None, int]:
        with self._lock:
            return self._payload, self._version


# The root whose contracts expire every session (the 0DTE series) per symbol.
EXPIRING_ROOT = {"SPX": "SPXW"}


def build_view(agg: dict, snap=None, source: str = "collector store") -> dict:
    """What the browser receives. Everything needed to render AND to judge
    whether the render should be trusted."""
    net = net_exposure(agg, per_pct=True)
    now = datetime.now(timezone.utc)

    # The honest staleness fields. `snapshot_age` is publication age, NOT data
    # age -- the archive shows the payload reading ~45s fresh while the newest
    # trade was the previous session's close. Both are sent; the UI must label
    # them differently and must never present snapshot_age as "data age".
    newest_trade = getattr(snap, "newest_trade_time", None)
    snap_time = getattr(snap, "snapshot_time", None)
    # CBOE's last_trade_time is naive EASTERN (measured 2026-09-04: a 09:30:06
    # print arrived as "...T09:30:06"). The page used to parse it as UTC and
    # showed it ~4-5 h off; send an unambiguous UTC copy alongside the raw one.
    newest_trade_utc = None
    if newest_trade:
        try:
            newest_trade_utc = (datetime.fromisoformat(str(newest_trade))
                                .replace(tzinfo=_NY).astimezone(timezone.utc).isoformat())
        except ValueError:
            pass
    # When options trading ends for the displayed session (scheduled close,
    # early closes included). The page compares it with the CLOCK: receipt
    # times cannot tell a stalled collector from a closed market.
    close_utc = None
    try:
        d = datetime.fromisoformat(str(agg.get("session_date"))).date()
        close_utc = (datetime.combine(d, options_close(d, RTH_CLOSE)).replace(tzinfo=_NY)
                     .astimezone(timezone.utc).isoformat())
    except (TypeError, ValueError):
        pass
    # When THIS session's expiring (0DTE) series settles: for SPX that is SPXW
    # at 16:00 (13:00 early), 15 min before the 16:15 session close above; for
    # SPY it equals options_close. The page's post-close 0DTE note uses it.
    expiring_utc = None
    try:
        d = datetime.fromisoformat(str(agg.get("session_date"))).date()
        pm_root = EXPIRING_ROOT.get(agg.get("symbol"), agg.get("symbol"))
        expiring_utc = (datetime.combine(d, settlement_time(pm_root, d, RTH_CLOSE)).replace(tzinfo=_NY)
                        .astimezone(timezone.utc).isoformat())
    except (TypeError, ValueError):
        pass
    return {
        "generated_at": now.isoformat(),
        "source": source,
        "session_date": agg.get("session_date"),
        "options_close_utc": close_utc,
        "expiring_settles_utc": expiring_utc,
        "spot": agg.get("spot"),
        "net_gex_per_pct": net.get("net_gex"),
        "net_dex": net.get("net_dex"),
        "unit": net.get("unit"),
        # Raw units (GEXStream's published ones) and the GEX Ratio, over the
        # same scope as the two above: ALL expiries, all strikes. None when
        # unavailable; none of them needs spot.
        "net_gamma_raw": net.get("net_gamma_raw"),
        "net_delta_shares": net.get("net_delta_shares"),
        "gex_ratio": net.get("gex_ratio"),
        "symbol": agg.get("symbol"),
        "n_contracts": agg.get("n_contracts"),
        "n_rows": agg.get("n_rows"),
        "n_excluded_settled": agg.get("n_excluded_settled", 0),
        "usable": agg.get("usable", False),
        "problems": agg.get("problems", []),
        "feed": {
            "snapshot_time": snap_time.isoformat() if snap_time else None,
            # Age at PUSH time only -- one push per poll, so the page must
            # recompute ages from the timestamps every second, not show this.
            "snapshot_age_s": (now - snap_time).total_seconds() if snap_time else None,
            "newest_trade_time": newest_trade,         # raw, naive Eastern
            "newest_trade_utc": newest_trade_utc,
            # When the collector received it (clock 3). The browser can be at
            # most --check-every seconds behind this.
            "received_at": getattr(snap, "received_at", None),
            "fetch_seconds": getattr(snap, "fetch_seconds", None),
            "http_status": getattr(snap, "http_status", None),
            # Stated, not computed: we have not measured the real delay yet.
            "delay_note": "CBOE delayed feed - exact quote age unavailable",
        },
        "rows": agg.get("rows", []),
    }


# The fields the page reads today. The quote columns (schema v2) are left out
# on purpose: they add ~45% to a ~1.6 MB push and nothing draws them yet --
# the IV-smile panel will add them when it exists.
VIEW_COLS = ("strike", "expiry", "bucket", "call_gamma_oi", "put_gamma_oi",
             "call_delta_oi", "put_delta_oi", "call_oi", "put_oi",
             "call_volume", "put_volume")


def eligible(row: dict, session: str | None) -> bool:
    """THE display-eligibility rule, applied in exactly one place (_read_rows):
    an AM-settled root (SPX) on its own expiration date has settled at the
    open, so it is not a live position for any panel that day. The collector
    still STORES those rows (capture is lossless; the rule is a read-time
    policy, 2026-10-03). Rows without a root (SPY files before schema
    v4) are always eligible."""
    return not (session and row.get("root") in AM_SETTLED_ROOTS and row.get("expiry") == session)


NET_COLS = ("strike", "call_gamma_oi", "put_gamma_oi", "call_delta_oi", "put_delta_oi")


# Symbols whose store predates schema v4: a file without a `root` column is
# legitimate (every row is that symbol's one root). For any other symbol a
# missing root would silently bypass the AM filter, so it is an error.
LEGACY_ROOTLESS = frozenset({"SPY"})


def poll_session(key: str) -> str | None:
    """The New York session date (ISO) of a poll key
    'date=YYYY-MM-DD/HHMMSS_ffffff.parquet' -- the partition date is the UTC
    receipt date, so derive the session from the receipt instant rather than
    assume the two coincide (they do for every 09:30-17:00 ET poll)."""
    try:
        day, stamp = key.split("/", 1)
        t = datetime.strptime(day[len("date="):] + stamp[:13], "%Y-%m-%d%H%M%S_%f")
        return t.replace(tzinfo=timezone.utc).astimezone(_NY).date().isoformat()
    except (ValueError, IndexError, AttributeError):
        return None


def _read_rows(path, cols, key: str, symbol: str, stats: dict | None = None) -> list[dict]:
    """Every panel's aggregate read. Reads the `cols` the file HAS (v1-v3
    files have no `root`, v1 no quotes; asking pyarrow for an absent column
    raises) plus `root`/`expiry` internally, and returns each row with exactly
    `cols`, absent ones as None. Rows failing eligible() are dropped, judged
    against THIS poll's own session (poll_session), so a past settlement day
    renders the same way whenever it is viewed. `stats["excluded_settled"]`
    receives the number dropped, so a view can say what it left out."""
    names = set(pq.ParquetFile(path).schema_arrow.names)
    if "root" not in names and symbol not in LEGACY_ROOTLESS:
        raise ValueError(f"{symbol} aggregate has no root column -- cannot apply the settlement filter")
    need = [c for c in cols if c in names]
    need += [c for c in ("root", "expiry") if c in names and c not in need]
    rows = pq.read_table(path, columns=need).to_pylist()
    excluded = 0
    if "root" in names and "expiry" in names:
        session = poll_session(key)
        kept = [r for r in rows if eligible(r, session)]
        excluded, rows = len(rows) - len(kept), kept
    if stats is not None:
        stats["excluded_settled"] = excluded
    if list(cols) != need:
        rows = [{c: r.get(c) for c in cols} for r in rows]
    if "root" in cols and "root" not in names:
        # A pre-v4 file of a single-root symbol: every row IS that root, so
        # keys built from (expiry, root) match across the v3/v4 boundary.
        for r in rows:
            r["root"] = symbol
    return rows


def age_seconds(t, newest_trade) -> float | None:
    """Receipt time minus the snapshot's newest option trade (CBOE's naive
    Eastern stamp), in seconds; None if either is missing or unparseable. The
    age of the newest trade -- not of every quote, greek or the spot."""
    if not t or not newest_trade:
        return None
    try:
        return (datetime.fromisoformat(str(t)) -
                datetime.fromisoformat(str(newest_trade)).replace(tzinfo=_NY)).total_seconds()
    except (TypeError, ValueError):
        return None


def median_lag_seconds(points, day: str) -> float | None:
    """Median of (receipt - newest option trade) over the LIVE points of a
    session (newest trade dated that session): how old the data is when it
    arrives, measured rather than assumed (~16 min, audit 2026-09-30). The
    newest trade is CBOE's naive Eastern stamp. None when no live point."""
    lags = []
    for p in points:
        if str(p.get("newest_trade") or "")[:10] != day:
            continue
        lag = age_seconds(p.get("t"), p.get("newest_trade"))
        if lag is not None and 0 <= lag < 6 * 3600:
            lags.append(lag)
    if not lags:
        return None
    lags.sort()
    m = len(lags) // 2
    return lags[m] if len(lags) % 2 else (lags[m - 1] + lags[m]) / 2


class StoreReader:
    """Newest complete (metadata + aggregate) poll the collector has written.

    Looks in today's partition first, then falls back to the most recent day
    that has one -- so before the open, on weekends and on holidays the page
    shows the last session's final snapshot, labelled by its own timestamps,
    rather than nothing.
    """

    def __init__(self, root: Path, symbol: str) -> None:
        self.root = Path(root)
        self.symbol = norm_symbol(symbol)
        self.meta_dir = Path(root) / "metadata" / f"symbol={symbol.lstrip('_')}"
        self.agg_dir = Path(root) / "aggregates" / f"symbol={symbol.lstrip('_')}"
        # Polls found unreadable or unusable. Written by store_loop only,
        # read by HistoryCache too, so the trend never includes a poll the
        # snapshot panels refused.
        self.rejected: set[str] = set()

    def _days(self) -> list[str]:
        if not self.meta_dir.exists():
            return []
        return sorted((p.name for p in self.meta_dir.iterdir()
                       if p.name.startswith("date=")), reverse=True)

    def _quarantined(self, day: str) -> set[str]:
        """On the VM quarantined polls are MOVED away; the append-only Mac
        backup keeps the originals too, so exclude them explicitly."""
        q = self.root / "quarantine" / day[len("date="):]
        return {f.name.split("_", 1)[1] for f in q.glob("*.parquet")} if q.exists() else set()

    def complete(self, day: str):
        """(key, metadata path, aggregate path) for every complete,
        non-quarantined poll of a `date=...` partition, newest first."""
        bad = self._quarantined(day)
        metas = sorted((self.meta_dir / day).glob("meta_*.parquet"), reverse=True)
        for m in metas:
            stamp = m.name[len("meta_"):]
            a = self.agg_dir / day / f"agg_{stamp}"
            if a.exists() and stamp not in bad:
                yield f"{day}/{stamp}", m, a

    def newest(self, skip=frozenset()) -> tuple[str, Path, Path] | None:
        """The newest complete poll whose key is not in `skip` (polls already
        found unreadable or unusable), searching back across days."""
        for day in self._days():
            for found in self.complete(day):
                if found[0] not in skip:
                    return found
        return None

    def load(self, day_key: str, meta_path: Path, agg_path: Path) -> tuple[dict, object] | None:
        """(agg, snap-like) in the shapes build_view expects, or None if the
        stored poll is not one to show."""
        meta = pq.read_table(meta_path).to_pylist()[0]
        if not meta.get("usable"):
            return None
        excl: dict = {}
        rows = _read_rows(agg_path, VIEW_COLS, day_key, self.symbol, excl)
        try:
            problems = json.loads(meta.get("problems") or "[]")
        except ValueError:
            problems = ["unreadable problems field in stored metadata"]
        agg = {
            "symbol": self.symbol,
            "spot": meta.get("spot"),
            "session_date": poll_session(day_key) or day_key.split("/")[0][len("date="):],
            "n_contracts": meta.get("n_contracts"),      # STORED chain size
            "n_rows": len(rows),                         # rows this view shows
            # AM-settled rows the view left out on their settlement day.
            "n_excluded_settled": excl.get("excluded_settled", 0),
            "usable": True,
            # The collector only stores usable polls; any notes it kept are
            # passed through so the page can show them.
            "problems": problems,
            "rows": rows,
        }
        snap_time = meta.get("snapshot_time")
        snap = SimpleNamespace(
            newest_trade_time=meta.get("newest_trade_time"),
            snapshot_time=datetime.fromisoformat(snap_time) if snap_time else None,
            received_at=meta.get("received_at"),
            fetch_seconds=meta.get("fetch_seconds"),
            http_status=meta.get("http_status"),
        )
        return agg, snap


def store_loop(state: State, reader: StoreReader, check_every: float,
               stop: threading.Event) -> None:
    """Push the newest stored poll whenever a new one lands. Reads local files
    only; a failed read keeps the last good view on screen."""
    shown = None           # key currently on screen
    while not stop.is_set():
        found = None
        try:
            found = reader.newest(skip=reader.rejected)
            if found and found[0] != shown:
                loaded = reader.load(*found)
                if loaded is None:
                    raise ValueError("stored poll is marked unusable")
                view = build_view(*loaded)
                # Which stored poll this is: HistoryCache anchors the trend to
                # it, and the page matches the trend to it.
                view["poll_key"] = found[0]
                state.set(view)
                shown = found[0]
        except Exception as e:
            # Each bad poll is logged ONCE and then skipped for good, and the
            # search immediately falls back to the next-newest readable poll --
            # so a bad newest file neither spams the log every check nor
            # leaves a freshly started server with nothing to show.
            if found:
                reader.rejected.add(found[0])
            print(json.dumps({"msg": "stored poll rejected, falling back",
                              "poll": found[0] if found else None,
                              "error": f"{type(e).__name__}: {e}"}), flush=True)
            if found:
                continue
        stop.wait(check_every)


class HistoryCache:
    """Intraday series ANCHORED to the poll the snapshot panels are showing.

    Reads the collector's metadata layer (never CBOE). The series is the day
    of the displayed poll, up to and including it, complete (meta + aggregate)
    polls only, minus any poll the snapshot reader rejected -- so the trend can
    never show a different day, or run ahead of, the profiles above it. (A
    first version picked its own "newest" poll and could show today's trend
    under yesterday's profiles when today's aggregate was unreadable.)

    Recomputed only when the displayed poll changes, i.e. about once a minute.
    """

    def __init__(self, state: "State", reader: StoreReader) -> None:
        self.state, self.reader = state, reader
        self._key: str | None = None
        self._out: dict = {"session_date": None, "poll_key": None, "series": []}
        self._lock = threading.Lock()
        # The metadata's net GEX/DEX was computed by the collector over EVERY
        # stored row, including an AM leg on its own settlement day, which
        # every panel drops (eligible()). So for a symbol that can carry such
        # rows, EACH POLL is checked on its own aggregate (an AM leg can vanish
        # mid-session) and, if it holds ineligible rows, its point is
        # recomputed from the eligible ones. Cached per poll; a poll whose
        # aggregate cannot be read is an UNAVAILABLE point (a gap), never the
        # unfiltered metadata, and is retried.
        self._pts: dict[str, tuple[float | None, float | None]] = {}

    def _point(self, k: str, a: Path, meta: dict) -> tuple[float | None, float | None] | None:
        """(gex, dex) for one poll, or None if its aggregate is unreadable."""
        stored = (meta.get("net_gex_per_pct"), meta.get("net_dex"))
        if self.reader.symbol in LEGACY_ROOTLESS:     # one PM root: the metadata is exact
            return stored
        if k in self._pts:
            return self._pts[k]
        try:
            names = set(pq.ParquetFile(a).schema_arrow.names)
            if not {"root", "expiry"} <= names:
                raise ValueError("aggregate has no root column")
            session = poll_session(k)
            t = pq.read_table(a, columns=["root", "expiry"]).to_pylist()
            if any(not eligible(r, session) for r in t):
                n = net_exposure({"spot": meta.get("spot"),
                                  "rows": _read_rows(a, NET_COLS, k, self.reader.symbol)}, per_pct=True)
                val = (n.get("net_gex"), n.get("net_dex"))
            else:
                val = stored
        except Exception:
            return None
        self._pts[k] = val
        return val

    def series(self) -> dict:
        payload, _ = self.state.get()
        key = payload.get("poll_key") if payload else None
        with self._lock:
            if key == self._key:
                return self._out
        out = {"session_date": None, "poll_key": key, "series": []}
        unavailable = 0
        if key and pq is not None:
            day = key.split("/")[0]
            rows = []
            polls = list(self.reader.complete(day))
            self._pts = {p: v for p, v in self._pts.items() if p.startswith(day + "/")}
            for k, m, a in reversed(polls):
                if k > key or k in self.reader.rejected:
                    continue
                try:
                    r = pq.read_table(m).to_pylist()[0]
                except Exception:
                    continue          # a bad file must not break the panel
                if not r.get("usable"):
                    continue
                pt = self._point(k, a, r)
                if pt is None:
                    unavailable += 1
                gex, dex = pt if pt is not None else (None, None)
                rows.append({
                    "t": r.get("received_at"),
                    "spot": r.get("spot"),
                    "gex": gex,
                    "dex": dex,
                    **({"unavailable": True} if pt is None else {}),
                    "newest_trade": r.get("newest_trade_time"),
                    "age_s": age_seconds(r.get("received_at"), r.get("newest_trade_time")),
                })
            out = {"session_date": day[len("date="):], "poll_key": key, "series": rows,
                   "median_lag_s": median_lag_seconds(rows, day[len("date="):]),
                   "n_unavailable": unavailable}
        if not unavailable:           # an unreadable poll is retried on the next request
            with self._lock:
                self._key, self._out = key, out
        return out


SMILE_MONEYNESS = 0.20       # strikes within +-20% of spot
SMILE_MAX_REL_SPREAD = 0.20  # (ask-bid)/mid; a DISPLAY-quality threshold, not a validity test
SMILE_RETRY_SECONDS = 30.0   # a transient read failure is retried, not cached for good
SMILE_COLS = ("strike", "expiry", "root", "call_iv", "put_iv", "call_bid", "call_ask",
              "put_bid", "put_ask")


def series_meta(expiry: str, root: str | None) -> dict:
    """What the page needs to tell series apart (SPX lists an AM and a PM
    series on the same date -- never merged, 2026-10-03): a stable id,
    the settlement label and the settlement instant, which is when the series
    stops being a live position (AM: the open; PM: the index close)."""
    d = date.fromisoformat(expiry)
    settles = (datetime.combine(d, settlement_time(root, d, RTH_CLOSE)).replace(tzinfo=_NY)
               .astimezone(timezone.utc))
    return {"id": oiwalls.series_key(expiry, root), "expiry": expiry, "root": root,
            "settle": oiwalls.settle_label(root), "settles_utc": settles.isoformat()}


class SmileCache:
    """IV smile for the poll ON SCREEN (anchored to view["poll_key"], like
    HistoryCache): per expiry, CBOE's own implied volatility by strike on the
    OUT-OF-THE-MONEY side -- puts below spot, calls at or above -- the usual
    way to draw one smile from two option types. A point is kept only with a
    real two-sided quote on that side (bid > 0, ask > bid, relative spread
    <= SMILE_MAX_REL_SPREAD) and a positive, finite IV, within SMILE_MONEYNESS
    of spot; per-expiry retained/excluded counts are returned. Neither filter
    proves an individual quote is fresh. Each point carries its side so the
    page never draws a seamless line across the put/call switch. The IV is
    CBOE's model value, not ours; snapshots saved before schema v2
    (2026-09-29) have none.

    Caching: a built result, and the PERMANENT "no IV stored"
    answer, are cached per poll; a transient read failure is retried after
    SMILE_RETRY_SECONDS instead of being served forever for that poll.
    """

    def __init__(self, state: "State", reader: StoreReader) -> None:
        self.state, self.reader = state, reader
        self._key: str | None = None
        self._out: dict = {"available": False, "poll_key": None}
        self._retry_at = 0.0          # >0: cached answer is a transient failure
        self._lock = threading.Lock()

    def get(self) -> dict:
        payload, _ = self.state.get()
        key = payload.get("poll_key") if payload else None
        with self._lock:
            if key == self._key and (not self._retry_at or time.time() < self._retry_at):
                return self._out
        out = self._build(key, payload) if key and pq is not None else \
            {"available": False, "poll_key": key, "reason": "no snapshot yet"}
        with self._lock:
            self._key, self._out = key, out
            self._retry_at = time.time() + (SMILE_RETRY_SECONDS if out.get("transient") else OI_REFRESH_SECONDS)
        return out

    def _build(self, key: str, payload: dict) -> dict:
        day, stamp = key.split("/")
        agg = self.reader.agg_dir / day / f"agg_{stamp}"
        session, spot = payload.get("session_date"), payload.get("spot")
        base = {"poll_key": key, "session_date": session, "spot": spot}
        if not (isinstance(spot, (int, float)) and math.isfinite(spot) and spot > 0):
            return {**base, "spot": None, "available": False,
                    "reason": "snapshot has no valid spot"}
        try:
            names = pq.read_schema(agg).names
            if "call_iv" not in names:            # permanent: cached for this poll
                return {**base, "available": False,
                        "reason": "no IV stored for this snapshot (saved before schema v2, "
                                  "2026-09-29)"}
            rows = _read_rows(agg, SMILE_COLS, key, self.reader.symbol)
        except Exception as e:                    # transient: retried later
            return {**base, "available": False, "transient": True,
                    "reason": f"could not read the snapshot: {type(e).__name__}"}
        sess = datetime.fromisoformat(session).date() if session else None
        by_exp: dict[str, dict] = {}
        for r in rows:
            k = r.get("strike")
            if not (isinstance(k, (int, float)) and math.isfinite(k)
                    and abs(k / spot - 1.0) <= SMILE_MONEYNESS):
                continue
            side = "P" if k < spot else "C"
            p = "put_" if side == "P" else "call_"
            e = by_exp.setdefault((r["expiry"], r.get("root")),
                                  {"points": [], "excluded": {}, "dropped": set()})
            iv, bid, ask = r.get(p + "iv"), r.get(p + "bid"), r.get(p + "ask")
            why = None
            if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (iv, bid, ask)):
                why = "missing or non-finite"
            elif iv <= 0:
                why = "no IV"
            elif bid <= 0 or ask <= bid:
                why = "no two-sided quote"
            elif (ask - bid) / ((ask + bid) / 2.0) > SMILE_MAX_REL_SPREAD:
                why = "spread too wide"
            if why:
                e["excluded"][why] = e["excluded"].get(why, 0) + 1
                e["dropped"].add(k)
            else:
                e["points"].append([k, round(iv, 5), side])
        expiries = []
        order = sorted(by_exp, key=lambda er: (er[0], series_meta(*er)["settles_utc"], er[1] or ""))
        for exp, root in order:
            # An expiry whose quotes ALL fail stays in the list with zero
            # points and its exclusion counts: a deteriorating chain must show
            # "0 kept, spreads too wide", not silently disappear.
            e = by_exp[(exp, root)]
            dte = (datetime.fromisoformat(exp).date() - sess).days if sess else None
            # 4th field: 1 if a LISTED strike was filtered out between this
            # point and the previous kept one -- the page breaks the line
            # there, and only there. (Breaking on spacing instead cut every
            # $1 -> $5 strike-grid step and hid the far wings.)
            pts = sorted(e["points"])
            dropped = sorted(e["dropped"])
            for i, pt in enumerate(pts):
                prev = pts[i - 1][0] if i else None
                pt.append(1 if prev is not None and any(prev < d < pt[0] for d in dropped) else 0)
            expiries.append({**series_meta(exp, root), "dte": dte, "points": pts,
                             "retained": len(e["points"]), "excluded": e["excluded"]})
        any_pts = any(e["retained"] for e in expiries)
        return {**base, "available": any_pts, "expiries": expiries,
                "rule": {"side": "OTM (puts below spot, calls at/above)",
                         "moneyness": SMILE_MONEYNESS, "max_rel_spread": SMILE_MAX_REL_SPREAD,
                         "quote": "bid > 0, ask > bid, (ask-bid)/mid <= max_rel_spread",
                         "source": "CBOE-reported model IV, delayed; not recomputed"},
                **({} if any_pts else {"reason": "no quote-valid points"})}


# Symbols the daily accuracy report (validate.py) covers.
VALIDATED_SYMBOLS = frozenset({"SPY"})


class StatusCache:
    """Market/collector status and the latest accuracy report, for the
    feed-health panel. Reads local files only; cached CACHE_SECONDS.

    Deliberately narrow claims: "stored" counts
    SAVED snapshots (unchanged CBOE responses are not saved), and the
    accuracy report certifies the one poll it checked, not later ones.
    """
    CACHE_SECONDS = 10.0

    def __init__(self, root: Path, symbol: str) -> None:
        self.root, self.sym = Path(root), symbol.lstrip("_")
        self.reader = StoreReader(root, symbol)
        self._at, self._out = 0.0, None
        self._lock = threading.Lock()

    @staticmethod
    def _key_time(key: str) -> datetime | None:
        """UTC receipt time from a poll key 'date=YYYY-MM-DD/HHMMSS_ffffff.parquet'
        (partitions are by UTC receipt date; names carry the UTC receipt time)."""
        try:
            day, stamp = key.split("/")
            return datetime.strptime(day[len("date="):] + stamp[:13],
                                     "%Y-%m-%d%H%M%S_%f").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    def _stored_recent(self, now: datetime) -> tuple[int, str | None]:
        """(COMPLETE, non-quarantined snapshots saved in the last 10 min,
        newest complete snapshot's save time -- searched back across days, so
        weekends and pre-open still show the real last save)."""
        n = 0
        for day in sorted({now.date(), (now - timedelta(minutes=10)).date()}):
            for key, _m, _a in self.reader.complete(f"date={day.isoformat()}"):
                t = self._key_time(key)
                if t and timedelta(0) <= now - t <= timedelta(minutes=10):
                    n += 1
        newest = self.reader.newest()
        t = self._key_time(newest[0]) if newest else None
        return n, t.isoformat() if t else None

    def _validation(self) -> dict | None:
        # validation/date=*.json is not partitioned by symbol: every report so
        # far is SPY's. Another symbol must get NO report, not SPY's one
        # under its name -- see validation_note in get().
        if self.sym not in VALIDATED_SYMBOLS:
            return None
        d = self.root / "validation"
        files = sorted(d.glob("date=*.json")) if d.exists() else []
        if not files:
            return None
        try:
            v = json.loads(files[-1].read_text())
        except Exception as e:
            return {"status": "unreadable", "error": f"{type(e).__name__}: {e}",
                    "file": files[-1].name}
        occ, gk, ar = v.get("occ_oi") or {}, v.get("greeks") or {}, v.get("arithmetic") or {}
        b, coh = gk.get("benchmark") or {}, gk.get("cohort") or {}
        return {
            "date": v.get("date"), "status": v.get("status"), "run_at": v.get("run_at"),
            "reason": v.get("reason"), "error": v.get("error"), "poll": v.get("poll"),
            "occ": {"status": occ.get("status"), "sides_equal": occ.get("sides_equal"),
                    "sides_compared": occ.get("sides_compared"),
                    "n_unexplained": occ.get("n_unexplained"),
                    "n_invalid_oi": occ.get("n_invalid_oi"), "reason": occ.get("reason"),
                    "error": occ.get("error")},
            "greeks": {"status": gk.get("status"), "flags": gk.get("flags"), "n": b.get("n"),
                       "gamma_median": b.get("gamma_rel_err_median"),
                       "gamma_p95": b.get("gamma_rel_err_p95"),
                       "delta_p95": b.get("delta_abs_err_p95"),
                       "delta_median": b.get("delta_abs_err_median"),
                       "scope": gk.get("scope"),
                       "gross_gamma_coverage": coh.get("gross_gamma_coverage"),
                       "error": gk.get("error")},
            "arithmetic": {"status": ar.get("status"), "net_over_gross": ar.get("net_over_gross")},
            # Report-only diagnostic (audit F12); absent from reports written before it existed.
            "short_dated": None if not v.get("short_dated") else {
                "status": v["short_dated"].get("status"), "flags": v["short_dated"].get("flags"),
                "error": v["short_dated"].get("error"),
                "cohorts": {lab: {"status": c.get("status"), "n": c.get("n"),
                                  "coverage": c.get("gross_gamma_coverage"),
                                  "gamma_16_15": (c.get("at_16_15") or {}).get("gamma_rel_err_median"),
                                  "gamma_16_00": (c.get("at_16_00") or {}).get("gamma_rel_err_median"),
                                  "multiplier": (c.get("fitted") or {}).get("multiplier_of_16_15_T"),
                                  "effective_hours": c.get("effective_hours"),
                                  "hours_to_16_15": c.get("hours_to_16_15")}
                            for lab, c in (v["short_dated"].get("cohorts") or {}).items()}},
            "stale_open": v.get("stale_open"),
            "n_unreadable_polls": len(v.get("unreadable_polls") or []),
            "n_prev_unreadable_polls": len(v.get("previous_session_unreadable_polls") or []),
        }

    def get(self, now: datetime | None = None) -> dict:
        if now is None:
            with self._lock:
                if self._out is not None and time.time() - self._at < self.CACHE_SECONDS:
                    return self._out
        now = now or datetime.now(timezone.utc)
        is_open, why = market_session(now)
        n10, newest = self._stored_recent(now)
        out = {"now": now.isoformat(),
               "today_et": now.astimezone(_NY).date().isoformat(),
               "market": {"open": is_open, "reason": why},
               "collecting": in_session(now),
               "stored_last_10min": n10, "last_stored_at": newest,
               "symbol": self.sym,
               "validation": self._validation(),
               "validation_note": None if self.sym in VALIDATED_SYMBOLS else
               f"{self.sym} is not validated yet -- the daily accuracy report covers SPY only"}
        with self._lock:
            self._out, self._at = out, time.time()
        return out


OI_RETRY_SECONDS = 30.0      # a transient read failure is retried, not cached
OI_REFRESH_SECONDS = 60.0    # even a clean result is rechecked this often
                             # (a quarantined source must surface after the close too)
OI_COLS = ("strike", "expiry", "root", "call_oi", "put_oi")


class OiCache:
    """Front-expiry open interest per trading session (gex/oiwalls.py) for
    the poll ON SCREEN. The displayed session uses its newest LIVE poll at or
    before view["poll_key"] -- never a later or a stale one;
    earlier sessions use their newest live poll. "Live" means the snapshot's
    newest trade is dated that session: trading was observed. It does NOT
    prove CBOE had already updated the OI -- the chosen snapshot's own time
    is returned so the effective date stays visible.

    Weekdays with no partition or no live poll are GAPS (2026-09-23: two
    pre-open polls whose OI equals the day before). Market closures are
    listed apart, never as missing sessions (2026-09-07, Labor Day: 49
    stale polls stored before holiday skipping existed).

    Only a clean "ok" summary of a settled session is cached, keyed by its
    partition's newest complete poll, so a partition that changes (a file
    pair completed, a poll rejected) is rebuilt; gaps, invalid summaries and
    unreadable fallbacks are never cached. The displayed
    session is rebuilt when the poll on screen changes; a read failure is
    transient -- retried after OI_RETRY_SECONDS.
    """

    def __init__(self, state: "State", reader: StoreReader) -> None:
        self.state, self.reader = state, reader
        self._done: dict[str, tuple] = {}    # settled session iso -> (signature, summary)
        self._key: str | None = None
        self._out: dict = {"poll_key": None, "sessions": [], "current": None}
        self._retry_at = 0.0
        self._lock = threading.Lock()

    def get(self) -> dict:
        payload, _ = self.state.get()
        key = payload.get("poll_key") if payload else None
        with self._lock:
            if key == self._key and (not self._retry_at or time.time() < self._retry_at):
                return self._out
        out = self._build(key) if key and pq is not None else \
            {"poll_key": key, "sessions": [], "current": None, "reason": "no snapshot yet"}
        with self._lock:
            self._key, self._out = key, out
            # Every result has a bounded life, so source changes surface even
            # while the poll on screen does not change (after the close,
            # weekends): negative results (gaps, invalid) and read errors are
            # retried sooner. Settled sessions still come from _done unless
            # their source changed, so a refresh is a few globs and one read.
            self._retry_at = time.time() + (OI_RETRY_SECONDS if out.get("retry") else OI_REFRESH_SECONDS)
        return out

    def _pick(self, day_key: str, anchor: str | None):
        """(key, meta, agg rows, n unreadable skipped) of the newest live,
        usable, non-rejected poll of a partition at or before `anchor`."""
        day, skipped = day_key[len("date="):], 0
        for k, m, a in self.reader.complete(day_key):
            if (anchor and k > anchor) or k in self.reader.rejected:
                continue
            try:
                meta = pq.read_table(m).to_pylist()[0]
                if not meta.get("usable") or str(meta.get("newest_trade_time") or "")[:10] != day:
                    continue
                rows = _read_rows(a, OI_COLS, k, self.reader.symbol)
            except Exception:
                skipped += 1           # disclosed, and the result is not cached
                continue
            return k, meta, rows, skipped
        return None, None, None, skipped

    def _session(self, s: date, day_key: str, anchor: str | None, parts: set) -> dict:
        iso = s.isoformat()
        if day_key not in parts:
            return {"date": iso, "status": "gap", "reason": "no snapshots stored"}
        k, meta, rows, skipped = self._pick(day_key, anchor)
        if k is None:
            return {"date": iso, "status": "error" if skipped else "gap", "unreadable_skipped": skipped,
                    "reason": (f"{skipped} unreadable snapshot(s), none usable" if skipped else
                               "no live snapshot (none with a trade dated this session)")}
        out = oiwalls.summarize(rows, meta.get("spot"), s)
        valid = out["total_oi"] is not None
        out.update({"date": iso, "status": "ok" if valid else "invalid",
                    # When the FRONT series stops being live (SPXW 16:00 / 13:00,
                    # SPX AM the open, SPY options_close) -- the page's "ended"
                    # note uses this, not the session's 16:15 close.
                    "front_settles_utc": (series_meta(out["front_expiry"], out["front_root"])["settles_utc"]
                                          if out.get("front_expiry") else None),
                    "reason": None if valid else "; ".join(out["problems"]),
                    "snapshot_key": k, "received_at": meta.get("received_at"),
                    "newest_trade": meta.get("newest_trade_time"), "unreadable_skipped": skipped})
        return out

    def _sig(self, day_key: str, parts: set):
        """What a cached summary was built from: the partition's newest
        complete poll. Changes when a file pair completes or lands late."""
        if day_key not in parts:
            return None
        return next(iter(self.reader.complete(day_key)), (None,))[0]

    def _build(self, key: str) -> dict:
        shown_key = key.split("/")[0]
        shown = date.fromisoformat(shown_key[len("date="):])
        parts = set(self.reader._days())
        dated = sorted(date.fromisoformat(d[len("date="):]) for d in parts)
        if not dated:
            return {"poll_key": key, "sessions": [], "current": None, "reason": "no partitions"}
        first, transient = dated[0], False
        closures = []
        d = first
        while d <= shown:
            if d.weekday() < 5 and oiwalls.holiday_name(d):
                dk = "date=" + d.isoformat()
                closures.append({"date": d.isoformat(), "name": oiwalls.holiday_name(d),
                                 "stored_polls": len(list(self.reader.complete(dk))) if dk in parts else 0})
            d += timedelta(days=1)
        summaries: dict[str, dict] = {}
        for s in oiwalls.trading_days(first, shown):
            iso, dk = s.isoformat(), "date=" + s.isoformat()
            anchor = key if dk == shown_key else None
            if anchor is None and iso in self._done:
                sig, cached = self._done[iso]
                # Reused only while its source is unchanged: same newest complete
                # poll, AND the snapshot it used is still complete, not
                # quarantined (complete() drops those) and not rejected.
                keys = [k for k, _, _ in self.reader.complete(dk)] if dk in parts else []
                if (keys and sig == keys[0] and cached.get("snapshot_key") in set(keys)
                        and cached.get("snapshot_key") not in self.reader.rejected):
                    summaries[iso] = cached
                    continue
                del self._done[iso]              # its source changed: rebuild
            try:
                one = self._session(s, dk, anchor, parts)
            except Exception as e:                       # a bad file must not break the panel
                one = {"date": iso, "status": "error", "reason": f"{type(e).__name__}: {e}"}
            if one["status"] == "error" or one.get("unreadable_skipped"):
                transient = True
            elif anchor is None and one["status"] == "ok":
                self._done[iso] = (self._sig(dk, parts), one)
            summaries[iso] = one
        sessions = []
        for iso, one in summaries.items():
            s = date.fromisoformat(iso)
            prev = oiwalls.previous_trading_day(s)
            if one["status"] == "ok":
                if prev < first:
                    chg = oiwalls.same_expiry_change(one, None, prev, "before the first stored session")
                else:
                    base = summaries.get(prev.isoformat())
                    chg = oiwalls.same_expiry_change(
                        one, base if base and base["status"] == "ok" else None, prev,
                        None if not base or base["status"] == "gap" else
                        f"previous trading session {prev} {base['status']}: {base.get('reason')}")
            else:
                chg = None
            row = {k: v for k, v in one.items() if k not in ("expiry_totals", "strikes")}
            row["change"] = chg
            sessions.append(row)
        ok = [x for x in sessions if x["status"] == "ok"]
        cur = ok[-1] if ok else None
        current = None
        if cur:
            current = {**cur, "strikes": summaries[cur["date"]]["strikes"],
                       "is_shown_session": cur["date"] == shown.isoformat()}
        negative = any(x["status"] in ("gap", "invalid") for x in sessions)
        return {"poll_key": key, "shown_session": shown.isoformat(), "window": oiwalls.WINDOW,
                "sessions": sessions, "closures": closures, "current": current,
                "transient": transient,              # read errors: shown as a warning
                "retry": transient or negative}      # anything that could still change


TRACKS_COLS = ("strike", "call_gamma_oi", "put_gamma_oi")
TRACKS_WAIT_SECONDS = 20.0   # a request waits this long for an in-flight build
TRACKS_OUT = ("t", "spot", "newest_trade", "age_s", "status", "reason", "max", "min", "ratio",
              "max_none", "min_none")      # what the page reads; ~400 points a day


class TracksCache:
    """Intraday max/min gamma strikes and the all-expiry GEX ratio, one entry
    per stored poll of the session ON SCREEN (gex/gammatracks.py).

    Anchored like HistoryCache: the displayed day's complete, usable,
    non-rejected polls at or before view["poll_key"]. A poll's result is
    cached by its key (a stored aggregate never changes); the series is
    reassembled from the CURRENT complete() list each time, so quarantined
    or rejected polls drop out, and only the displayed day is kept.

    Design checked (2026-09-30):
      * an unreadable aggregate with readable metadata stays in the series as
        a TIMESTAMPED unavailable entry (the page breaks its lines there);
        unreadable metadata cannot be placed and is counted apart; neither
        is cached, and the result is transient (retried);
      * single-flight: one build at a time (a cold day is ~4 s on the VM),
        other requests wait for it; a result is published only while its
        anchor is still the poll on screen;
      * bounded life: 30 s after a read error, else 60 s.

    It also builds the intraday heatmap (step 5, gex/heatmap.py) from the SAME
    reads and the same netting pass: each cached poll keeps
    its full per-strike nets; /api/heat is a second projection of the build,
    published under the same anchor. /api/tracks is unchanged -- the nets are
    never in TRACKS_OUT -- and a heat-assembly failure cannot touch it.
    """

    def __init__(self, state: "State", reader: StoreReader) -> None:
        self.state, self.reader = state, reader
        self._day: str | None = None
        self._polls: dict[str, dict] = {}     # poll key -> result (displayed day only)
        self._key: str | None = None
        self._out: dict = {"poll_key": None, "series": []}
        self._heat: dict = {"poll_key": None, "strikes": [], "columns": []}
        self._retry_at = 0.0
        self._cond = threading.Condition()
        self._building = False
        self.builds = 0                        # for tests: how many builds ran

    def _anchor(self) -> str | None:
        payload, _ = self.state.get()
        return payload.get("poll_key") if payload else None

    def get(self) -> dict:
        return self._get()[0]

    def get_heat(self) -> dict:
        return self._get()[1]

    def _get(self) -> tuple[dict, dict]:
        with self._cond:
            deadline = time.time() + TRACKS_WAIT_SECONDS
            while self._building and time.time() < deadline:
                self._cond.wait(timeout=max(0.0, deadline - time.time()))
            key = self._anchor()               # read AFTER waiting: the screen may have moved
            if key == self._key and time.time() < self._retry_at:
                return self._out, self._heat
            if self._building:                 # waited too long: answer without a build
                busy = {"poll_key": key, "transient": True, "retry": True, "reason": "a build is still running"}
                return {**busy, "series": []}, {**busy, "strikes": [], "columns": []}
            self._building = True
        out = heat = None
        try:
            if key and pq is not None:
                out, heat = self._build(key)
            else:
                out = {"poll_key": key, "series": [], "reason": "no snapshot yet"}
                heat = {"poll_key": key, "strikes": [], "columns": [], "reason": "no snapshot yet"}
        finally:
            # Publish (or discard) BEFORE releasing the waiters, in one critical
            # section -- else a waiter can wake to the old _key and build again
            # (reproduced three builds for three requests).
            with self._cond:
                if out is not None and self._anchor() == key:     # still on screen
                    self._key, self._out, self._heat = key, out, heat
                    self._retry_at = time.time() + (OI_RETRY_SECONDS if out.get("transient") or heat.get("transient")
                                                    else OI_REFRESH_SECONDS)
                self._building = False
                self._cond.notify_all()
        return out, heat

    def _one(self, k: str, m: Path, a: Path) -> dict:
        try:
            meta = pq.read_table(m).to_pylist()[0]
        except Exception:
            return {"key": k, "status": "unplaceable", "cacheable": False}
        if not meta.get("usable"):
            return {"key": k, "status": "unusable", "cacheable": True}
        try:                                   # a point that cannot be placed in time is disclosed
            datetime.fromisoformat(str(meta.get("received_at")))
        except ValueError:
            return {"key": k, "status": "unplaceable", "cacheable": False}
        spot = meta.get("spot")
        spot = spot if isinstance(spot, (int, float)) and not isinstance(spot, bool) \
            and math.isfinite(spot) and spot > 0 else None     # NaN would make the JSON invalid
        base = {"key": k, "t": meta.get("received_at"), "spot": spot,
                "newest_trade": meta.get("newest_trade_time"),
                "age_s": age_seconds(meta.get("received_at"), meta.get("newest_trade_time"))}
        try:
            rows = _read_rows(a, TRACKS_COLS, k, self.reader.symbol)
        except Exception as e:
            return {**base, "status": "unreadable", "reason": f"aggregate unreadable: {type(e).__name__}",
                    "cacheable": False}
        netted = gammatracks.strike_nets(rows)
        ex = gammatracks.extrema(rows, meta.get("spot"), netted)
        return {**base, **ex, "status": "ok" if ex["ok"] else "invalid", "cacheable": True,
                "nets": netted[0] if ex["ok"] else None}     # every strike: the heat window is applied later

    def _build(self, key: str) -> tuple[dict, dict]:
        self.builds += 1
        day = key.split("/")[0]
        if day != self._day:
            self._day, self._polls = day, {}
        polls = [x for x in reversed(list(self.reader.complete(day)))
                 if x[0] <= key and x[0] not in self.reader.rejected]
        present = {x[0] for x in polls}
        self._polls = {k: v for k, v in self._polls.items() if k in present}
        series, cols, unreadable, unplaceable = [], [], 0, 0
        for k, m, a in polls:
            rec = self._polls.get(k)
            if rec is None:
                rec = self._one(k, m, a)
                if rec.pop("cacheable"):
                    self._polls[k] = rec
            if rec["status"] == "unusable":
                continue
            if rec["status"] == "unplaceable":
                unplaceable += 1
                continue
            unreadable += rec["status"] == "unreadable"
            series.append({f: rec[f] for f in TRACKS_OUT if f in rec})
            cols.append(rec)
        sess = day[len("date="):]
        common = {"poll_key": key, "session_date": sess, "median_lag_s": median_lag_seconds(series, sess),
                  "n_unreadable": unreadable, "n_unplaceable": unplaceable,
                  "transient": bool(unreadable or unplaceable), "retry": bool(unreadable or unplaceable)}
        try:
            heat = {**common, **heatmap.grid(cols)}
        except Exception as e:                 # never takes the tracks down with it
            heat = {**common, "strikes": [], "columns": [], "transient": True, "retry": True,
                    "reason": f"heatmap assembly failed: {type(e).__name__}"}
        tracks = {"poll_key": key, "session_date": sess, "series": series}     # key order as before
        tracks.update({k: common[k] for k in ("median_lag_s", "n_unreadable", "n_unplaceable", "transient", "retry")})
        return tracks, heat


TERM_COLS = ("strike", "expiry", "root", "call_iv", "put_iv", "call_bid", "call_ask", "put_bid", "put_ask",
             "call_delta", "put_delta", "call_delta_oi", "put_delta_oi", "call_oi", "put_oi")


class TermCache:
    """IV term structure at fixed deltas + the 25Δ skew per expiry
    (gex/ivterm.py) for the poll ON SCREEN. Reads whichever of TERM_COLS
    the poll's schema has (v3: stored deltas; v2: recovered; v1: no IV ->
    unavailable). Every expiry carries its scheduled options close so the
    PAGE drops ended expiries by the clock -- a result cached per poll key
    cannot know the time. Built results and the permanent "no IV"
    answer are cached per poll; read failures are retried."""

    def __init__(self, state: "State", reader: StoreReader) -> None:
        self.state, self.reader = state, reader
        self._key: str | None = None
        self._out: dict = {"available": False, "poll_key": None}
        self._retry_at = 0.0
        self._lock = threading.Lock()

    def get(self) -> dict:
        payload, _ = self.state.get()
        key = payload.get("poll_key") if payload else None
        with self._lock:
            if key == self._key and (not self._retry_at or time.time() < self._retry_at):
                return self._out
        out = self._build(key, payload) if key and pq is not None else \
            {"available": False, "poll_key": key, "reason": "no snapshot yet"}
        with self._lock:
            self._key, self._out = key, out
            # Bounded life: a quarantined/rejected source must surface
            # even while the poll on screen does not change.
            self._retry_at = time.time() + (SMILE_RETRY_SECONDS if out.get("transient") else OI_REFRESH_SECONDS)
        return out

    def _build(self, key: str, payload: dict) -> dict:
        spot = payload.get("spot")
        feed = payload.get("feed") or {}
        # The result's OWN timestamps: the page's mixed-age warning must follow
        # the snapshot these IVs came from, not whatever is on screen now.
        base = {"poll_key": key, "session_date": payload.get("session_date"),
                "received_at": feed.get("received_at"), "newest_trade": feed.get("newest_trade_time")}
        if key in self.reader.rejected:
            return {**base, "available": False, "transient": True, "reason": "snapshot rejected by the reader"}
        if not isinstance(spot, (int, float)) or not math.isfinite(spot) or spot <= 0:
            return {**base, "available": False, "reason": "no valid spot"}
        try:
            day = key.split("/")[0]
            agg = next((a for k, _m, a in self.reader.complete(day) if k == key), None)
            if agg is None:
                return {**base, "available": False, "transient": True, "reason": "poll files not found"}
            names = set(pq.ParquetFile(agg).schema_arrow.names)
            rows = _read_rows(agg, TERM_COLS, key, self.reader.symbol)     # absent columns -> None
        except Exception as e:
            return {**base, "available": False, "transient": True,
                    "reason": f"read failed: {type(e).__name__}"}
        if "call_iv" not in names:
            return {**base, "available": False, "reason": "snapshot predates schema v2 (no IV stored)"}
        sess = str(payload.get("session_date"))
        # One curve per SERIES (expiry, root): ivterm interpolates between the
        # contracts it is given, so an AM and a PM series on one date must
        # never share a call.
        by = {}
        for r in rows:
            if r.get("expiry") and r["expiry"] >= sess:
                by.setdefault((r["expiry"], r.get("root")), []).append(r)
        try:      # the fixed reference for fractional days: this snapshot's receipt
            ref = datetime.fromisoformat(str(base.get("received_at")))
        except ValueError:
            ref = None
        expiries = []
        for exp, root in sorted(by, key=lambda er: (er[0], series_meta(*er)["settles_utc"], er[1] or "")):
            m = series_meta(exp, root)
            d = date.fromisoformat(exp)
            c = ivterm.expiry_curve(by[(exp, root)], spot, "call_delta" in names and "put_delta" in names)
            days = ((datetime.fromisoformat(m["settles_utc"]) - ref).total_seconds() / 86400.0
                    if ref else None)
            # close_utc = the series' SETTLEMENT (AM: the open; SPXW: 16:00 /
            # 13:00), which is when the page drops it; `days` places it on the
            # square-root axis; `dte` stays the calendar count for labels.
            expiries.append({**m, "dte": (d - datetime.fromisoformat(sess).date()).days,
                             "close_utc": m["settles_utc"], "days": days, **c})
        return {**base, "available": bool(expiries), "spot": spot, "expiries": expiries,
                "targets": [t[0] for t in ivterm.TARGETS],
                "rules": {"max_rel_spread": ivterm.MAX_REL_SPREAD, "max_delta_gap": ivterm.MAX_DELTA_GAP,
                          "max_strike_gap": ivterm.MAX_STRIKE_GAP}}


def norm_symbol(sym: str | None) -> str:
    """'SPX', '_SPX', 'spx' -> 'SPX' (the store's partition name)."""
    return (sym or "").strip().lstrip("_").upper()


def bundle(symbol: str, **parts) -> SimpleNamespace:
    """One symbol's request context; parts not given are None (the tests wire
    only the caches they exercise)."""
    b = SimpleNamespace(symbol=symbol, state=None, reader=None, history=None, status=None,
                        smile=None, oi=None, tracks=None, term=None)
    b.__dict__.update(parts)
    return b


def make_bundle(data_root, symbol: str, check_every: float, stop: threading.Event) -> SimpleNamespace:
    """Everything one symbol's page needs, with its own store_loop thread.
    Symbols share nothing but the process: a slow or empty store for one
    (e.g. SPX before its first poll) cannot hold up the other."""
    state = State()
    reader = StoreReader(data_root, symbol)
    threading.Thread(target=store_loop, args=(state, reader, check_every, stop),
                     daemon=True, name=f"store_loop-{symbol}").start()
    return bundle(symbol, state=state, reader=reader,
                  history=HistoryCache(state, reader), status=StatusCache(data_root, symbol),
                  smile=SmileCache(state, reader), oi=OiCache(state, reader),
                  tracks=TracksCache(state, reader), term=TermCache(state, reader))


class Handler(BaseHTTPRequestHandler):
    # symbol -> bundle (make_bundle); the first symbol is the default, so the
    # URL without ?symbol= keeps showing what it always showed (SPY).
    bundles: dict = {}
    default_symbol: str = "SPY"

    def log_message(self, *args) -> None:      # quiet; the poller logs instead
        pass

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path, _, query = self.path.partition("?")
        sym = norm_symbol((parse_qs(query).get("symbol") or [self.default_symbol])[0])
        b = self.bundles.get(sym)
        if path.startswith("/api/") and path != "/api/symbols" and b is None:
            self._send(404, json.dumps({"error": f"unknown symbol {sym!r}",
                                        "symbols": list(self.bundles)}).encode(), "application/json")
            return
        if path == "/api/symbols":
            self._send(200, json.dumps({"symbols": list(self.bundles),
                                        "default": self.default_symbol}).encode(), "application/json")
        elif path in ("/", "/index.html"):
            f = WEB_DIR / "index.html"
            if not f.exists():
                self._send(404, b"web/index.html not built yet", "text/plain")
                return
            self._send(200, f.read_bytes(), "text/html; charset=utf-8")
        elif path == "/gexmath.js":
            # The page's pure math, shared with the node test. One named file,
            # not a static directory: nothing else under web/ is served.
            f = WEB_DIR / "gexmath.js"
            if not f.exists():
                self._send(404, b"web/gexmath.js missing", "text/plain")
                return
            self._send(200, f.read_bytes(), "text/javascript; charset=utf-8")
        elif path == "/api/snapshot":
            payload, version = b.state.get()
            if payload is None:
                self._send(503, json.dumps({"error": "no snapshot yet"}).encode(),
                           "application/json")
                return
            body = json.dumps({**payload, "version": version}).encode()
            self._send(200, body, "application/json")
        elif path == "/api/history":
            out = (b.history.series() if b.history
                   else {"session_date": None, "poll_key": None, "series": []})
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/smile":
            out = b.smile.get() if b.smile else {"available": False, "poll_key": None}
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/oi":
            out = b.oi.get() if b.oi else {"poll_key": None, "sessions": [], "current": None}
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/tracks":
            out = b.tracks.get() if b.tracks else {"poll_key": None, "series": []}
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/heat":
            out = b.tracks.get_heat() if b.tracks else {"poll_key": None, "strikes": [], "columns": []}
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/term":
            out = b.term.get() if b.term else {"available": False, "poll_key": None}
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/status":
            out = b.status.get() if b.status else {}
            self._send(200, json.dumps(out).encode(), "application/json")
        elif path == "/api/stream":
            self._stream(b.state)
        else:
            self._send(404, b"not found", "text/plain")

    def _stream(self, state: State) -> None:
        """SSE. Pushes only when the version changes, so an idle source costs
        one keepalive comment rather than a re-send."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        last = -1
        try:
            while True:
                payload, version = state.get()
                if payload is not None and version != last:
                    last = version
                    self.wfile.write(b"data: ")
                    self.wfile.write(json.dumps({**payload, "version": version}).encode())
                    self.wfile.write(b"\n\n")
                    self.wfile.flush()
                else:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                time.sleep(2)
        except (BrokenPipeError, ConnectionResetError):
            return


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="GEX dashboard server")
    ap.add_argument("--symbol", action="append", default=None,
                    help="repeatable; the FIRST is the page's default (an old unit's "
                         "single --symbol SPY still starts unchanged)")
    ap.add_argument("--port", type=int, default=8788)
    ap.add_argument("--bind", default="127.0.0.1",
                    help="DO NOT expose publicly -- Cboe prohibits redistribution")
    ap.add_argument("--check-every", type=float, default=5.0,
                    help="seconds between checks of the collector's store")
    ap.add_argument("--interval", type=float, default=None,
                    help="IGNORED since 2026-09-29 -- this server no longer polls "
                         "CBOE; accepted so an old unit file still starts")
    ap.add_argument("--data-root", default="data",
                    help="the collector's output root; everything shown comes from here")
    a = ap.parse_args(argv)
    if pq is None:
        print(json.dumps({"msg": "pyarrow is required to read the store"}), flush=True)
        return 2

    if a.bind not in ("127.0.0.1", "localhost", "::1"):
        print(json.dumps({"msg": "REFUSING to bind non-loopback",
                          "bind": a.bind,
                          "why": "Cboe delayed data may not be redistributed "
                                 "externally; use an SSH tunnel"}), flush=True)
        return 2

    stop = threading.Event()
    symbols = list(dict.fromkeys(norm_symbol(x) for x in (a.symbol or ["SPY"])))
    Handler.bundles = {sym: make_bundle(a.data_root, sym, a.check_every, stop) for sym in symbols}
    Handler.default_symbol = symbols[0]
    srv = ThreadingHTTPServer((a.bind, a.port), Handler)
    print(json.dumps({"msg": "serving", "url": f"http://{a.bind}:{a.port}",
                      "symbols": symbols, "source": "collector store (no CBOE polling)",
                      "data_root": a.data_root, "check_every": a.check_every,
                      **({"ignored_interval": a.interval} if a.interval is not None else {})}),
          flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
