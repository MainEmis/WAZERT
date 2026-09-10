#!/usr/bin/env python3
import logging
import math
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse

from waze_client import (
    WazeSession,
    _region,
    _shrinking_boxes,
    _shrink,
    _parse_alerts,
    _parse_removed_ids,
)

log = logging.getLogger("waze_api")

_API_KEY    = os.environ.get("API_KEY", "")
_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# ── session lifetime ──────────────────────────────────────────────────────────
SESSION_TTL    = 25.0   # server kills anonymous sessions ~30s
_EMA_ALPHA     = 0.3    # smoothing factor for all EMA metrics
_SAFETY_MARGIN = 2.0    # buffer on top of creation EMA before expiry

# ── traffic / idle ───────────────────────────────────────────────────────────
_IDLE_MULTIPLIER  = 3.0   # stop pre-baking after (EMA_interval × 3) of silence
_MIN_IDLE_TIMEOUT = 90.0  # never stop pre-baking if last req was <90s ago

# ── error / backoff ──────────────────────────────────────────────────────────
_BACKOFF_BASE  = 5.0    # first backoff duration (seconds)
_BACKOFF_MAX   = 120.0  # cap
_OK_TO_RESET   = 3      # consecutive successes needed to clear backoff


def _auth(key: str | None = Security(_key_header)):
    if not _API_KEY:
        return
    if key != _API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


class _Session:
    def __init__(self, sess: WazeSession, lat: float, lon: float):
        self.sess  = sess
        self.lat   = lat
        self.lon   = lon
        self._born = time.monotonic()

    def alive(self) -> bool:
        return self.age < SESSION_TTL

    @property
    def age(self) -> float:
        return time.monotonic() - self._born


class _Slot:
    """
    Per-region adaptive state machine.

    Signals tracked:
      creation_ema     — how long register→login→handshake actually takes
      req_interval_ema — EMA of seconds between incoming requests
      consec_errors    — consecutive failures driving exponential backoff
      last_req_at      — monotonic timestamp of last request (drives idle gate)
    """
    def __init__(self):
        self.cv              = threading.Condition(threading.Lock())

        # session slots
        self.current         : _Session | None = None
        self.warm            : _Session | None = None
        self.creating        : bool  = False
        self.baking          : bool  = False

        # last known coords for pre-bake
        self.lat             : float = 0.0
        self.lon             : float = 0.0

        # adaptive creation EMA (drives prefetch_age)
        self.creation_ema    : float = 15.0

        # traffic signal (drives idle_window)
        self.last_req_at     : float = 0.0
        self.req_interval_ema: float = 60.0

        # error budget (drives backoff)
        self.consec_errors   : int   = 0
        self.consec_ok       : int   = 0
        self.backoff_until   : float = 0.0

    # ── derived thresholds ────────────────────────────────────────────────────

    @property
    def prefetch_age(self) -> float:
        """Age at which to start pre-baking the next session."""
        return max(0.5, SESSION_TTL - self.creation_ema - _SAFETY_MARGIN)

    @property
    def idle_window(self) -> float:
        """How long we pre-bake after last request before going idle."""
        return max(_MIN_IDLE_TIMEOUT, self.req_interval_ema * _IDLE_MULTIPLIER)

    @property
    def region_active(self) -> bool:
        """True if traffic was recent enough to warrant pre-baking."""
        if self.last_req_at == 0.0:
            return False
        return time.monotonic() - self.last_req_at < self.idle_window

    @property
    def in_backoff(self) -> bool:
        return time.monotonic() < self.backoff_until

    # ── signal recorders ─────────────────────────────────────────────────────

    def record_request(self):
        """Called on every incoming request. Updates traffic EMA."""
        now = time.monotonic()
        if self.last_req_at > 0:
            interval = now - self.last_req_at
            self.req_interval_ema = (
                _EMA_ALPHA * interval + (1 - _EMA_ALPHA) * self.req_interval_ema
            )
        self.last_req_at = now

    def record_creation(self, elapsed: float):
        """Update creation EMA after a successful session creation."""
        self.creation_ema = _EMA_ALPHA * elapsed + (1 - _EMA_ALPHA) * self.creation_ema

    def record_success(self):
        """One successful query. Clear error budget after 3 consecutive."""
        self.consec_ok    += 1
        self.consec_errors = 0
        if self.consec_ok >= _OK_TO_RESET:
            self.backoff_until = 0.0

    def record_error(self):
        """One failed query. Grow backoff exponentially."""
        self.consec_ok     = 0
        self.consec_errors += 1
        delay = min(_BACKOFF_BASE * (2 ** (self.consec_errors - 1)), _BACKOFF_MAX)
        self.backoff_until = time.monotonic() + delay
        log.warning(
            "region error #%d — backing off %.0fs (idle_window=%.0fs)",
            self.consec_errors, delay, self.idle_window,
        )


_slots = {r: _Slot() for r in ("row", "na", "il")}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="Waze RT API", lifespan=lifespan)


# ── session factory ───────────────────────────────────────────────────────────

def _make_session(lat: float, lon: float) -> _Session:
    sess = WazeSession(lat, lon, debug=False)
    sess.register(lat, lon)
    sess.login(lat, lon)
    sess.prepare_for_area(lat, lon)
    return _Session(sess, lat, lon)


def _prebake(slot: _Slot):
    """Background thread: create warm session using last known coords."""
    with slot.cv:
        lat, lon = slot.lat, slot.lon

    t0 = time.monotonic()
    try:
        warm    = _make_session(lat, lon)
        elapsed = time.monotonic() - t0
        with slot.cv:
            slot.warm = warm
            slot.record_creation(elapsed)
            log.debug(
                "pre-bake ready %.1fs (creation_ema=%.1fs prefetch_age=%.1fs)",
                elapsed, slot.creation_ema, slot.prefetch_age,
            )
    except Exception:
        log.exception("pre-bake failed")
    finally:
        with slot.cv:
            slot.baking = False


def _get_session(slot: _Slot, lat: float, lon: float) -> _Session:
    with slot.cv:
        slot.record_request()
        slot.lat = lat
        slot.lon = lon

        # ── backoff gate ─────────────────────────────────────────────────────
        if slot.in_backoff:
            remaining = slot.backoff_until - time.monotonic()
            raise HTTPException(
                503, f"Region backing off for {remaining:.0f}s after repeated errors"
            )

        # ── fast path: current session alive ─────────────────────────────────
        if slot.current and slot.current.alive():
            s = slot.current
            should_bake = (
                s.age >= slot.prefetch_age
                and not slot.baking
                and slot.warm is None
                and slot.region_active      # idle gate: don't bake for silence
                and not slot.in_backoff
            )
            if should_bake:
                slot.baking = True
                threading.Thread(target=_prebake, args=(slot,), daemon=True).start()
                log.debug(
                    "pre-bake triggered age=%.1fs prefetch_age=%.1fs "
                    "idle_window=%.0fs silent=%.0fs",
                    s.age, slot.prefetch_age,
                    slot.idle_window, time.monotonic() - slot.last_req_at,
                )
            return s

        # ── warm slot ready: zero-downtime swap ──────────────────────────────
        if slot.warm and slot.warm.alive():
            slot.current = slot.warm
            slot.warm    = None
            slot.baking  = False
            log.debug("promoted warm session (age=0s)")
            return slot.current

        # ── cold path: coalesce concurrent requests ───────────────────────────
        if slot.creating:
            log.debug("waiting on in-progress cold-start")
            slot.cv.wait_for(lambda: not slot.creating, timeout=60)
            if slot.current and slot.current.alive():
                return slot.current

        slot.creating = True

    # create outside the lock — blocks ~15s
    t0 = time.monotonic()
    try:
        new_sess = _make_session(lat, lon)
        elapsed  = time.monotonic() - t0
    except Exception as exc:
        with slot.cv:
            slot.creating = False
            slot.cv.notify_all()
        raise RuntimeError(f"Session creation failed: {exc}") from exc

    with slot.cv:
        slot.current = new_sess
        slot.warm    = None
        slot.record_creation(elapsed)
        slot.creating = False
        slot.cv.notify_all()
        log.debug(
            "cold-start done %.1fs (creation_ema=%.1fs prefetch_age=%.1fs)",
            elapsed, slot.creation_ema, slot.prefetch_age,
        )
        return new_sess


def _return_session(slot: _Slot, s: _Session, discard: bool = False):
    with slot.cv:
        if slot.current is s and (discard or not s.alive()):
            slot.current = None


# ── query logic ───────────────────────────────────────────────────────────────

def _run_query(s: _Session, lat: float, lon: float, radius_km: float) -> list:
    if (abs(lat - s.lat) * 110574 > 50_000 or
            abs(lon - s.lon) * math.cos(math.radians(lat)) * 111320 > 50_000):
        s.sess.prepare_for_area(lat, lon)
        s.lat, s.lon = lat, lon

    cache: dict = {}
    for raw_box in _shrinking_boxes(lon, lat, radius_km * 1000):
        batch = s.sess.query_box(_shrink(raw_box, 0.75))
        for a in _parse_alerts(batch):
            k = a.get("uuid") or str(a.get("id"))
            if k:
                cache[k] = a
        for rid in _parse_removed_ids(batch):
            cache.pop(rid, None)
    return list(cache.values())


# ── endpoints ─────────────────────────────────────────────────────────────────

@app.get("/alerts")
def alerts(
    lat: float       = Query(21.12,   description="Latitude"),
    lon: float       = Query(-101.68, description="Longitude"),
    radius_km: float = Query(15.0,    ge=1, le=50, description="Radius km"),
    _: None          = Security(_auth),
):
    t0     = time.time()
    region = _region(lat, lon)
    slot   = _slots[region]
    s      = _get_session(slot, lat, lon)

    discard = False
    try:
        result = _run_query(s, lat, lon, radius_km)
        with slot.cv:
            slot.record_success()
    except RuntimeError as exc:
        discard = True
        with slot.cv:
            slot.record_error()
        raise HTTPException(502, str(exc))
    finally:
        _return_session(slot, s, discard=discard)

    return JSONResponse({
        "query_center":    {"lat": lat, "lon": lon},
        "query_radius_km": radius_km,
        "region":          region,
        "timestamp":       int(time.time()),
        "latency_ms":      int((time.time() - t0) * 1000),
        "alert_count":     len(result),
        "alerts":          result,
    })


@app.get("/health")
def health():
    now = time.monotonic()
    out = {}
    for r, sl in _slots.items():
        with sl.cv:
            silent = now - sl.last_req_at if sl.last_req_at else None
            out[r] = {
                "current":         "alive"   if sl.current and sl.current.alive() else "empty",
                "warm":            "ready"   if sl.warm    and sl.warm.alive()    else "empty",
                "state":           "backoff" if sl.in_backoff else ("active" if sl.region_active else "idle"),
                "creation_ema_s":  round(sl.creation_ema, 1),
                "prefetch_at_s":   round(sl.prefetch_age, 1),
                "req_interval_s":  round(sl.req_interval_ema, 1),
                "idle_window_s":   round(sl.idle_window, 1),
                "silent_for_s":    round(silent, 1) if silent is not None else None,
                "consec_errors":   sl.consec_errors,
                "backoff_left_s":  round(max(0.0, sl.backoff_until - now), 1) if sl.in_backoff else 0,
            }
    return {"status": "ok", "regions": out}
