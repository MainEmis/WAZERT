#!/usr/bin/env python3
import logging
import math
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from enum import Enum

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
SESSION_TTL    = 25.0
_EMA_ALPHA     = 0.3
_SAFETY_MARGIN = 2.0

# ── keeper ────────────────────────────────────────────────────────────────────
_KEEPER_INTERVAL = 0.5   # seconds between keeper ticks

# ── flood / circuit breaker ───────────────────────────────────────────────────
FLOOD_RPS      = float(os.environ.get("FLOOD_RPS", "10"))  # req/s → circuit opens
FLOOD_WINDOW   = 10.0   # sliding window (s)
_CIRCUIT_BASE  = 30.0   # first cooldown after flood detected (s)
_CIRCUIT_MAX   = 300.0  # max cooldown (s)

# ── error / backoff ───────────────────────────────────────────────────────────
_BACKOFF_BASE = 5.0
_BACKOFF_MAX  = 120.0
_OK_TO_RESET  = 3       # consecutive successes to clear backoff

# ── boot coords (pre-warm at startup) ────────────────────────────────────────
_BOOT_COORDS = {
    "row": (19.43, -99.13),   # CDMX — representative ROW city
    "na":  (40.71, -74.01),   # Nueva York
    "il":  (32.08,  34.78),   # Tel Aviv
}


class _State(str, Enum):
    IDLE    = "idle"
    ACTIVE  = "active"
    BACKOFF = "backoff"
    FLOOD   = "flood"   # circuit open — all requests → 429


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
    Per-region state machine with circuit breaker, adaptive pre-bake,
    flood detection, and error budget.

    Signals:
      creation_ema      — real cost of register→login→handshake (adapts prefetch_age)
      req_timestamps    — sliding window for req/s measurement (drives flood gate)
      req_interval_ema  — EMA of inter-request gap (informational, shown in health)
      consec_errors     — consecutive Waze failures (drives exponential backoff)
      circuit_trips     — how many times flood tripped (doubles cooldown)
    """

    def __init__(self, region: str):
        self.region          = region
        self.cv              = threading.Condition(threading.Lock())

        # session slots
        self.current         : _Session | None = None
        self.warm            : _Session | None = None
        self.baking          : bool  = False
        self.keeper_started  : bool  = False

        # last known coords for keeper/prebake
        self.lat             : float = _BOOT_COORDS[region][0]
        self.lon             : float = _BOOT_COORDS[region][1]

        # adaptive creation EMA → prefetch_age
        self.creation_ema    : float = 15.0

        # traffic rate (flood detection)
        self.req_timestamps  : deque = deque()   # monotonic timestamps
        self.req_interval_ema: float = 60.0      # seconds between requests (informational)
        self.last_req_at     : float = 0.0

        # error budget → backoff
        self.consec_errors   : int   = 0
        self.consec_ok       : int   = 0
        self.backoff_until   : float = 0.0

        # circuit breaker → flood
        self.circuit_trips   : int   = 0
        self.circuit_until   : float = 0.0

    # ── derived thresholds ────────────────────────────────────────────────────

    @property
    def prefetch_age(self) -> float:
        return max(0.5, SESSION_TTL - self.creation_ema - _SAFETY_MARGIN)

    @property
    def in_backoff(self) -> bool:
        return time.monotonic() < self.backoff_until

    @property
    def circuit_open(self) -> bool:
        return time.monotonic() < self.circuit_until

    @property
    def req_rate(self) -> float:
        """Requests/s measured over the last FLOOD_WINDOW seconds."""
        now = time.monotonic()
        while self.req_timestamps and now - self.req_timestamps[0] > FLOOD_WINDOW:
            self.req_timestamps.popleft()
        return len(self.req_timestamps) / FLOOD_WINDOW

    @property
    def state(self) -> _State:
        if self.circuit_open:
            return _State.FLOOD
        if self.in_backoff:
            return _State.BACKOFF
        if self.last_req_at and time.monotonic() - self.last_req_at < 120:
            return _State.ACTIVE
        return _State.IDLE

    # ── signal recorders ─────────────────────────────────────────────────────

    def record_request(self):
        """Call on every incoming request. Updates traffic signals."""
        now = time.monotonic()
        self.req_timestamps.append(now)
        if self.last_req_at:
            interval = now - self.last_req_at
            self.req_interval_ema = _EMA_ALPHA * interval + (1 - _EMA_ALPHA) * self.req_interval_ema
        self.last_req_at = now

        # flood detection — circuit breaker
        rate = self.req_rate
        if rate > FLOOD_RPS and not self.circuit_open:
            self.circuit_trips += 1
            cooldown = min(_CIRCUIT_BASE * (2 ** (self.circuit_trips - 1)), _CIRCUIT_MAX)
            self.circuit_until = now + cooldown
            log.warning(
                "[%s] FLOOD: %.1f req/s > %.0f — circuit open for %.0fs (trip #%d)",
                self.region, rate, FLOOD_RPS, cooldown, self.circuit_trips,
            )

    def record_creation(self, elapsed: float):
        self.creation_ema = _EMA_ALPHA * elapsed + (1 - _EMA_ALPHA) * self.creation_ema

    def record_success(self):
        self.consec_ok    += 1
        self.consec_errors = 0
        if self.consec_ok >= _OK_TO_RESET:
            self.backoff_until = 0.0
            self.circuit_trips = max(0, self.circuit_trips - 1)  # heal over time

    def record_error(self):
        self.consec_ok     = 0
        self.consec_errors += 1
        delay = min(_BACKOFF_BASE * (2 ** (self.consec_errors - 1)), _BACKOFF_MAX)
        self.backoff_until = time.monotonic() + delay
        log.warning("[%s] Waze error #%d — backoff %.0fs", self.region, self.consec_errors, delay)


_slots = {r: _Slot(r) for r in ("row", "na", "il")}


# ── background threads ────────────────────────────────────────────────────────

def _prebake(slot: _Slot):
    """Background: create next session and slot it as warm (or current if dead)."""
    with slot.cv:
        lat, lon = slot.lat, slot.lon

    t0 = time.monotonic()
    try:
        warm    = _make_session(lat, lon)
        elapsed = time.monotonic() - t0
        with slot.cv:
            slot.warm = warm
            slot.record_creation(elapsed)
            # promote immediately if current is dead (keeper doesn't need to wait)
            if not (slot.current and slot.current.alive()):
                slot.current = slot.warm
                slot.warm    = None
            slot.cv.notify_all()
            log.debug("[%s] prebake done %.1fs (ema=%.1fs)", slot.region, elapsed, slot.creation_ema)
    except Exception:
        log.exception("[%s] prebake failed", slot.region)
        with slot.cv:
            slot.record_error()
    finally:
        with slot.cv:
            slot.baking = False
            slot.cv.notify_all()


def _region_keeper(slot: _Slot):
    """
    Perpetual background thread per region.
    Maintains the current/warm cycle independently of incoming traffic.
    Respects circuit breaker and backoff — won't create sessions if Waze is angry.
    """
    while True:
        time.sleep(_KEEPER_INTERVAL)
        with slot.cv:
            # skip all creation if circuit open or backing off
            if slot.circuit_open or slot.in_backoff:
                # if circuit just opened, evict current session to stop serving stale data
                if slot.circuit_open and slot.current:
                    log.warning("[%s] circuit open — evicting current session", slot.region)
                    slot.current = None
                    slot.warm    = None
                    slot.baking  = False
                continue

            # promote warm → current if current dead
            if not (slot.current and slot.current.alive()):
                if slot.warm and slot.warm.alive():
                    slot.current = slot.warm
                    slot.warm    = None
                    slot.baking  = False
                    slot.cv.notify_all()
                    log.debug("[%s] keeper promoted warm session", slot.region)

            s = slot.current
            alive = s and s.alive()

            needs_bake = (
                not slot.baking
                and slot.warm is None
                and (
                    not alive                        # emergency: no session
                    or s.age >= slot.prefetch_age    # normal: getting old, bake next
                )
            )
            if needs_bake:
                slot.baking = True
                reason = "emergency" if not alive else f"age={s.age:.1f}s"
                log.debug("[%s] keeper baking (%s, prefetch_age=%.1fs)", slot.region, reason, slot.prefetch_age)
                threading.Thread(target=_prebake, args=(slot,), daemon=True).start()


# ── session acquisition ───────────────────────────────────────────────────────

def _get_session(slot: _Slot, lat: float, lon: float) -> _Session:
    with slot.cv:
        slot.record_request()
        slot.lat = lat
        slot.lon = lon

        # ── circuit breaker: flood → 429 ─────────────────────────────────────
        if slot.circuit_open:
            remaining = slot.circuit_until - time.monotonic()
            raise HTTPException(
                429, f"[{slot.region}] Flood protection active — retry in {remaining:.0f}s"
            )

        # ── Waze backoff → 503 ───────────────────────────────────────────────
        if slot.in_backoff:
            remaining = slot.backoff_until - time.monotonic()
            raise HTTPException(
                503, f"[{slot.region}] Backing off Waze errors — retry in {remaining:.0f}s"
            )

        # ── fast path: current alive ──────────────────────────────────────────
        if slot.current and slot.current.alive():
            return slot.current

        # ── keeper is running: wait for it to provide a session ───────────────
        if slot.keeper_started:
            ready = slot.cv.wait_for(
                lambda: (
                    (slot.current and slot.current.alive())
                    or slot.circuit_open
                    or slot.in_backoff
                ),
                timeout=35,
            )
            if slot.circuit_open:
                raise HTTPException(429, f"[{slot.region}] Flood detected mid-wait")
            if slot.in_backoff:
                raise HTTPException(503, f"[{slot.region}] Waze error mid-wait")
            if slot.current and slot.current.alive():
                return slot.current
            raise HTTPException(503, f"[{slot.region}] Session unavailable after 35s wait")

        # ── first ever request: cold start inline, then hand off to keeper ────
        # (only happens once per region per process lifetime)
        slot.baking = True  # prevent keeper from double-creating

    t0 = time.monotonic()
    try:
        new_sess = _make_session(lat, lon)
        elapsed  = time.monotonic() - t0
    except Exception as exc:
        with slot.cv:
            slot.baking = False
            slot.record_error()
            slot.cv.notify_all()
        raise RuntimeError(f"Session creation failed: {exc}") from exc

    with slot.cv:
        slot.current = new_sess
        slot.warm    = None
        slot.baking  = False
        slot.record_creation(elapsed)
        slot.cv.notify_all()
        log.info("[%s] cold-start done %.1fs → keeper starting", slot.region, elapsed)
        if not slot.keeper_started:
            slot.keeper_started = True
            threading.Thread(target=_region_keeper, args=(slot,), daemon=True).start()
        return new_sess


def _return_session(slot: _Slot, s: _Session, discard: bool = False):
    with slot.cv:
        if slot.current is s and (discard or not s.alive()):
            slot.current = None


# ── query ─────────────────────────────────────────────────────────────────────

def _make_session(lat: float, lon: float) -> _Session:
    sess = WazeSession(lat, lon, debug=False)
    sess.register(lat, lon)
    sess.login(lat, lon)
    sess.prepare_for_area(lat, lon)
    return _Session(sess, lat, lon)


def _run_query(s: _Session, lat: float, lon: float, radius_km: float) -> list:
    if (abs(lat - s.lat) * 110574 > 50_000
            or abs(lon - s.lon) * math.cos(math.radians(lat)) * 111320 > 50_000):
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


# ── lifespan: pre-warm all regions at boot ────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    for region, slot in _slots.items():
        with slot.cv:
            slot.keeper_started = True
        t = threading.Thread(target=_region_keeper, args=(slot,), daemon=True)
        t.start()
        log.info("[%s] keeper started at boot (pre-warming at %.4f, %.4f)", region, slot.lat, slot.lon)
    yield


app = FastAPI(title="Waze RT API", lifespan=lifespan)


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
            silent  = round(now - sl.last_req_at, 1) if sl.last_req_at else None
            cb_left = round(max(0.0, sl.circuit_until - now), 1) if sl.circuit_open else 0
            bo_left = round(max(0.0, sl.backoff_until - now), 1) if sl.in_backoff else 0
            out[r]  = {
                "state":           sl.state,
                "current":         "alive" if sl.current and sl.current.alive() else "empty",
                "warm":            "ready" if sl.warm    and sl.warm.alive()    else "empty",
                "creation_ema_s":  round(sl.creation_ema, 1),
                "prefetch_at_s":   round(sl.prefetch_age, 1),
                "req_rate":        round(sl.req_rate, 2),
                "flood_rps_limit": FLOOD_RPS,
                "req_interval_s":  round(sl.req_interval_ema, 1),
                "silent_for_s":    silent,
                "circuit_trips":   sl.circuit_trips,
                "circuit_left_s":  cb_left,
                "backoff_left_s":  bo_left,
                "consec_errors":   sl.consec_errors,
            }
    return {"status": "ok", "regions": out}
