#!/usr/bin/env python3
import logging
import math
import os
import random
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

# ── global register serializer ────────────────────────────────────────────────
# One register at a time, globally across all regions.
# Prevents burst-at-boot and desynchronizes region cycles.
_REG_INTERVAL   = float(os.environ.get("REG_INTERVAL", "6"))   # min seconds between any two registers
_reg_lock        = threading.Lock()
_reg_last        = 0.0          # monotonic timestamp of last register
_reg_429_until   = 0.0          # global pause when any region hits 429
_reg_429_lock    = threading.Lock()

# ── keeper ────────────────────────────────────────────────────────────────────
_KEEPER_TICK     = 0.5          # seconds between keeper ticks
_IDLE_DORMANT    = float(os.environ.get("IDLE_DORMANT", "120"))  # keeper goes dormant after N idle seconds

# ── flood / circuit breaker (API-side) ───────────────────────────────────────
FLOOD_RPS        = float(os.environ.get("FLOOD_RPS", "10"))
FLOOD_WINDOW     = 10.0
_CIRCUIT_BASE    = 30.0
_CIRCUIT_MAX     = 300.0

# ── error budget ──────────────────────────────────────────────────────────────
_BACKOFF_BASE    = 5.0
_BACKOFF_MAX     = 120.0
_OK_TO_RESET     = 3

# ── boot coords (used for keeper pre-warm on startup) ─────────────────────────
_BOOT_COORDS = {
    "row": (19.43, -99.13),
    "na":  (40.71, -74.01),
    "il":  (32.08,  34.78),
}


class _State(str, Enum):
    IDLE    = "idle"
    ACTIVE  = "active"
    BACKOFF = "backoff"
    FLOOD   = "flood"
    DORMANT = "dormant"   # keeper paused, no traffic, no registers


def _auth(key: str | None = Security(_key_header)):
    if not _API_KEY:
        return
    if key != _API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


# ── global register gate ──────────────────────────────────────────────────────

def _global_429(cooldown: float = 60.0):
    """Any region hits 429 → globally pause all registrations."""
    global _reg_429_until
    with _reg_429_lock:
        until = time.monotonic() + cooldown
        if until > _reg_429_until:
            _reg_429_until = until
            log.warning("[GLOBAL] 429 from Waze — all regions pausing %.0fs", cooldown)


def _register_session(lat: float, lon: float, region: str) -> "WazeSession":
    """
    Rate-limited session factory.
    Serializes ALL registers globally: one at a time, min _REG_INTERVAL apart.
    Respects global 429 cooldown before even trying.
    """
    global _reg_last

    with _reg_lock:
        now = time.monotonic()

        # wait out global 429 cooldown
        with _reg_429_lock:
            pause = _reg_429_until - now
        if pause > 0:
            log.info("[%s] global 429 cooldown — sleeping %.1fs", region, pause)
            time.sleep(pause)
            now = time.monotonic()

        # enforce minimum interval between any two registers
        gap = now - _reg_last
        if gap < _REG_INTERVAL:
            time.sleep(_REG_INTERVAL - gap)

        _reg_last = time.monotonic()

    # create outside the lock so other regions can queue without deadlock
    sess = WazeSession(lat, lon, debug=False)
    try:
        sess.register(lat, lon)
    except RuntimeError as exc:
        if "429" in str(exc):
            _global_429(cooldown=90.0)
        raise
    sess.login(lat, lon)
    sess.prepare_for_area(lat, lon)
    return sess


# ── session model ─────────────────────────────────────────────────────────────

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


# ── per-region slot ───────────────────────────────────────────────────────────

class _Slot:
    def __init__(self, region: str):
        self.region          = region
        self.cv              = threading.Condition(threading.Lock())

        # session slots
        self.current         : _Session | None = None
        self.warm            : _Session | None = None
        self.baking          : bool  = False
        self.keeper_started  : bool  = False
        self.dormant         : bool  = False   # keeper paused — no traffic

        # coords for prebake (updated on each request)
        self.lat             : float = _BOOT_COORDS[region][0]
        self.lon             : float = _BOOT_COORDS[region][1]

        # adaptive creation EMA + per-region jitter (desync region cycles)
        self.creation_ema    : float = 15.0
        self._jitter         : float = random.uniform(0, 3.0)

        # traffic tracking
        self.req_timestamps  : deque = deque()
        self.req_interval_ema: float = 60.0
        self.last_req_at     : float = 0.0

        # error budget
        self.consec_errors   : int   = 0
        self.consec_ok       : int   = 0
        self.backoff_until   : float = 0.0

        # flood / circuit breaker
        self.circuit_trips   : int   = 0
        self.circuit_until   : float = 0.0

    @property
    def prefetch_age(self) -> float:
        """Session age at which to pre-bake next, with jitter to desync regions."""
        return max(0.5, SESSION_TTL - self.creation_ema - _SAFETY_MARGIN) + self._jitter

    @property
    def in_backoff(self) -> bool:
        return time.monotonic() < self.backoff_until

    @property
    def circuit_open(self) -> bool:
        return time.monotonic() < self.circuit_until

    @property
    def req_rate(self) -> float:
        now = time.monotonic()
        while self.req_timestamps and now - self.req_timestamps[0] > FLOOD_WINDOW:
            self.req_timestamps.popleft()
        return len(self.req_timestamps) / FLOOD_WINDOW

    @property
    def idle_seconds(self) -> float | None:
        return (time.monotonic() - self.last_req_at) if self.last_req_at else None

    @property
    def state(self) -> _State:
        if self.circuit_open:  return _State.FLOOD
        if self.in_backoff:    return _State.BACKOFF
        if self.dormant:       return _State.DORMANT
        if self.last_req_at and time.monotonic() - self.last_req_at < 120:
            return _State.ACTIVE
        return _State.IDLE

    def record_request(self):
        now = time.monotonic()
        self.req_timestamps.append(now)
        if self.last_req_at:
            iv = now - self.last_req_at
            self.req_interval_ema = _EMA_ALPHA * iv + (1 - _EMA_ALPHA) * self.req_interval_ema
        self.last_req_at = now
        # wake dormant keeper
        self.cv.notify_all()

        # flood detection
        rate = self.req_rate
        if rate > FLOOD_RPS and not self.circuit_open:
            self.circuit_trips += 1
            cooldown = min(_CIRCUIT_BASE * (2 ** (self.circuit_trips - 1)), _CIRCUIT_MAX)
            self.circuit_until = now + cooldown
            log.warning("[%s] FLOOD %.1f req/s — circuit open %.0fs (trip #%d)",
                        self.region, rate, cooldown, self.circuit_trips)

    def record_creation(self, elapsed: float):
        self.creation_ema = _EMA_ALPHA * elapsed + (1 - _EMA_ALPHA) * self.creation_ema

    def record_success(self):
        self.consec_ok    += 1
        self.consec_errors = 0
        if self.consec_ok >= _OK_TO_RESET:
            self.backoff_until = 0.0
            self.circuit_trips = max(0, self.circuit_trips - 1)

    def record_error(self):
        self.consec_ok     = 0
        self.consec_errors += 1
        delay = min(_BACKOFF_BASE * (2 ** (self.consec_errors - 1)), _BACKOFF_MAX)
        self.backoff_until = time.monotonic() + delay
        log.warning("[%s] Waze error #%d — backoff %.0fs", self.region, self.consec_errors, delay)


_slots = {r: _Slot(r) for r in ("row", "na", "il")}


# ── background threads ────────────────────────────────────────────────────────

def _prebake(slot: _Slot):
    """Background: create next session via global rate-limited gate."""
    with slot.cv:
        lat, lon = slot.lat, slot.lon

    t0 = time.monotonic()
    try:
        raw  = _register_session(lat, lon, slot.region)
        sess = _Session(raw, lat, lon)
        elapsed = time.monotonic() - t0
        with slot.cv:
            slot.warm = sess
            slot.record_creation(elapsed)
            # promote immediately if current dead
            if not (slot.current and slot.current.alive()):
                slot.current = slot.warm
                slot.warm    = None
            slot.cv.notify_all()
            log.debug("[%s] prebake done %.1fs (ema=%.1fs jitter=%.1fs)",
                      slot.region, elapsed, slot.creation_ema, slot._jitter)
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
    Perpetual keeper per region.

    Smart idle: if region has been silent > IDLE_DORMANT seconds AND current
    session is dead, keeper goes dormant (no more registers). Wakes on next
    incoming request. This prevents registering forever for unused regions.
    """
    while True:
        time.sleep(_KEEPER_TICK)
        with slot.cv:
            now = time.monotonic()

            # ── circuit open: evict and do nothing ───────────────────────────
            if slot.circuit_open:
                if slot.current or slot.warm:
                    slot.current = None
                    slot.warm    = None
                    slot.baking  = False
                    slot.cv.notify_all()
                continue

            # ── Waze backoff: don't create sessions ──────────────────────────
            if slot.in_backoff:
                continue

            # ── dormant check: no traffic + no live session → sleep ──────────
            idle = slot.idle_seconds
            current_dead = not (slot.current and slot.current.alive())
            if current_dead and idle is not None and idle > _IDLE_DORMANT:
                if not slot.dormant:
                    slot.dormant = True
                    log.info("[%s] keeper dormant after %.0fs idle", slot.region, idle)
                # wait for a request to arrive (record_request notifies)
                slot.cv.wait_for(
                    lambda: (slot.last_req_at and
                             time.monotonic() - slot.last_req_at < _IDLE_DORMANT),
                    timeout=3600,
                )
                slot.dormant = False
                log.info("[%s] keeper waking — traffic resumed", slot.region)
                continue

            slot.dormant = False

            # ── promote warm → current if current dead ────────────────────────
            if current_dead and slot.warm and slot.warm.alive():
                slot.current = slot.warm
                slot.warm    = None
                slot.baking  = False
                slot.cv.notify_all()
                log.debug("[%s] keeper promoted warm session", slot.region)
                current_dead = False

            s = slot.current
            alive = s and s.alive()

            # ── trigger prebake ───────────────────────────────────────────────
            needs_bake = (
                not slot.baking
                and slot.warm is None
                and (
                    not alive                       # emergency: nothing alive
                    or s.age >= slot.prefetch_age   # normal: session aging out
                )
            )
            if needs_bake:
                slot.baking = True
                reason = "emergency" if not alive else f"age={s.age:.1f}s≥{slot.prefetch_age:.1f}s"
                log.debug("[%s] keeper firing prebake (%s)", slot.region, reason)
                threading.Thread(target=_prebake, args=(slot,), daemon=True).start()


# ── session acquisition ───────────────────────────────────────────────────────

def _get_session(slot: _Slot, lat: float, lon: float) -> _Session:
    with slot.cv:
        slot.record_request()
        slot.lat = lat
        slot.lon = lon

        if slot.circuit_open:
            left = slot.circuit_until - time.monotonic()
            raise HTTPException(429, f"[{slot.region}] Flood — retry in {left:.0f}s")

        if slot.in_backoff:
            left = slot.backoff_until - time.monotonic()
            raise HTTPException(503, f"[{slot.region}] Waze errors — retry in {left:.0f}s")

        if slot.current and slot.current.alive():
            return slot.current

        if slot.keeper_started:
            # keeper will handle creation — wait for it
            ok = slot.cv.wait_for(
                lambda: (
                    (slot.current and slot.current.alive())
                    or slot.circuit_open
                    or slot.in_backoff
                ),
                timeout=35,
            )
            if slot.circuit_open:
                raise HTTPException(429, f"[{slot.region}] Flood mid-wait")
            if slot.in_backoff:
                raise HTTPException(503, f"[{slot.region}] Waze error mid-wait")
            if slot.current and slot.current.alive():
                return slot.current
            raise HTTPException(503, f"[{slot.region}] No session after 35s")

        # first ever request: cold-start inline, then hand to keeper
        slot.baking = True

    t0 = time.monotonic()
    try:
        raw  = _register_session(lat, lon, slot.region)
        sess = _Session(raw, lat, lon)
        elapsed = time.monotonic() - t0
    except Exception as exc:
        with slot.cv:
            slot.baking = False
            slot.record_error()
            slot.cv.notify_all()
        raise RuntimeError(f"Session creation failed: {exc}") from exc

    with slot.cv:
        slot.current = sess
        slot.warm    = None
        slot.baking  = False
        slot.record_creation(elapsed)
        slot.cv.notify_all()
        log.info("[%s] cold-start %.1fs → keeper starting", slot.region, elapsed)
        if not slot.keeper_started:
            slot.keeper_started = True
            threading.Thread(target=_region_keeper, args=(slot,), daemon=True).start()
        return sess


def _return_session(slot: _Slot, s: _Session, discard: bool = False):
    with slot.cv:
        if slot.current is s and (discard or not s.alive()):
            slot.current = None


# ── query ─────────────────────────────────────────────────────────────────────

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


# ── lifespan: staggered boot pre-warm ────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Keepers start sequentially — the global register serializer enforces
    # _REG_INTERVAL between each registration automatically, so no burst.
    for region, slot in _slots.items():
        with slot.cv:
            slot.keeper_started = True
        threading.Thread(target=_region_keeper, args=(slot,), daemon=True).start()
        log.info("[%s] keeper started at boot", region)
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
            out[r] = {
                "state":           sl.state,
                "current":         "alive" if sl.current and sl.current.alive() else "empty",
                "warm":            "ready" if sl.warm    and sl.warm.alive()    else "empty",
                "creation_ema_s":  round(sl.creation_ema, 1),
                "prefetch_at_s":   round(sl.prefetch_age, 1),
                "jitter_s":        round(sl._jitter, 1),
                "req_rate":        round(sl.req_rate, 2),
                "flood_limit":     FLOOD_RPS,
                "req_interval_s":  round(sl.req_interval_ema, 1),
                "silent_for_s":    round(sl.idle_seconds, 1) if sl.idle_seconds else None,
                "dormant":         sl.dormant,
                "circuit_trips":   sl.circuit_trips,
                "circuit_left_s":  round(max(0, sl.circuit_until - now), 1) if sl.circuit_open else 0,
                "backoff_left_s":  round(max(0, sl.backoff_until - now), 1) if sl.in_backoff else 0,
                "consec_errors":   sl.consec_errors,
            }
    global_429_left = max(0, _reg_429_until - now)
    return {
        "status":           "ok",
        "reg_interval_s":   _REG_INTERVAL,
        "global_429_left_s": round(global_429_left, 1),
        "regions":          out,
    }
