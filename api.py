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

SESSION_TTL    = 25.0  # server kills anonymous sessions at ~30s
_EMA_ALPHA     = 0.3   # weight for new creation-time samples
_SAFETY_MARGIN = 2.0   # seconds of buffer on top of EMA


def _auth(key: str | None = Security(_key_header)):
    if not _API_KEY:
        return
    if key != _API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


class _Session:
    def __init__(self, sess: WazeSession, lat: float, lon: float):
        self.sess = sess
        self.lat  = lat
        self.lon  = lon
        self._born = time.monotonic()

    def alive(self) -> bool:
        return self.age < SESSION_TTL

    @property
    def age(self) -> float:
        return time.monotonic() - self._born


class _Slot:
    """
    Per-region state machine:
      current  → live session serving requests
      warm     → pre-baked next session ready to swap in
      creating → inline cold-start in progress (other threads wait)
      baking   → background pre-bake in progress
      ema      → exponential moving average of creation time (adapts prefetch_age)
    """
    def __init__(self):
        self.cv       = threading.Condition(threading.Lock())
        self.current  : _Session | None = None
        self.warm     : _Session | None = None
        self.creating : bool  = False
        self.baking   : bool  = False
        self.lat      : float = 0.0
        self.lon      : float = 0.0
        self.ema      : float = 15.0  # initial estimate; adapts on real data

    @property
    def prefetch_age(self) -> float:
        """Session age at which to start pre-baking the next one."""
        return max(0.5, SESSION_TTL - self.ema - _SAFETY_MARGIN)

    def _update_ema(self, elapsed: float):
        self.ema = _EMA_ALPHA * elapsed + (1 - _EMA_ALPHA) * self.ema


_slots = {r: _Slot() for r in ("row", "na", "il")}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="Waze RT API", lifespan=lifespan)


def _make_session(lat: float, lon: float) -> _Session:
    sess = WazeSession(lat, lon, debug=False)
    sess.register(lat, lon)
    sess.login(lat, lon)
    sess.prepare_for_area(lat, lon)
    return _Session(sess, lat, lon)


def _prebake(slot: _Slot):
    """Background thread: bake next session using last known coords."""
    with slot.cv:
        lat, lon = slot.lat, slot.lon

    t0 = time.monotonic()
    try:
        warm = _make_session(lat, lon)
        elapsed = time.monotonic() - t0
        with slot.cv:
            slot.warm = warm
            slot._update_ema(elapsed)
            log.debug("pre-bake ready in %.1fs (EMA=%.1fs)", elapsed, slot.ema)
    except Exception:
        log.exception("pre-bake failed")
    finally:
        with slot.cv:
            slot.baking = False


def _get_session(slot: _Slot, lat: float, lon: float) -> _Session:
    with slot.cv:
        slot.lat = lat
        slot.lon = lon

        # fast path: current session still alive
        if slot.current and slot.current.alive():
            s = slot.current
            # trigger pre-bake if old enough and nothing already warming
            if s.age >= slot.prefetch_age and not slot.baking and slot.warm is None:
                slot.baking = True
                threading.Thread(target=_prebake, args=(slot,), daemon=True).start()
                log.debug("triggered pre-bake at age=%.1fs prefetch_age=%.1fs", s.age, slot.prefetch_age)
            return s

        # warm session ready — zero-downtime swap
        if slot.warm and slot.warm.alive():
            slot.current = slot.warm
            slot.warm    = None
            slot.baking  = False
            log.debug("promoted warm session")
            return slot.current

        # another thread is already creating — wait for it
        if slot.creating:
            log.debug("waiting for in-progress cold-start")
            slot.cv.wait_for(lambda: not slot.creating, timeout=60)
            if slot.current and slot.current.alive():
                return slot.current
            # fall through if timed out or failed

        slot.creating = True

    # cold path: create outside the lock (blocks ~15s)
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
        slot._update_ema(elapsed)
        slot.creating = False
        slot.cv.notify_all()
        log.debug("cold-start done in %.1fs (EMA=%.1fs prefetch_age=%.1fs)",
                  elapsed, slot.ema, slot.prefetch_age)
        return new_sess


def _return_session(slot: _Slot, s: _Session, discard: bool = False):
    with slot.cv:
        if slot.current is s and (discard or not s.alive()):
            slot.current = None


def _run_query(s: _Session, lat: float, lon: float, radius_km: float) -> list:
    if abs(lat - s.lat) * 110574 > 50_000 or \
       abs(lon - s.lon) * math.cos(math.radians(lat)) * 111320 > 50_000:
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
    except RuntimeError as exc:
        discard = True
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
    return {
        "status": "ok",
        "sessions": {
            r: {
                "current":      "alive" if (sl := _slots[r]).current and sl.current.alive() else "empty",
                "warm":         "ready" if sl.warm and sl.warm.alive() else "empty",
                "ema_s":        round(sl.ema, 1),
                "prefetch_at_s": round(sl.prefetch_age, 1),
            }
            for r in _slots
        },
    }
