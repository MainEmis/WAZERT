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

SESSION_TTL = 25  # server kills anonymous sessions at ~30s


def _auth(key: str | None = Security(_key_header)):
    if not _API_KEY:
        return
    if key != _API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


class _Session:
    def __init__(self, sess: WazeSession, lat: float, lon: float):
        self.sess    = sess
        self.lat     = lat
        self.lon     = lon
        self.born    = time.time()

    def alive(self) -> bool:
        return time.time() - self.born < SESSION_TTL


# one slot per region — on-demand, no background thread
_slots: dict[str, "_Session | None"] = {}
_locks: dict[str, threading.Lock]    = {}

for _r in ("row", "na", "il"):
    _slots[_r] = None
    _locks[_r] = threading.Lock()


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


def _get_session(region: str, lat: float, lon: float) -> _Session:
    with _locks[region]:
        s = _slots[region]
        if s and s.alive():
            return s
        s = _make_session(lat, lon)
        _slots[region] = s
        return s


def _return_session(region: str, s: _Session, discard: bool = False):
    with _locks[region]:
        if discard or not s.alive():
            if _slots[region] is s:
                _slots[region] = None
        else:
            _slots[region] = s


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
    s      = _get_session(region, lat, lon)

    discard = False
    try:
        result = _run_query(s, lat, lon, radius_km)
    except RuntimeError as exc:
        discard = True
        raise HTTPException(502, str(exc))
    finally:
        _return_session(region, s, discard=discard)

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
        "sessions": {r: ("alive" if s and s.alive() else "empty")
                     for r, s in _slots.items()},
    }
