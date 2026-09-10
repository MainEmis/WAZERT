#!/usr/bin/env python3
"""
Waze RT — FastAPI, fresh session per request.

Sessions are anonymous and expire server-side in ~30s,
so pooling is unreliable. Each request registers+logins fresh.

GET /alerts?lat=21.12&lon=-101.68&radius_km=15
GET /health
"""

import logging
import math
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
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

_API_KEY   = os.environ.get("API_KEY", "")
_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_pool      = ThreadPoolExecutor(max_workers=10)


def _auth(key: str | None = Security(_key_header)):
    if not _API_KEY:
        return
    if key != _API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    _pool.shutdown(wait=False)


app = FastAPI(title="Waze RT API", lifespan=lifespan)


def _fetch(lat: float, lon: float, radius_km: float) -> list:
    """Fresh register→login→handshake→query. Called in threadpool."""
    sess = WazeSession(lat, lon, debug=False)
    sess.register(lat, lon)
    sess.login(lat, lon)
    sess.prepare_for_area(lat, lon)

    alert_cache: dict = {}
    for raw_box in _shrinking_boxes(lon, lat, radius_km * 1000):
        box   = _shrink(raw_box, 0.75)
        batch = sess.query_box(box)
        for a in _parse_alerts(batch):
            k = a.get("uuid") or str(a.get("id"))
            if k:
                alert_cache[k] = a
        for rid in _parse_removed_ids(batch):
            alert_cache.pop(rid, None)

    return list(alert_cache.values())


@app.get("/alerts")
def alerts(
    lat: float       = Query(21.12,   description="Latitude"),
    lon: float       = Query(-101.68, description="Longitude"),
    radius_km: float = Query(15.0,    ge=1, le=50, description="Radius km"),
    _: None          = Security(_auth),
):
    t0     = time.time()
    region = _region(lat, lon)
    fut    = _pool.submit(_fetch, lat, lon, radius_km)
    try:
        result = fut.result(timeout=60)
    except FuturesTimeout:
        raise HTTPException(503, "RT query timed out")
    except Exception as exc:
        raise HTTPException(502, str(exc))

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
    return {"status": "ok"}
