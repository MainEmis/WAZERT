#!/usr/bin/env python3
"""
Waze RT — FastAPI with smart session pool.

Anonymous sessions expire server-side in ~30s.
Pool rotates sessions every 25s so every request
gets a session that's guaranteed still alive.

GET /alerts?lat=21.12&lon=-101.68&radius_km=15
GET /health
"""

import logging
import math
import os
import queue
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

POOL_SIZE_PER_REGION = {"row": 2, "na": 1, "il": 0}
SESSION_TTL          = 25   # server expires anonymous sessions at ~30s
SESSION_MAX_USES     = 5
REFILL_BACKOFF       = [5, 15, 30, 60]


def _auth(key: str | None = Security(_key_header)):
    if not _API_KEY:
        return
    if key != _API_KEY:
        raise HTTPException(401, "Invalid or missing API key")


class _PooledSession:
    def __init__(self, sess: WazeSession, lat: float, lon: float):
        self.sess     = sess
        self.lat      = lat
        self.lon      = lon
        self.created  = time.time()
        self.uses     = 0

    def expired(self) -> bool:
        return (time.time() - self.created >= SESSION_TTL
                or self.uses >= SESSION_MAX_USES)


class RegionPool:
    def __init__(self, region: str, size: int):
        self._region = region
        self._size   = size
        self._pool   = queue.Queue()
        self._stop   = threading.Event()
        self._filler = threading.Thread(
            target=self._fill_loop, daemon=True, name=f"pool-{region}"
        )
        self._filler.start()

    def _seed(self):
        seeds = {"row": (21.12, -101.68), "na": (40.71, -74.00), "il": (31.77, 35.21)}
        return seeds[self._region]

    def _create(self) -> _PooledSession:
        lat, lon = self._seed()
        sess = WazeSession(lat, lon, debug=False)
        sess.register(lat, lon)
        sess.login(lat, lon)
        sess.prepare_for_area(lat, lon)
        return _PooledSession(sess, lat, lon)

    def _fill_loop(self):
        backoff_idx = 0
        while not self._stop.is_set():
            # drain expired sessions sitting in the queue
            fresh = []
            while True:
                try:
                    ps = self._pool.get_nowait()
                    if not ps.expired():
                        fresh.append(ps)
                except queue.Empty:
                    break
            for ps in fresh:
                self._pool.put(ps)

            need = self._size - self._pool.qsize()
            for _ in range(need):
                try:
                    ps = self._create()
                    self._pool.put(ps)
                    log.info("[%s] session ready (age=0s), pool=%d/%d",
                             self._region, self._pool.qsize(), self._size)
                    backoff_idx = 0
                except Exception as exc:
                    delay = REFILL_BACKOFF[min(backoff_idx, len(REFILL_BACKOFF)-1)]
                    log.warning("[%s] create failed (%s) — retry in %ds",
                                self._region, exc, delay)
                    backoff_idx += 1
                    self._stop.wait(delay)
                    break

            self._stop.wait(2)  # check every 2s

    def acquire(self, timeout: float = 30) -> _PooledSession:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                ps = self._pool.get(timeout=1)
                if not ps.expired():
                    return ps
                # expired — discard silently, filler will replace
            except queue.Empty:
                pass
        raise queue.Empty("No fresh session available")

    def release(self, ps: _PooledSession, discard: bool = False):
        if not discard and not ps.expired():
            self._pool.put(ps)

    def shutdown(self):
        self._stop.set()

    @property
    def size(self) -> int:
        return self._pool.qsize()


_pools: dict[str, RegionPool] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    for region, size in POOL_SIZE_PER_REGION.items():
        if size > 0:
            _pools[region] = RegionPool(region, size)
            log.info("Pool [%s] started (target=%d, TTL=%ds)", region, size, SESSION_TTL)
    yield
    for p in _pools.values():
        p.shutdown()


app = FastAPI(title="Waze RT API", lifespan=lifespan)


def _query(ps: _PooledSession, lat: float, lon: float, radius_km: float) -> list:
    sess = ps.sess
    if abs(lat - ps.lat) * 110574 > 50_000 or \
       abs(lon - ps.lon) * math.cos(math.radians(lat)) * 111320 > 50_000:
        sess.prepare_for_area(lat, lon)
        ps.lat, ps.lon = lat, lon

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
    pool   = _pools.get(region)

    # fallback: no pool for this region → fresh session inline
    if pool is None:
        from waze_client import WazeSession as WS
        ps = _PooledSession(WS(lat, lon), lat, lon)
        ps.sess.register(lat, lon)
        ps.sess.login(lat, lon)
        ps.sess.prepare_for_area(lat, lon)
    else:
        try:
            ps = pool.acquire(timeout=30)
        except queue.Empty:
            raise HTTPException(503, f"[{region}] No fresh sessions — pool warming up")

    discard = False
    try:
        ps.uses += 1
        result = _query(ps, lat, lon, radius_km)
    except RuntimeError as exc:
        discard = True
        raise HTTPException(502, str(exc))
    finally:
        if pool:
            pool.release(ps, discard=discard)

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
        "pools":  {r: {"ready": p.size, "target": POOL_SIZE_PER_REGION[r], "ttl": SESSION_TTL}
                   for r, p in _pools.items()},
    }
