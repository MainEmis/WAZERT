#!/usr/bin/env python3
"""
Waze RT — FastAPI with smart session pool.

Sessions are registered once and reused across many requests.
A background thread keeps the pool at target size and recycles
sessions that expire (server relogin) or fail.

GET /alerts?lat=21.12&lon=-101.68&radius_km=15
GET /health
"""

import logging
import math
import queue
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
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

# Per-region seed coords used only to create sessions on the right server.
_REGION_SEED = {
    "row": (21.12,  -101.68),   # León MX → rt-xlb-row.waze.com
    "na":  (40.71,  -74.00),    # NYC    → rt-xlb-am.waze.com
    "il":  (31.77,   35.21),    # Jerusalem → rt-xlb-il.waze.com
}

POOL_SIZE_PER_REGION = {"row": 3, "na": 2, "il": 1}
SESSION_TTL      = 600
SESSION_MAX_USES = 30
REFILL_BACKOFF   = [5, 15, 30, 60]


class _PooledSession:
    def __init__(self, sess: WazeSession):
        self.sess     = sess
        self.created  = time.time()
        self.uses     = 0
        self.last_lat: float | None = None
        self.last_lon: float | None = None

    def expired(self) -> bool:
        return (time.time() - self.created > SESSION_TTL
                or self.uses >= SESSION_MAX_USES)


class RegionPool:
    """Pool of pre-warmed sessions for one RT region."""

    def __init__(self, region: str, size: int):
        self._region = region
        self._size   = size
        self._pool   = queue.Queue()
        self._stop   = threading.Event()
        self._filler = threading.Thread(
            target=self._fill_loop, daemon=True, name=f"pool-{region}"
        )
        self._filler.start()

    def _create(self) -> _PooledSession:
        lat, lon = _REGION_SEED[self._region]
        sess = WazeSession(lat, lon, debug=False)
        sess.register(lat, lon)
        sess.login(lat, lon)
        sess.prepare_for_area(lat, lon)
        return _PooledSession(sess)

    def _fill_loop(self):
        backoff_idx = 0
        while not self._stop.is_set():
            if self._pool.qsize() < self._size:
                try:
                    ps = self._create()
                    self._pool.put(ps)
                    log.info("[%s] session ready, pool=%d/%d",
                             self._region, self._pool.qsize(), self._size)
                    backoff_idx = 0
                except Exception as exc:
                    delay = REFILL_BACKOFF[min(backoff_idx, len(REFILL_BACKOFF)-1)]
                    log.warning("[%s] create failed (%s) — retry in %ds",
                                self._region, exc, delay)
                    backoff_idx += 1
                    self._stop.wait(delay)
            else:
                self._stop.wait(2)

    def acquire(self, timeout: float = 30) -> _PooledSession:
        return self._pool.get(timeout=timeout)

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
        _pools[region] = RegionPool(region, size)
        log.info("Pool [%s] started (target=%d)", region, size)
    yield
    for p in _pools.values():
        p.shutdown()


app = FastAPI(title="Waze RT API", lifespan=lifespan)


def _query(ps: _PooledSession, lat: float, lon: float, radius_km: float) -> list:
    """Run 5-box query on an existing session. May raise RuntimeError on session expiry."""
    sess = ps.sess

    # Re-handshake if querying a far-away area (>50km from last handshake)
    needs_hs = True
    if ps.last_lat is not None:
        dlat = abs(lat - ps.last_lat) * 110574
        dlon = abs(lon - ps.last_lon) * math.cos(math.radians(lat)) * 111320
        needs_hs = math.hypot(dlat, dlon) > 50_000

    if needs_hs:
        sess.prepare_for_area(lat, lon)
        ps.last_lat, ps.last_lon = lat, lon

    alert_cache: dict = {}
    raw_boxes = _shrinking_boxes(lon, lat, radius_km * 1000)
    for i, raw_box in enumerate(raw_boxes):
        box = _shrink(raw_box, 0.75)
        batch = sess.query_box(box, debug_label=str(i))
        for a in _parse_alerts(batch):
            key = a.get("uuid") or str(a.get("id")) or None
            if key:
                alert_cache[key] = {k: v for k, v in a.items()}
        for removed_id in _parse_removed_ids(batch):
            alert_cache.pop(removed_id, None)
    return list(alert_cache.values())


@app.get("/alerts")
def alerts(
    lat: float       = Query(21.12,   description="Latitude"),
    lon: float       = Query(-101.68, description="Longitude"),
    radius_km: float = Query(15.0,    ge=1, le=50, description="Radius km"),
):
    t0 = time.time()
    region = _region(lat, lon)
    pool = _pools.get(region)
    if pool is None:
        raise HTTPException(500, f"No pool for region '{region}'")
    try:
        ps = pool.acquire(timeout=30)
    except queue.Empty:
        raise HTTPException(503, f"[{region}] No sessions available — pool warming up, retry in a few seconds")

    discard = False
    try:
        ps.uses += 1
        result = _query(ps, lat, lon, radius_km)
    except RuntimeError as exc:
        msg = str(exc)
        if any(k in msg.lower() for k in ("sessionexpired", "relogin", "unknown userid", "secretkey")):
            log.warning("[%s] session expired mid-query, discarding", region)
            discard = True
            raise HTTPException(502, f"Session expired: {msg}")
        discard = True
        raise HTTPException(502, msg)
    finally:
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
        "pools":  {r: {"ready": p.size, "target": POOL_SIZE_PER_REGION[r]}
                   for r, p in _pools.items()},
    }
