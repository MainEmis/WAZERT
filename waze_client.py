#!/usr/bin/env python3
"""
Method 2 — Waze mobile RT protocol.

Port of wzsabre 2.2 fetch path (WazeSession + WazeRtCodec + GeoBoxes).
Upstream: https://github.com/nicglazkov/highway-radar-sabre-plus
Commit:   577df48b8e5d0080118d3c665ddd9ec3460ef3da

Key fixes vs earlier version:
  - handshake includes SetMood,1 (was missing → server never subscribed us)
  - prepareForArea() is a separate command BEFORE the box loop
  - each box is shrunk by 0.75 before querying (circleToBox shrink, per GeoBoxes.java)
  - loginRequest.reason = NORMAL (field 3 = 0, explicitly set)
"""

import base64
import math
import random
import sys
import time
import uuid

import requests
import waze_pb2

# ── Constants (WazeConstants.java) ───────────────────────────────────────────

APP_VERSION      = "5.17.1.0"
PROTOCOL_VERSION = 234

PATH_STATIC  = "/rtserver/distrib/static"
PATH_LOGIN   = "/rtserver/distrib/login"
PATH_COMMAND = "/rtserver/distrib/command"

WAIT_TIMEOUT_LOGIN   = "8500"
WAIT_TIMEOUT_COMMAND = "10500"

M_PER_DEG_LAT = 110574.0
SHRINK_STEPS  = 5


def _region(lat: float, lon: float) -> str:
    """region(lat,lon).

    Upstream Java says NA for all of the Americas (-170≤lon≤-52, -15≤lat≤73),
    but rt-xlb-am.waze.com does NOT serve Mexico/LatAm data — the ROW server
    does (verified empirically: AM returns InfoAround,alerts=0 + NetworkCycleTime
    for León/CDMX while ROW returns real alerts). Restrict NA to US/Canada
    (lat ≥ 30), everything south goes to ROW.
    """
    if -170.0 <= lon <= -52.0 and 30.0 <= lat <= 73.0:
        return "na"
    if 34.0 <= lon <= 36.0 and 29.5 <= lat <= 33.5:
        return "il"
    return "row"


def _rt_host(region: str) -> str:
    """rtHost(region) — verbatim from WazeConstants.java."""
    if region == "na":
        return "rt-xlb-am.waze.com"
    if region == "il":
        return "rt-xlb-il.waze.com"
    return "rt-xlb-row.waze.com"


# ── DeviceIdentity (DeviceIdentity.java) ─────────────────────────────────────

_DEVICE_POOL = [
    {"manufacturer": "Samsung",  "model": "SM-G991B",    "os_version": "14", "width": 1080, "height": 2340},
    {"manufacturer": "Google",   "model": "Pixel 8",     "os_version": "14", "width": 1080, "height": 2400},
    {"manufacturer": "OnePlus",  "model": "CPH2413",     "os_version": "14", "width": 1080, "height": 2412},
    {"manufacturer": "Xiaomi",   "model": "2201123G",    "os_version": "13", "width": 1080, "height": 2400},
    {"manufacturer": "Motorola", "model": "XT2251-1",    "os_version": "13", "width": 1080, "height": 2400},
    {"manufacturer": "Nothing",  "model": "A063",        "os_version": "14", "width": 1080, "height": 2412},
    {"manufacturer": "Samsung",  "model": "SM-S908B",    "os_version": "14", "width": 1440, "height": 3088},
    {"manufacturer": "Google",   "model": "Pixel 7 Pro", "os_version": "13", "width": 1440, "height": 3120},
]


def _random_device() -> dict:
    d = dict(random.choice(_DEVICE_POOL))
    d["installation_id"] = str(uuid.uuid4())
    return d


# ── GeoBoxes (GeoBoxes.java) ─────────────────────────────────────────────────

def _m_per_deg_lon(lat: float) -> float:
    return math.cos(math.radians(lat)) * 111320.0


def _circle_to_box(lon: float, lat: float, radius_m: float) -> list:
    """circleToBox(lon, lat, radiusM) — returns [lonMin, latMin, lonMax, latMax]."""
    d_lat = radius_m / M_PER_DEG_LAT
    d_lon = radius_m / _m_per_deg_lon(lat)
    return [lon - d_lon, lat - d_lat, lon + d_lon, lat + d_lat]


def _default_box(lon: float, lat: float) -> list:
    """circleToBox(lon,lat) — fixed small box used in handshake (WazeRtCodec.circleToBox)."""
    return [lon - 0.018, lat - 0.015, lon + 0.018, lat + 0.015]


def _shrink(box: list, factor: float) -> list:
    """shrink(box, factor) — same center, half-extents scaled by factor."""
    cx = (box[0] + box[2]) / 2.0
    cy = (box[1] + box[3]) / 2.0
    hx = ((box[2] - box[0]) / 2.0) * factor
    hy = ((box[3] - box[1]) / 2.0) * factor
    return [cx - hx, cy - hy, cx + hx, cy + hy]


def _shrinking_boxes(lon: float, lat: float, radius_m: float) -> list:
    """shrinkingBoxes — full box + 4 progressively halved boxes (per GeoBoxes.java)."""
    boxes = [_circle_to_box(lon, lat, radius_m)]
    for _ in range(SHRINK_STEPS - 1):
        boxes.append(_shrink(boxes[-1], 0.5))
    return boxes


# ── WazeRtCodec (WazeRtCodec.java) ───────────────────────────────────────────

def _f6(v: float) -> str:
    return f"{v:.6f}"


def _wrap_batch_line(element: waze_pb2.Element) -> str:
    batch = waze_pb2.Batch()
    batch.element.append(element)
    return "ProtoBase64," + base64.b64encode(batch.SerializeToString()).decode()


def _jitter(lon: float, lat: float) -> tuple:
    """±500m position jitter (WazeRtCodec.buildClientInfoLine)."""
    j_lon = ((random.random() - 0.5) * 1000.0) / (math.cos(math.radians(lat)) * 111320.0)
    j_lat = ((random.random() - 0.5) * 1000.0) / M_PER_DEG_LAT
    return lon + j_lon, lat + j_lat


def _build_client_info_line(device: dict, lon: float, lat: float) -> str:
    jlon, jlat = _jitter(lon, lat)
    el = waze_pb2.Element()
    ci = el.client_info
    ci.protocol         = PROTOCOL_VERSION
    ci.client_version   = APP_VERSION
    ci.last_position.lon_times1000000 = int(round(jlon * 1_000_000))
    ci.last_position.lat_times1000000 = int(round(jlat * 1_000_000))
    ci.manufacturer     = device["manufacturer"]
    ci.model            = device["model"]
    ci.os_version       = device["os_version"]
    ci.locale           = "en"
    ci.installation_id  = device["installation_id"]
    ci.device_type      = waze_pb2.ANDROID_DEVICE
    ci.app_type         = waze_pb2.WAZE
    d = ci.display.add()
    d.type   = waze_pb2.Display.BUILT_IN
    d.width  = device["width"]
    d.height = device["height"]
    ci.os_language_id      = "en"
    ci.session_uuid        = str(uuid.uuid4())
    ci.current_time_millis = int(time.time() * 1000)
    ci.app_flavor          = waze_pb2.ALPHA
    return _wrap_batch_line(el)


def _build_register_line() -> str:
    el = waze_pb2.Element()
    el.register.SetInParent()
    return _wrap_batch_line(el)


def _build_login_line(username: str, password: str) -> str:
    el = waze_pb2.Element()
    el.login_request.password_credential.username = username
    el.login_request.password_credential.password = password
    el.login_request.reason = waze_pb2.LoginRequest.NORMAL   # field 3 = 0, explicitly set
    return _wrap_batch_line(el)


def _build_ads_line() -> str:
    el = waze_pb2.Element()
    el.report_ads_setting.SetInParent()
    return _wrap_batch_line(el)


def _build_uid_header(server_session_id: int, secret_key: str) -> str:
    uid = waze_pb2.UID()
    uid.id         = server_session_id
    uid.secret_key = secret_key
    return base64.b64encode(uid.SerializeToString()).decode()


def _see_me_command(mode: int = 1) -> str:
    return f"SeeMe,{mode},2,T,T,T,1,-1,1,7"


def _set_mood_command() -> str:
    return "SetMood,1"


def _location_command(lon: float, lat: float) -> str:
    return f"Location,{lon},{lat}"


def _map_displayed_command(lon_min: float, lat_min: float, lon_max: float, lat_max: float) -> str:
    """mapDisplayedCommand(lonMin,latMin,lonMax,latMax) — verbatim from WazeRtCodec.java.

    19-value format: NW NE SE SW mid_lon mid_lat 67186 NW NE SE SW
    (corners = lon,lat pairs clockwise from NW)
    """
    mid_lon = (lon_min + lon_max) / 2.0
    mid_lat = (lat_min + lat_max) / 2.0
    return (
        "MapDisplayed,"
        + _f6(lon_min) + "," + _f6(lat_max) + ","   # NW
        + _f6(lon_max) + "," + _f6(lat_max) + ","   # NE
        + _f6(lon_max) + "," + _f6(lat_min) + ","   # SE
        + _f6(lon_min) + "," + _f6(lat_min) + ","   # SW
        + _f6(mid_lon) + "," + _f6(mid_lat) + ",67186,"
        + _f6(lon_min) + "," + _f6(lat_max) + ","   # NW (repeat)
        + _f6(lon_max) + "," + _f6(lat_max) + ","   # NE (repeat)
        + _f6(lon_max) + "," + _f6(lat_min) + ","   # SE (repeat)
        + _f6(lon_min) + "," + _f6(lat_min)         # SW (repeat)
    )


def _handshake_payload(lon: float, lat: float) -> str:
    """handshakePayload(lon,lat) — verbatim from WazeRtCodec.java.

    SeeMe + SetMood + Location + MapDisplayed using the small fixed box.
    Sent as a single prepareForArea command BEFORE the box loop.
    """
    box = _default_box(lon, lat)
    return (
        _see_me_command(1) + "\n"
        + _set_mood_command() + "\n"
        + _location_command(lon, lat) + "\n"
        + _map_displayed_command(box[0], box[1], box[2], box[3])
    )


# ── Alert parsing (WazeRtCodec.parseAlerts / parseRemovedAlertIds) ────────────

def _type_name(val: int) -> str:
    try:
        name = waze_pb2.DESCRIPTOR.enum_types_by_name["AlertType"].values_by_number[val].name
    except KeyError:
        return "UNKNOWN"
    if name in ("UNKNOWN_TYPE", "UNKNOWN_ALERT") or name.startswith("__NOT_IN_USE"):
        return "UNKNOWN"
    return name


def _subtype_name(val: int) -> str:
    if val == 0:
        return ""
    try:
        name = waze_pb2.DESCRIPTOR.enum_types_by_name["AlertSubType"].values_by_number[val].name
    except KeyError:
        return ""
    if name == "NO_SUBTYPE" or name.startswith("__NOT_IN_USE"):
        return ""
    return name


def _parse_alerts(batch: waze_pb2.Batch) -> list:
    out = []
    for el in batch.element:
        if not el.HasField("add_alert_action"):
            continue
        aaa = el.add_alert_action
        if not aaa.HasField("realtime_alert"):
            continue
        ra = aaa.realtime_alert
        if not ra.HasField("alert_info"):
            continue
        info = ra.alert_info
        if not info.HasField("position"):
            continue
        c = info.position

        lon_raw = c.lon_times1000000 & 0xFFFFFFFF
        if lon_raw >= 0x80000000:
            lon_raw -= 0x100000000
        lon_f = lon_raw / 1_000_000.0
        lat_f = c.lat_times1000000 / 1_000_000.0

        street = city = None
        report_time = thumbs = None
        if ra.HasField("alert_reporting_info"):
            ri = ra.alert_reporting_info
            report_time = ri.report_time if ri.report_time else None
            if ri.thumbs_up_count > 0:
                thumbs = ri.thumbs_up_count
            s = ri.alert_address.street
            ci2 = ri.alert_address.city
            if s:
                street = s
            if ci2:
                city = ci2

        pub_millis = (report_time * 1000) if report_time else int(time.time() * 1000)
        out.append({
            "uuid":      ra.alert_uuid or None,
            "id":        ra.id or None,
            "type":      _type_name(info.type),
            "subtype":   _subtype_name(info.sub_type) or None,
            "lat":       lat_f,
            "lon":       lon_f,
            "azymuth":   info.azymuth if info.azymuth else None,
            "street":    street,
            "city":      city,
            "pub_millis": pub_millis,
            "thumbs_up": thumbs,
        })
    return out


def _parse_removed_ids(batch: waze_pb2.Batch) -> list:
    out = []
    for el in batch.element:
        oc = el.old_command
        if oc and oc.strip().startswith("RmAlert,"):
            out.append(oc.strip()[len("RmAlert,"):].strip())
    return out


def _read_varint(data: bytes, pos: int) -> tuple:
    result, shift = 0, 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return result, pos


def _raw_fields(data: bytes, depth: int = 0, max_depth: int = 4) -> list:
    """Decode all field numbers recursively from raw protobuf bytes."""
    lines = []
    pad = "  " * depth
    pos = 0
    while pos < len(data):
        try:
            tag, pos = _read_varint(data, pos)
        except Exception:
            break
        field_num = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 0:
            val, pos = _read_varint(data, pos)
            lines.append(f"{pad}f{field_num}:varint={val}")
        elif wire_type == 1:
            val = int.from_bytes(data[pos:pos+8], "little")
            pos += 8
            lines.append(f"{pad}f{field_num}:i64={val}")
        elif wire_type == 2:
            vlen, pos = _read_varint(data, pos)
            payload = data[pos:pos+vlen]; pos += vlen
            if depth < max_depth and vlen > 0:
                sub = _raw_fields(payload, depth + 1, max_depth)
                if sub:
                    lines.append(f"{pad}f{field_num}:msg({vlen}B){{")
                    lines.extend(sub[:12])  # cap sub-lines
                    if len(sub) > 12:
                        lines.append(f"{pad}  ... +{len(sub)-12} more")
                    lines.append(f"{pad}}}")
                else:
                    try:
                        txt = payload.decode("utf-8", errors="strict")
                        if all(32 <= ord(c) < 127 for c in txt[:60]):
                            lines.append(f"{pad}f{field_num}:str({vlen}B)={txt[:60]!r}")
                            continue
                    except Exception:
                        pass
                    lines.append(f"{pad}f{field_num}:bytes({vlen}B)={payload[:20].hex()}")
            else:
                lines.append(f"{pad}f{field_num}:bytes({vlen}B)={payload[:20].hex()}")
        elif wire_type == 5:
            val = int.from_bytes(data[pos:pos+4], "little")
            pos += 4
            lines.append(f"{pad}f{field_num}:i32={val}")
        else:
            lines.append(f"{pad}f{field_num}:UNKNOWN_wt={wire_type}")
            break
    return lines


def _debug_batch(label: str, batch: waze_pb2.Batch) -> None:
    # Also decode the entire batch raw to catch fields the descriptor doesn't know
    raw_batch = batch.SerializeToString()
    raw_lines = _raw_fields(raw_batch, depth=0, max_depth=3)
    print(f"  [batch {label}] {len(batch.element)} elements "
          f"({len(raw_batch)}B raw):", file=sys.stderr)
    for i, el in enumerate(batch.element):
        known = [(fd.name, fd.number) for fd, _ in el.ListFields()]
        print(f"    el[{i}] known={known}", file=sys.stderr)
    if raw_lines:
        print(f"  [raw decode]:", file=sys.stderr)
        for line in raw_lines[:60]:
            print(f"    {line}", file=sys.stderr)
        if len(raw_lines) > 60:
            print(f"    ... +{len(raw_lines)-60} more lines", file=sys.stderr)


# ── WazeSession (WazeSession.java) ────────────────────────────────────────────

class WazeSession:
    def __init__(self, lat: float, lon: float, debug: bool = False):
        region = _region(lat, lon)
        self._host    = _rt_host(region)
        self._device  = _random_device()
        self._debug   = debug
        self._http    = requests.Session()
        self._seq     = 1
        self.credentials  = None   # (username, password)
        self.session_info = None   # (server_session_id, secret_key, global_user_id)

    def _url(self, path: str) -> str:
        return f"https://{self._host}{path}"

    def _next_seq(self) -> str:
        s = str(self._seq)
        self._seq += 1
        return s

    def _post(self, path: str, body_str: str, extra: dict) -> requests.Response:
        body = body_str.encode("utf-8")
        headers = {"Content-Type": "binary/octet-stream"}
        headers.update(extra)
        if self._debug:
            print(f"  → POST {path}  seq={extra.get('sequence-number')}  {len(body)}B", file=sys.stderr)
            print(f"    body[:200] = {body[:200]!r}", file=sys.stderr)
        resp = self._http.post(self._url(path), data=body, headers=headers, timeout=(15, 25))
        if self._debug:
            print(f"  ← HTTP {resp.status_code}  {len(resp.content)}B", file=sys.stderr)
        return resp

    @staticmethod
    def _check_errors(batch: waze_pb2.Batch) -> None:
        for el in batch.element:
            if el.HasField("error"):
                code = el.error.code
                desc = el.error.description.lower()
                if ("relogin" in desc or "unknown userid" in desc
                        or "secretkey missing" in desc or "secret key missing" in desc):
                    raise RuntimeError(f"SessionExpired: {el.error.description}")
                if 400 <= code < 500:
                    raise RuntimeError(f"AccountRejected {code}: {el.error.description}")
                if code >= 500 and code != 504:
                    raise RuntimeError(f"ServerError {code}: {el.error.description}")

    def register(self, lat: float, lon: float) -> str:
        body = (
            _build_client_info_line(self._device, lon, lat) + "\n"
            + _build_register_line()
        )
        resp = self._post(PATH_STATIC, body, {
            "User-Agent":             APP_VERSION,
            "x-waze-network-version": "3",
            "sequence-number":        self._next_seq(),
        })
        if resp.status_code >= 400:
            raise RuntimeError(f"register HTTP {resp.status_code}")
        if not resp.content:
            raise RuntimeError("empty register response")
        batch = waze_pb2.Batch()
        batch.ParseFromString(resp.content)
        if self._debug:
            _debug_batch("register", batch)
        self._check_errors(batch)
        for el in batch.element:
            if el.HasField("register_successful"):
                rs = el.register_successful
                if not rs.username or not rs.password:
                    raise RuntimeError("empty credentials from register")
                self.credentials = (rs.username, rs.password)
                return rs.username
        raise RuntimeError(f"no RegisterSuccessful in {len(resp.content)}B response")

    def login(self, lat: float, lon: float) -> tuple:
        if not self.credentials:
            raise RuntimeError("login() before register()")
        self._http.cookies.clear()
        user, pwd = self.credentials
        body = (
            _build_client_info_line(self._device, lon, lat) + "\n"
            + _build_login_line(user, pwd) + "\n"
            + _build_ads_line()
        )
        resp = self._post(PATH_LOGIN, body, {
            "User-Agent":             f"waze/{APP_VERSION}",
            "cache-control":          "no-cache",
            "sequence-number":        self._next_seq(),
            "x-waze-network-version": "3",
            "x-waze-wait-timeout":    WAIT_TIMEOUT_LOGIN,
        })
        if 400 <= resp.status_code < 500:
            raise RuntimeError(f"login rejected HTTP {resp.status_code}")
        if resp.status_code >= 500:
            raise RuntimeError(f"login server error HTTP {resp.status_code}")
        if not resp.content:
            raise RuntimeError("empty login response")
        batch = waze_pb2.Batch()
        batch.ParseFromString(resp.content)
        if self._debug:
            _debug_batch("login", batch)
        self._check_errors(batch)
        for el in batch.element:
            if el.HasField("login_response"):
                which = el.login_response.WhichOneof("response")
                if which == "login_error":
                    err = el.login_response.login_error.error_type
                    raise RuntimeError(f"login error type={err}")
                if which == "login_success":
                    s = el.login_response.login_success
                    if not s.server_session_id:
                        raise RuntimeError("zero serverSessionId")
                    if not s.secret_key:
                        raise RuntimeError("empty secretKey")
                    self.session_info = (s.server_session_id, s.secret_key, s.global_user_id)
                    self._seq = 2
                    return s.server_session_id, batch
        raise RuntimeError(f"no LoginSuccess in {len(resp.content)}B response")

    def _command(self, payload: str) -> waze_pb2.Batch:
        if not self.session_info:
            raise RuntimeError("_command() before login()")
        sid, skey, _ = self.session_info
        resp = self._post(PATH_COMMAND, payload, {
            "User-Agent":             APP_VERSION,
            "cache-control":          "no-cache",
            "sequence-number":        self._next_seq(),
            "x-waze-network-version": "3",
            "x-waze-wait-timeout":    WAIT_TIMEOUT_COMMAND,
            "uid":                    _build_uid_header(sid, skey),
        })
        if 400 <= resp.status_code < 500:
            raise RuntimeError(f"command rejected HTTP {resp.status_code} — session expired")
        if resp.status_code >= 500:
            raise RuntimeError(f"command server error HTTP {resp.status_code}")
        if not resp.content:
            return waze_pb2.Batch()
        batch = waze_pb2.Batch()
        batch.ParseFromString(resp.content)
        return batch

    def prepare_for_area(self, lat: float, lon: float) -> waze_pb2.Batch:
        """prepareForArea(lat,lon) — handshake: SeeMe+SetMood+Location+MapDisplayed.

        The server sometimes returns 504 Retry on the handshake if the session
        slot is not ready yet. Java checkErrors() throws WazeOperationException
        for any code>=500 and lets refresh() retry with backoff. We mirror that:
        retry up to 3 times with 3s sleep between attempts.
        """
        for attempt in range(3):
            batch = self._command(_handshake_payload(lon, lat))
            if self._debug:
                _debug_batch(f"handshake attempt={attempt}", batch)
            has_error = any(el.HasField("error") for el in batch.element)
            if not has_error:
                return batch
            # Check if it's a 504 (retryable) or a fatal error
            for el in batch.element:
                if el.HasField("error"):
                    code = el.error.code
                    desc = el.error.description.lower()
                    if ("relogin" in desc or "unknown userid" in desc
                            or "secretkey missing" in desc):
                        raise RuntimeError(f"SessionExpired in handshake: {el.error.description}")
                    if 400 <= code < 500:
                        raise RuntimeError(f"AccountRejected in handshake {code}: {el.error.description}")
                    # code >= 500 (including 504): retryable
                    if self._debug:
                        print(f"  handshake error {code} (attempt {attempt+1}), sleep 3s",
                              file=sys.stderr)
                    if attempt < 2:
                        time.sleep(3)
        return batch   # return last batch; box queries will reveal if it worked

    def query_box(self, box: list, debug_label: str = "") -> waze_pb2.Batch:
        """queryBox(bbox) — one MapDisplayed command.

        box = [lonMin, latMin, lonMax, latMax] (already shrunk by 0.75 by caller).
        Retries up to 3 times on 504.
        """
        cmd = _map_displayed_command(box[0], box[1], box[2], box[3])
        for attempt in range(3):
            batch = self._command(cmd)
            if self._debug:
                _debug_batch(f"box{debug_label} attempt={attempt}", batch)
            has_504 = any(
                el.HasField("error") and el.error.code == 504
                for el in batch.element
            )
            if has_504:
                if attempt < 2:
                    if self._debug:
                        print(f"  504 Retry (attempt {attempt+1}), sleeping 2s", file=sys.stderr)
                    time.sleep(2)
                    continue
                # Return 504 batch for caller to harvest any co-delivered data
                return batch
            self._check_errors(batch)
            return batch
        return waze_pb2.Batch()


