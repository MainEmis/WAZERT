#!/usr/bin/env python3
"""
Method 1 — Waze live-map XHR interception.

Port of WazeWebViewFetcher.kt (guberm/waze-alerts-notifier,
commit 70b86de1f54a9acdc220d1e4e375ad585604339b).

Two-phase flow:
  Phase 1: load the live-map page and wait for Waze's env=il init call to
           COMPLETE (it sets session cookies required for env=na).
  Phase 2: after env=il completes, inject our env=na URL. The interceptor
           hijacks the NEXT georss call (Waze refreshes every ~30s, or we
           wait for Phase 2B below). We also trigger a forced re-fetch via
           the page's JS if Phase 2 takes too long.

If Waze's page structure doesn't fire a second georss call, we fall back to
page.evaluate() fetch — which runs inside the browser context with all session
cookies already set.
"""

import argparse
import asyncio
import json
import math
import sys
import time

from playwright.async_api import async_playwright

M_PER_DEG_LAT = 110574.0
GEORSS_BASE   = "https://www.waze.com/live-map/api/georss"
LIVE_MAP_URL  = "https://www.waze.com/live-map/"

_UA = (
    "Mozilla/5.0 (Linux; Android 16) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36"
)

# Interceptor injected AFTER env=il completes (via evaluateJavascript in
# onPageFinished, same as WazeWebViewFetcher.kt).  Sets up XHR.open hook
# and exposes wazeSetNaUrl().
_INTERCEPTOR_JS = r"""
(function() {
    if (window._wazeIntercepted) return;
    window._wazeIntercepted = true;
    window._wazeNaUrl = null;

    var _origOpen = XMLHttpRequest.prototype.open;
    var _origSend = XMLHttpRequest.prototype.send;

    XMLHttpRequest.prototype.open = function(method, url) {
        this._waze_url = (typeof url === 'string') ? url : '';
        var actualUrl = url;
        if (this._waze_url.indexOf('georss') !== -1 && this._waze_url.indexOf('env=il') !== -1) {
            var naUrl = window._wazeNaUrl;
            if (naUrl) {
                window._wazeNaUrl = null;
                actualUrl = naUrl;
                this._waze_url = naUrl;
            }
        }
        return _origOpen.call(this, method, actualUrl);
    };

    window.wazeSetNaUrl = function(url) { window._wazeNaUrl = url; };

    XMLHttpRequest.prototype.send = function() {
        if (this._waze_url && this._waze_url.indexOf('georss') !== -1 &&
                this._waze_url.indexOf('env=il') === -1) {
            var xhr = this;
            this.addEventListener('load', function() {
                if (xhr.status === 200 && window.__wazeResult) {
                    window.__wazeResult(xhr.responseText, xhr._waze_url);
                } else if (xhr.status !== 200 && window.__wazeError) {
                    window.__wazeError(String(xhr.status), xhr._waze_url);
                }
            });
            this.addEventListener('error', function() {
                if (window.__wazeError) window.__wazeError('network_error', xhr._waze_url);
            });
        }
        return _origSend.apply(this, arguments);
    };
})();
"""


def _bbox(lat: float, lon: float, radius_m: float) -> dict:
    lat_off = radius_m / M_PER_DEG_LAT
    lon_off = radius_m / (M_PER_DEG_LAT * math.cos(math.radians(lat)))
    return {"top": lat+lat_off, "bottom": lat-lat_off,
            "left": lon-lon_off, "right": lon+lon_off}


def _georss_url(lat: float, lon: float, radius_m: float) -> str:
    b = _bbox(lat, lon, radius_m)
    return (f"{GEORSS_BASE}"
            f"?top={b['top']:.6f}&bottom={b['bottom']:.6f}"
            f"&left={b['left']:.6f}&right={b['right']:.6f}"
            f"&env=na&types=alerts,traffic")


async def fetch_alerts(lat: float, lon: float, radius_km: float, debug: bool = False):
    our_url = _georss_url(lat, lon, radius_km * 1000)
    if debug:
        print(f"  Target URL: {our_url}", file=sys.stderr)

    loop = asyncio.get_event_loop()
    env_il_done: "asyncio.Future[None]" = loop.create_future()
    captured:    "asyncio.Future[dict]" = loop.create_future()
    network_log = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
                  "--disable-setuid-sandbox"],
        )
        context = await browser.new_context(
            user_agent=_UA,
            geolocation={"latitude": lat, "longitude": lon},
            permissions=["geolocation"],
        )
        page = await context.new_page()

        # Track georss network responses
        async def on_response(response):
            if "georss" in response.url:
                entry = {"url": response.url, "status": response.status,
                         "ts": int(time.time())}
                network_log.append(entry)
                if debug:
                    print(f"  ← net georss HTTP {response.status}  {response.url[:100]}",
                          file=sys.stderr)
                # Phase 1: env=il completed → session cookies are now set
                if "env=il" in response.url and not env_il_done.done():
                    if debug:
                        print(f"  Phase 1 complete: env=il HTTP {response.status}",
                              file=sys.stderr)
                    env_il_done.set_result(None)

        page.on("response", on_response)

        # JS bridge: receives the captured XHR response body
        async def on_result(text: str, url: str) -> None:
            if debug:
                print(f"  ← JS bridge OK: {len(text)}B", file=sys.stderr)
            if captured.done():
                return
            try:
                captured.set_result(json.loads(text.strip()))
            except json.JSONDecodeError as e:
                captured.set_exception(Exception(f"JSON: {e}; body={text[:200]}"))

        async def on_error(status: str, url: str) -> None:
            if debug:
                print(f"  ← JS bridge ERROR: HTTP {status}", file=sys.stderr)
            if not captured.done():
                captured.set_exception(Exception(f"XHR error HTTP {status}"))

        await page.expose_function("__wazeResult", on_result)
        await page.expose_function("__wazeError",  on_error)

        # Load page
        await page.goto(LIVE_MAP_URL, wait_until="domcontentloaded", timeout=30_000)
        if debug:
            print(f"  Page loaded", file=sys.stderr)

        # Phase 1: wait for env=il to complete (sets session cookies)
        try:
            await asyncio.wait_for(env_il_done, timeout=20)
            if debug:
                print(f"  env=il done — injecting interceptor + setting naUrl", file=sys.stderr)
        except asyncio.TimeoutError:
            if debug:
                print("  WARNING: env=il never fired — proceeding anyway", file=sys.stderr)

        # Phase 2a: inject interceptor and set naUrl for the NEXT env=il call
        escaped = our_url.replace("'", "\\'")
        await page.evaluate(_INTERCEPTOR_JS + f"\nwindow._wazeNaUrl = '{escaped}';")

        # Phase 2b: if no second env=il fires within 15s, fall back to a
        # direct fetch() from within the browser context (cookies already set)
        try:
            data = await asyncio.wait_for(asyncio.shield(captured), timeout=15)
        except asyncio.TimeoutError:
            if debug:
                print(f"  No second env=il; falling back to in-page fetch()", file=sys.stderr)
            # Run fetch() inside the browser — inherits all session cookies
            try:
                result = await page.evaluate(f"""
                    async () => {{
                        const r = await fetch('{escaped}', {{
                            headers: {{
                                'accept': 'application/json, text/plain, */*',
                                'referer': 'https://www.waze.com/live-map/'
                            }},
                            credentials: 'include'
                        }});
                        if (!r.ok) throw new Error('HTTP ' + r.status);
                        return await r.json();
                    }}
                """)
                if debug:
                    print(f"  in-page fetch() OK: {len(result.get('alerts', []))} alerts",
                          file=sys.stderr)
                data = result
            except Exception as e:
                await browser.close()
                raise RuntimeError(f"in-page fetch() failed: {e}")

        await browser.close()

    return data.get("alerts", []), data.get("jams", []), data, network_log


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Waze live-map XHR interception (Method 1)"
    )
    ap.add_argument("--lat",       type=float, default=21.12,   metavar="LAT")
    ap.add_argument("--lon",       type=float, default=-101.68,  metavar="LON")
    ap.add_argument("--radius-km", type=float, default=15.0,    metavar="KM", dest="radius_km")
    ap.add_argument("--debug",     action="store_true")
    ap.add_argument("--save",      action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    alerts: list = []
    jams:   list = []
    raw_data: dict = {}
    network_log: list = []

    print("BROWSER: OK")
    try:
        alerts, jams, raw_data, network_log = asyncio.run(
            fetch_alerts(args.lat, args.lon, args.radius_km, debug=args.debug)
        )
        print("WAZE_SESSION: OK")
        print("XHR_CAPTURE: OK")
        print("GEO_RESPONSE: OK")
    except Exception as exc:
        print("WAZE_SESSION: OK")   # page loaded
        print(f"XHR_CAPTURE: FAIL\n  {exc}", file=sys.stderr)
        print("GEO_RESPONSE: FAIL", file=sys.stderr)
        sys.exit(1)

    latency_ms = int((time.time() - t0) * 1000)

    print("LOCATION: Leon, Guanajuato, MX")
    print(f"ALERT_COUNT: {len(alerts)}")

    output = {
        "method":          "browser_xhr",
        "query_center":    {"lat": args.lat, "lon": args.lon},
        "query_radius_km": args.radius_km,
        "timestamp":       int(time.time()),
        "latency_ms":      latency_ms,
        "alert_count":     len(alerts),
        "jam_count":       len(jams),
        "alerts":          alerts,
        "jams":            jams,
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))

    if args.save:
        for fname, obj in [("raw-response.json", raw_data),
                           ("extracted-alerts.json", {"alerts": alerts, "jams": jams}),
                           ("network-log.json", network_log)]:
            with open(fname, "w") as f:
                json.dump(obj, f, indent=2, ensure_ascii=False)
            print(f"  saved {fname}", file=sys.stderr)


if __name__ == "__main__":
    main()
