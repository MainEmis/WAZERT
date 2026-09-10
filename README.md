# Waze RT Client — León, Guanajuato, MX

Python port of the Waze mobile real-time protocol. No API key, no Waze for Cities account, no browser.

## Upstream source

- **Repo:** https://github.com/nicglazkov/highway-radar-sabre-plus
- **Commit:** `577df48b8e5d0080118d3c665ddd9ec3460ef3da`
- **Files ported:**
  - `WazeConstants.java` — hosts, paths, version, timeouts
  - `DeviceIdentity.java` — Android device pool
  - `GeoBoxes.java` — circle→box, shrink, shrinkingBoxes
  - `WazeRtCodec.java` — request encoding / response parsing
  - `WazeSession.java` — register / login / queryBox
  - `WazeProtocolSource.java` — fetch flow + delta-merge cache

Proto schema reconstructed from the same upstream:
- `app/src/main/proto/waze.proto` (verbatim copy)

## Acceptance test

```bash
cd waze_rt
docker compose run --rm waze-test
```

Expected output:
```
REGISTER: OK
LOGIN: OK
QUERY: OK
LOCATION: Leon, Guanajuato, MX
ALERTS: <integer>
```

Followed by JSON. Exit code 0 on success, 1 on any Waze failure.

## Custom location

```bash
docker compose run --rm waze-test \
  --lat 21.12 --lon -101.68 --radius-km 15

# CDMX
docker compose run --rm waze-test \
  --lat 19.43 --lon -99.13 --radius-km 20

# Debug (prints HTTP traffic)
docker compose run --rm waze-test --debug
```

## Protocol flow

```
POST /rtserver/distrib/static      # register anonymous account
  Body: ProtoBase64(ClientInfo) \n ProtoBase64(Register)
  → RegisterSuccessful{username, password}

POST /rtserver/distrib/login       # login with anonymous creds
  Body: ProtoBase64(ClientInfo) \n ProtoBase64(LoginRequest) \n ProtoBase64(ReportAdsSettings)
  → LoginResponse{LoginSuccess{serverSessionId, secretKey}}

POST /rtserver/distrib/command     # query area (x5 shrinking boxes)
  Body (first): SeeMe,1,... \n Location,lon,lat \n MapDisplayed,...
  Body (subsequent): MapDisplayed,...
  Header: uid=base64(UID{serverSessionId, secretKey})
  → Batch{AddAlertAction...}
```

RT host for Mexico (lon < -20): `rt-xlb-am.waze.com`

## Zero alerts

Zero alerts is a valid result if REGISTER + LOGIN + QUERY all print OK. Waze RT returns no data when the area has no active crowdsourced incidents.

## Files

| File | Purpose |
|------|---------|
| `waze.proto` | Wire-compatible proto2 schema (upstream verbatim) |
| `waze_client.py` | Complete Python client (~340 LOC) |
| `requirements.txt` | `grpcio-tools`, `protobuf`, `requests` |
| `Dockerfile` | Compiles proto at build time, runs client |
| `docker-compose.yml` | `waze-test` service with León defaults |
