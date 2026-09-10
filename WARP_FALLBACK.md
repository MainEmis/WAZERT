# Cloudflare WARP — IP Fallback for VPS Deployments

When the VPS IP gets rate-limited by Waze (HTTP 429), WARP switches outbound
traffic to a Cloudflare IP, giving the register flow a clean slate.

## Install (Ubuntu/Debian)

```bash
curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg | sudo apt-key add -
echo "deb https://pkg.cloudflareclient.com/ $(lsb_release -cs) main" \
  | sudo tee /etc/apt/sources.list.d/cloudflare-client.list
sudo apt update && sudo apt install -y cloudflare-warp
warp-cli register
```

## Usage

```bash
warp-cli connect     # enable — traffic exits via Cloudflare IP
warp-cli disconnect  # disable — back to VPS IP
warp-cli status      # check current state
```

## Programmatic toggle on 429

Add to `api.py` inside `_global_429()`:

```python
import subprocess

def _activate_warp():
    """Switch to Cloudflare IP when Waze bans the VPS IP."""
    subprocess.run(["warp-cli", "connect"], check=True, timeout=10)
    log.warning("[GLOBAL] 429 detected — WARP activated (Cloudflare IP)")

def _global_429(cooldown: float = 30.0):
    global _reg_429_until
    with _reg_429_lock:
        until = time.monotonic() + cooldown
        if until > _reg_429_until:
            _reg_429_until = until
            log.warning("[GLOBAL] 429 from Waze — all regions pausing %.0fs", cooldown)
    try:
        _activate_warp()
    except Exception:
        pass  # WARP not installed or failed — cooldown still applies
```

## Notes

- WARP adds ~10-20ms latency while active (irrelevant for a fallback path).
- The Waze register rate limit is IP-based and triggered by bursts, not
  sustained rate. In normal operation (keeper cycle = ~3 registers/min across
  all regions) this fallback should rarely activate.
- After WARP is active, leave it connected until the next planned maintenance
  window — disconnecting mid-session has no benefit since sessions are already
  rotating.
- Not implemented in code yet — activate manually or wire into `_global_429()`
  when needed.
