# Operations: TLS termination for the hub

> The ForgeWire hub speaks plain HTTP. **Always** put a TLS-terminating
> reverse proxy in front of any hub reachable beyond a single trusted host.

This guide gives copy-pasteable recipes for the three most common options:
**Caddy** (zero-config Let's Encrypt), **nginx + certbot**, and **Tailscale
Funnel** (zero public exposure, leverages your tailnet identity).

The hub binds to `127.0.0.1:8765` in every recipe below. Keep it that way.
The reverse proxy listens on the public interface and forwards to localhost.

> **Bind change required**: the install scripts default to `0.0.0.0`. When
> putting a proxy in front, edit the service unit / NSSM args to bind
> `--host 127.0.0.1` instead.

---

## Option 1 — Caddy (recommended for most homelabs)

Caddy auto-provisions and renews Let's Encrypt certificates. Zero config
beyond the hostname.

```caddyfile
# /etc/caddy/Caddyfile
hub.example.com {
    encode zstd gzip
    reverse_proxy 127.0.0.1:8765 {
        # SSE: keep connections open and don't buffer.
        flush_interval -1
        transport http {
            read_buffer 8k
            response_header_timeout 0s
        }
    }

    # Optional: lock the proxy down to a specific source range
    # @lan remote_ip 10.0.0.0/8 192.168.0.0/16 fd00::/8
    # handle @lan { reverse_proxy 127.0.0.1:8765 }
    # respond 403
}
```

Reload:

```bash
sudo systemctl reload caddy
```

That's it. Caddy will obtain a cert on first request and renew it before
expiry. Point the VS Code extension / CLI at `https://hub.example.com`.

---

## Option 2 — nginx + certbot

```nginx
# /etc/nginx/sites-available/forgewire
server {
    listen 80;
    server_name hub.example.com;
    location /.well-known/acme-challenge/ { root /var/www/certbot; }
    location / { return 301 https://$host$request_uri; }
}

server {
    listen 443 ssl http2;
    server_name hub.example.com;

    ssl_certificate     /etc/letsencrypt/live/hub.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/hub.example.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # Required for SSE: long-lived connections, no buffering.
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 24h;

    client_max_body_size 32m;

    location / {
        proxy_pass http://127.0.0.1:8765;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";   # keep-alive for SSE
    }
}
```

Issue cert:

```bash
sudo certbot --nginx -d hub.example.com
```

`certbot.timer` (installed by default on Debian/Ubuntu) handles renewal.

---

## Option 3 — Tailscale Funnel / Serve

If your hub and your dispatchers are all on the same tailnet, you don't
need public TLS at all — Tailscale already encrypts end-to-end and gives
each node a stable `*.ts.net` hostname with a managed cert.

```bash
# On the hub host
tailscale serve --bg --https=8765 http://127.0.0.1:8765

# Get the URL
tailscale serve status
# https://hub.tailnet-name.ts.net:8765 → http://127.0.0.1:8765
```

To expose the hub to a single non-tailnet collaborator (e.g. for a one-off
demo):

```bash
tailscale funnel --bg --https=443 http://127.0.0.1:8765
```

Funnel is rate-limited and disabled by default; turn it back off when the
demo is over with `tailscale funnel reset`.

---

## Verifying the chain

From any client:

```bash
curl -fsS https://hub.example.com/healthz
# {"status":"ok","protocol_version":2,...}

curl -fsS -H "Authorization: Bearer <token>" \
    https://hub.example.com/runners
```

If `/healthz` works but `/runners` returns `401`, the proxy is fine and the
token is wrong. If `/healthz` returns `502`, the proxy can't reach the hub
on `127.0.0.1:8765` — check that the service is bound to localhost and
running.

---

## Hardening checklist

- [ ] Hub is bound to `127.0.0.1`, not `0.0.0.0`.
- [ ] Reverse proxy enforces TLS 1.2+ only.
- [ ] HSTS is set (Caddy does this by default).
- [ ] Token file is readable only by the service account (the install
      scripts set this on Windows; on Linux/macOS verify
      `chmod 0640 /etc/forgewire/hub.token` and group ownership).
- [ ] Firewall blocks direct access to port 8765 from the public interface.
- [ ] Backups of `--db-path` are running on a schedule (see
      [`service-install.md`](service-install.md#backups)).

---

## Why ForgeWire doesn't terminate TLS itself

A reverse proxy is one moving part the operator already understands and
already monitors. Building TLS into the hub would mean reinventing cert
rotation, OCSP stapling, and HTTP/2 keepalive handling — all problems
Caddy/nginx/Traefik solve much better. The hub stays focused on the
signed dispatch protocol; the proxy handles the edge.
