# AMIR Edge Panel — Render adapter

This package wraps the existing Cloudflare Worker-style `worker_AMIR.js` in a Node.js HTTP/WebSocket server for Render.

## Deploy on Render
- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Health check path: `/`

## Environment variables
- `ADMIN_PASSWORD`: admin login password.
- `SESSION_SECRET`: long random secret used by the adapter as `KEY`.
- `UUID`: optional VLESS UUID; use a valid UUID v4 if this mode is enabled.
- `REDIS_URL`: recommended durable KV replacement, using an Upstash Redis TCP URL (`rediss://...`) or another Redis-compatible endpoint.
- Alternative: `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN` can be used instead of `REDIS_URL`.

## Storage warning
Without a configured Redis provider, the adapter falls back to an in-memory map for local smoke tests only. Render's filesystem is ephemeral on free services, and the service can sleep; do not use the in-memory fallback for panel users, subscriptions, or configuration. Render Free Key Value also loses data when it restarts. Upstash Redis has a free tier with usage limits; review its current pricing and quotas before relying on it.

## Compatibility notes
This is a compatibility adapter, not a guarantee that every Cloudflare-specific behavior is identical. It emulates the Worker WebSocketPair response and maps `request.fetcher.connect` to Node TCP/TLS sockets. Cloudflare-only fields such as `request.cf` are populated with best-effort values; the Cloudflare `ASSETS` binding is not available. `startTls()` is not implemented. Syntax checks pass, but test admin login, user management, subscriptions, and each WebSocket client on the deployed service before relying on it.
