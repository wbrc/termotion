# The web renderer does not serve its client assets

The `WebRenderer` exposes only the WebSocket event stream; it no longer embeds or
serves `termotion.js` / `termotion.css`. Delivering the browser client is the
library user's responsibility. The assets remain in the repository's `web/`
directory as the source of truth, and users copy them into their own static
assets (or pin them from a CDN against a matching release tag). This keeps the Go
component's single responsibility — reduce the event stream and fan it out over a
socket — and gives users full control over how, where, and with what caching the
assets are delivered.

## Considered Options

- **Serve the assets from Go (status quo).** Rejected: it bundled presentation
  into the renderer via `//go:embed`, coupled asset URLs to the renderer's mount
  point, and gave users no say over delivery (bundling, CDN, cache headers).
- **Keep the embed but expose it as an opt-in exported `fs.FS`.** Rejected: the
  extra surface earns little once the default no longer serves assets; users who
  want Go to serve the files can trivially serve the `web/` directory themselves.
- **Add a protocol-version handshake so a stale client refuses a newer server.**
  Rejected as over-engineering for a library this size. Instead the JS/CSS and the
  Go wire protocol are released together and users pin matching versions; the JS
  already ignores unknown fields, keeping the protocol additively compatible.

## Consequences

- Breaking change: `ServeHTTP` now upgrades unconditionally as a single-endpoint
  handler. Users mount it directly at the WebSocket path (e.g.
  `http.Handle("/termotion/ws", web)`) instead of at a prefix with
  `http.StripPrefix`, and serve `termotion.js` / `termotion.css` themselves.
- The JSON wire format (`wireSnapshot` / `wireEvent`, the `kind` strings,
  RFC3339 timestamps) is now a public contract between the Go side and any browser
  client. Keep it backward-compatible and release the JS/CSS on the same version.
