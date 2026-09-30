# Changelog

## 0.4.0 — 2026-09-30

### Added
- Static route scanner: `dataflow.scan(dir, opts)` walks `*.lua` sources (skipping `.git/`, `deps/`, `t/`; `io.popen` `find`/`dir` for the listing), pure helpers `dataflow.scan_extract(filename, source)` / `dataflow.scan_json(catalog)`, and best-effort `dataflow.scan_post(catalog, opts)` posting `POST /api/v1/catalog` (same curl mechanics as the manifest; ≤1000 routes; base URL `opts.url` > `DATAFLOW_HTTP_URL` > URL-form `DATAFLOW_ENDPOINT`, key `opts.api_key` / `DATAFLOW_API_KEY`).
- Recognized OpenResty idioms: `r:get/post/put/delete/patch("/path", handler)` (lua-resty-route), one level of `route("/base", function(r) ... end)` prefixing via do-end tracking, `verb = { ["/path"] = handler }` dispatch tables (best effort) and `ngx.var.uri == "/path"` guards (emitted as `GET` with an empty handler); `:id` / `{id}` path params kept as written, commented-out code ignored, duplicates collapsed.
- CLI `scan_cli.lua` (`lua scan_cli.lua --dir . [--service ...] [--url ...] [--api-key ...] [--print]`); a missing URL or API key is a skip with a message, not an error.
- `scan_extract` / `scan_json` tests in `tests/test_dataflow.lua` (run manually — no Lua toolchain in CI).

## 0.3.0 — 2026-09-30

### Added
- `dataflow.http_span(method, url)` / `dataflow.db_span(system, statement)`: best-effort transport span handles emitting `HTTP_CLIENT` / `DB_QUERY` events, with explicit `:finish()` (GC `__gc` only as a safety net), `:set_status()`, `:record_error()` and `:trace_id()` for `X-Dataflow-Trace-Id` header propagation.
- Pure helpers `dataflow.stmt_summary(sql)` (`<VERB> <table>` naming), `dataflow.clip_statement(sql)` (single-spaced, clipped to 200 chars) and `dataflow.http_span_name(method, url)`; covered by `tests/test_dataflow.lua` (run manually — no Lua toolchain in CI).
- `README.md` documenting the SDK and the transport spans (lua-resty-http + redis sketches).

## 0.2.0 — 2026-09-30

### Added
- Service manifest reported once at startup via `POST /api/v1/manifest` (best-effort curl, mirrors the Go SDK; OpenResty detected via the `ngx` global).

## 0.1.0 — 2026-09-28

### Added
- Initial SDK: `configure`, `trace`, `trace_kind`, `start_server_span`, `current_span`, span attributes/payloads, PII classification of field names, curl-based REST ingest (`POST /api/v1/ingest`).
