# Changelog

## 0.5.0 — 2026-09-30

### Added
- Crash capture: `dataflow.capture(fn, ...)` runs `fn` under `xpcall` with a `debug.traceback` message handler and records any error on the current span (a synthetic `exception` span, ended immediately, when none is open): status `500`, `error_message` clipped to 500 bytes, metadata `error.stack` = the traceback clipped to 8192 bytes — then returns `false, err` (the caller decides; nothing is re-raised). `dataflow.capture_or_raise(fn, ...)` records the same and re-raises with `error(err, 2)`. Recording is pcall-wrapped best-effort and never masks the original error; an unconfigured/disabled SDK degrades to a plain `xpcall` passthrough; success forwards `true` + all of `fn`'s results (pack/unpack keeps Lua 5.1 `xpcall` compatibility).
- Pure `dataflow.clip_text(s, n)` byte cap (first `n` bytes, nil-safe), now also backing `clip_statement`'s 200-char clip.
- `tests/test_dataflow.lua` covers `clip_text` and the capture paths (synthetic vs. current-span recording, `status_code`/`error_message`/`error.stack` fields, success and disabled passthrough) via a test-only `dataflow._test_buffer` hook; run manually — no Lua toolchain in CI.

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
