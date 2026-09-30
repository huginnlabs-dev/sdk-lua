# Changelog

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
