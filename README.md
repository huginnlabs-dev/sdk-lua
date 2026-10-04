# dataflow.lua — HuginnLabs Dataflow SDK for Lua

Runtime tracing for Lua services (OpenResty, game scripting, ETL glue).
Completed events batch in memory and ship to the Dataflow REST ingest
endpoint (`POST /api/v1/ingest`) via `curl` on PATH; application logs ship
the same way (`POST /api/v1/logs`, see [Log capture](#log-capture-060)).
No dependencies beyond Lua 5.1+/LuaJIT and `curl`.

## Quick start

```lua
local dataflow = require("dataflow")

dataflow.configure() -- reads DATAFLOW_* env; call once at startup
dataflow.trace("worker.Sync", function(span)
  span:data("rows", 42)
end)
dataflow.flush()
```

Flushing runs `curl` synchronously (fire-and-forget, 10s timeout) and fires
automatically every 25 buffered events; on OpenResty that briefly blocks the
worker, so keep call frequency low and let the automatic flush do its job.

### Configuration (environment)

| Variable | Meaning |
| --- | --- |
| `DATAFLOW_ENDPOINT` | base URL or host:port of the Dataflow server (required) |
| `DATAFLOW_API_KEY` | ingest API key (required) |
| `DATAFLOW_SERVICE_NAME` | service name (defaults to the hostname) |
| `DATAFLOW_SAMPLE_RATIO` | 0..1, default `1.0` |
| `DATAFLOW_BUFFER_SIZE` | buffer ceiling (default `10000`; v0.x flushes at 25 events) |
| `DATAFLOW_DISABLED` | `"true"` disables recording |
| `DATAFLOW_HTTP_URL` | HTTP base for the startup manifest, the route catalog post and log shipping |
| `DATAFLOW_APP_VERSION` | reported in the startup manifest |

## Transport spans (0.3.0)

Best-effort helpers for outgoing HTTP calls (event type `HTTP_CLIENT`) and
database queries (`DB_QUERY`). Both return a lightweight handle that **must
be finished explicitly with `:finish()`** — Lua GC timing is unreliable, so
durations are only correct when you call it. A `__gc` fallback exists purely
as a last-resort net on runtimes that honor table finalizers; never rely on
it. Finalization is pcall-wrapped and skipped when tracing is disabled, and
the trace id stays available either way so context propagation keeps working.

### Outgoing HTTP (lua-resty-http)

```lua
local http = require("resty.http")
local dataflow = require("dataflow")

local function http_request(method, url, body)
  local span = dataflow.http_span(method, url)
  local httpc = http.new()
  local res, err = httpc:request_uri(url, {
    method = method,
    body = body,
    -- context propagation: downstream services adopt this trace id via
    -- start_server_span(route, incoming_trace_id)
    headers = { ["X-Dataflow-Trace-Id"] = span:trace_id() },
  })
  if not res then
    span:record_error(err)
    span:finish()
    return nil, err
  end
  span:set_status(res.status) -- status_code = HTTP status
  span:finish()
  return res
end
```

The span is named `"<METHOD> <host>/<path>"` (query strings and userinfo are
dropped), `callee_package` is the host, and metadata carries `http.method`
and `http.url`.

### Redis / database (resty.redis)

```lua
local redis = require("resty.redis")
local dataflow = require("dataflow")

local function session_get(sid)
  local span = dataflow.db_span("redis", "GET session:" .. sid)
  local red = redis:new()
  red:set_timeout(2000)
  local ok, err = red:connect("127.0.0.1", 6379)
  if not ok then
    span:record_error(err)
    span:finish()
    return nil, err
  end
  local value, err2 = red:get("session:" .. sid)
  if not value then span:record_error(err2) end
  red:set_keepalive(10000, 100)
  span:finish() -- DB spans leave status_code unset on success
  return value
end
```

`db_span(system, statement)` names the span `"<VERB> <table>"` and works for
any backend (`redis`, `postgres`, `mysql`, ...). Statements are summarized by
pure helpers you can also call directly:

- `dataflow.stmt_summary(sql)` — uppercase first keyword as the verb; table
  from the first `FROM`/`INTO`/`UPDATE`/`TABLE`/`JOIN`, skipping
  `IF [NOT] EXISTS` and schema qualifiers (`public.items` → `items`); a
  non-SQL first word degrades to that word uppercased.
- `dataflow.clip_statement(sql)` — collapses whitespace and clips the
  statement to 200 characters for the `db.statement` metadata.

**Never pass bound values** to `db_span` — statements only:

| statement | span name |
| --- | --- |
| `SELECT * FROM public.users WHERE id = $1` | `SELECT users` |
| `INSERT INTO orders (id) VALUES ($1)` | `INSERT orders` |
| `UPDATE public.items SET stock = $1` | `UPDATE items` |
| `CREATE TABLE IF NOT EXISTS public.audit_log (id int)` | `CREATE audit_log` |

## Route scanning (0.4.0)

`dataflow.scan(dir, opts)` walks `*.lua` files under `dir` (skipping
`.git/`, `deps/`, `t/` and `opts.exclude` entries), extracts declared HTTP
endpoints with line-based pattern matching (pure Lua — no `lpeg`, no
`luasocket`) and returns a catalog table for the server's route catalog:

```lua
local dataflow = require("dataflow")

local catalog = dataflow.scan(".")
-- catalog = { service_name = "...", routes = { { method = "GET",
--   path = "/api/orders/:id", handler = "orders.show",
--   source_file = "app/routes.lua" }, ... } }

dataflow.scan_post(catalog) -- best-effort POST /api/v1/catalog
```

Recognized OpenResty idioms — all best-effort; a route needs a
string-literal path starting with `/`, so client calls like
`red:get("session:" .. sid)` never match:

| source pattern | extracted |
| --- | --- |
| `r:get("/path", handler)` — `post`/`put`/`delete`/`patch` too (lua-resty-route) | `METHOD /path handler` |
| `route("/base", function(r) r:get("/x", h) end)` | one level of prefix via do-end tracking → `GET /base/x` |
| `get = { ["/path"] = handler }` dispatch tables | `GET /path handler` (brace-depth tracked, best effort) |
| `if ngx.var.uri == "/path" then` | heuristic → `GET /path` with an empty handler |

Path parameters keep their written form (`:id` or `{id}`). The handler is
the identifier or string where visible (anonymous functions and table
arguments yield `""`), and `source_file` is the path relative to the scan
directory. Commented-out code is ignored; duplicates collapse by
method + path (first handler wins, upgrading an empty one). `scan` sorts
routes by `(source_file, method, path)` and caps at 1000 like the server.

The pieces (all pcall-wrapped — nothing raises into the caller):

- `dataflow.scan_extract(filename, source)` — pure; scan one source string,
  returns the route list. Covered by `tests/test_dataflow.lua`.
- `dataflow.scan_json(catalog)` — pure; stable-order JSON body
  (`{"service_name":...,"routes":[{"method","path","handler","source_file"}]}`).
- `dataflow.scan(dir, opts)` — walks the directory via `io.popen` (`find`
  on POSIX, `dir /s /b` on Windows — the same external-tool approach as the
  curl transport). Returns `nil, reason` for a bad directory.
  `opts.service_name` overrides `DATAFLOW_SERVICE_NAME` / the directory
  basename.
- `dataflow.scan_post(catalog, opts)` — posts via `curl` exactly like the
  manifest/ingest; returns `true` or `nil, reason`. Base URL:
  `opts.url` > `DATAFLOW_HTTP_URL` > URL-form `DATAFLOW_ENDPOINT`; a bare
  `host:port` endpoint without an HTTP override skips the post. API key:
  `opts.api_key` > `DATAFLOW_API_KEY`.

### CLI

```
lua scan_cli.lua --dir . [--service name] [--url base] [--api-key key] [--print]
```

`--print` prints the JSON body and skips the post. A missing URL or API key
is a skip (message on stderr, exit code 0), not an error; a failing scan
(bad directory) exits 1 and a usage error exits 2.

## Crash capture (0.5.0)

`dataflow.capture(fn, ...)` runs `fn` under `xpcall` with a `debug.traceback`
message handler and records any error before handing it back to the caller:

- on the current span — an `HTTP_SERVER` span opened with `start_server_span`
  in an OpenResty access/content phase, or the open `trace` span — status
  `500`, `error_message` clipped to 500 bytes and metadata `error.stack` =
  the traceback clipped to 8192 bytes (from the top);
- on a synthetic `exception` span (ended immediately so the event ships)
  when no span is open;
- then **returns `false, err`** — the original error object is handed back,
  never re-raised: the caller decides what to do (render an error page, log,
  re-raise). `dataflow.capture_or_raise(fn, ...)` records the same way and
  then raises `error(err, 2)` at the call site for handlers that must let
  the crash propagate.

Recording is best-effort (pcall-wrapped, never masks the original error) and
degrades to a plain `xpcall` passthrough when the SDK is unconfigured or
`DATAFLOW_DISABLED=true`. On success `capture` forwards `true` plus all of
`fn`'s results.

```lua
local dataflow = require("dataflow")

-- OpenResty content phase: capture decides the response, nothing re-raised
local function content()
  local span = dataflow.start_server_span(ngx.var.uri,
    ngx.var.http_x_dataflow_trace_id)
  local ok = dataflow.capture(render_page)
  if not ok then
    -- the span now carries status 500 + error_message + error.stack
    ngx.status = 500
    ngx.say("internal error")
  elseif ngx.status >= 400 then
    span:set_status(ngx.status)
  end
  span:end_()
end

-- access phase: the crash must propagate (OpenResty renders the error page)
local ok, session = dataflow.capture_or_raise(verify_session)
```

`dataflow.clip_text(s, n)` is the pure byte cap behind the limits (first `n`
bytes, nil-safe); `clip_statement` reuses it for the 200-char `db.statement`
clip.

## Log capture (0.6.0)

`dataflow.debug/info/warn/error(message, fields)` — and
`dataflow.log(level, message, fields)` for a dynamic level — record
application logs with the current span's `trace_id`/`span_id` (empty when no
span is open), a millisecond timestamp and stringified `fields` (max 50
entries, values coerced with `tostring`). Levels normalize to
`debug|info|warn|error`: case and surrounding whitespace are folded,
`warning`/`err`/`critical`/`fatal` are accepted aliases, anything unknown
falls back to `info`.

Entries batch in a bounded buffer (1024 lines; when full the oldest is
dropped and counted) and ship to `POST /api/v1/logs` — body
`{"logs":[{"timestamp","level","message","trace_id","span_id","service_name","fields"}]}`,
at most 1000 entries per batch, header `X-Api-Key`. Delivery is
fire-and-forget `curl`, byte-for-byte the same invocation as the ingest
flush: response ignored, never raises, and entries leave the buffer before
the request so a missing `curl` loses at most one batch.

The endpoint base resolves manifest-style: `DATAFLOW_HTTP_URL`, else a
URL-form `DATAFLOW_ENDPOINT`. A bare `host:port` endpoint has no derivable
HTTP base, so logging stays off — as do `DATAFLOW_DISABLED=true` and a
missing API key; in all of those cases recording is a complete no-op.

```lua
local dataflow = require("dataflow")

dataflow.trace("job.Sync", function(span)
  span:data("rows", 42)
  dataflow.warn("cache miss", { key = "users:1" })   -- correlated with job.Sync
  dataflow.error("upstream failed", { attempt = 3 })
  dataflow.flush_logs() -- explicit drain; auto-flush fires at 50 buffered
end)
```

Plain Lua has no timer facility this SDK could rely on (OpenResty timers
live in a separate module), so flushing is **threshold-triggered**: every
recorded line checks the buffer and, once it holds 50 entries, posts
synchronously — the same brief worker-blocking caveat as the 25-event span
flush, so keep call frequency moderate. `dataflow.flush_logs()` forces a
drain (call it at the end of a request or batch like `dataflow.flush()`),
and `dataflow.log_stats()` returns `{ buffered = n, dropped = m }` for
dashboards and tests.

## Tests

`lua tests/test_dataflow.lua` from the repository root — plain assert-based
runner over the pure helpers (statement summary, clipping, `clip_text`,
URL-to-span naming, route extraction, catalog JSON, `logs_json`) and the
crash-capture and log-recording paths, which run against test-only buffer
hooks (`dataflow._test_buffer`, `dataflow._test_log_buffer`, plus a counting
stub swapped in for `dataflow.flush_logs`) after a re-`configure()` with
fake `DATAFLOW_*` values — still no server and no outbound curl (the startup
manifest is sent once at require time).

## Versioning

The SDK version lives in `_VERSION` in `dataflow.lua`; changes are recorded
in `CHANGELOG.md`.

## Performance

The runtime overhead of every Dataflow SDK is measured with a uniform
benchmark: the same ~1 ms CPU-bound HTTP endpoint in three configs (no
instrumentation / Dataflow SDK / OpenTelemetry), one shared load driver,
spans exported live. Numbers for this SDK: measured on an alpine container
with a minimal socket server - baseline 25 rps, **25 rps instrumented** -
the trace wrapper's cost is invisible on this workload. No OTEL Lua SDK
exists, so the OTEL comparison is n/a; the export uses a curl subprocess
per flush (the SDK's design). The full harness lives in the Dataflow
monorepo `bench/`.
