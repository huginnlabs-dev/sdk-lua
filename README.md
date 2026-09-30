# dataflow.lua — HuginnLabs Dataflow SDK for Lua

Runtime tracing for Lua services (OpenResty, game scripting, ETL glue).
Completed events batch in memory and ship to the Dataflow REST ingest
endpoint (`POST /api/v1/ingest`) via `curl` on PATH. No dependencies beyond
Lua 5.1+/LuaJIT and `curl`.

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
| `DATAFLOW_HTTP_URL` | HTTP base for the startup manifest |
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

## Tests

`lua tests/test_dataflow.lua` from the repository root — plain assert-based
runner over the pure helpers only (statement summary, clipping, URL-to-span
naming).

## Versioning

The SDK version lives in `_VERSION` in `dataflow.lua`; changes are recorded
in `CHANGELOG.md`.
