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

## Tests

`lua tests/test_dataflow.lua` from the repository root — plain assert-based
runner over the pure helpers only (statement summary, clipping, URL-to-span
naming, route extraction, catalog JSON).

## Versioning

The SDK version lives in `_VERSION` in `dataflow.lua`; changes are recorded
in `CHANGELOG.md`.
