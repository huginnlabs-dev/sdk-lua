-- tests/test_dataflow.lua — pure-function tests for the transport helpers.
--
-- Run from the repository root:
--   lua tests/test_dataflow.lua
--
-- Only pure functions are covered (stmt_summary, clip_statement,
-- http_span_name, scan_extract, scan_json); the span handle, delivery and
-- directory-walking paths need a live process and a reachable server, so
-- they stay out of this runner.

-- Locate the module relative to this script so `lua tests/test_dataflow.lua`
-- works from any working directory.
local script_dir = (arg and arg[0] or ""):match("^(.*)[/\\][^/\\]+$") or "."
package.path = script_dir .. "/../?.lua;" .. package.path

-- configure() runs at require time and would best-effort POST a startup
-- manifest if DATAFLOW_* variables are set, so neutralize them for the load.
local real_getenv = os.getenv
os.getenv = function(k)
  if k:sub(1, 9) == "DATAFLOW_" then return nil end
  return real_getenv(k)
end
local dataflow = require("dataflow")
os.getenv = real_getenv

local passed, failed = 0, 0

local function eq(actual, expected, label)
  if actual == expected then
    passed = passed + 1
  else
    failed = failed + 1
    print("FAIL " .. label .. ": expected " .. string.format("%q", expected) ..
        ", got " .. string.format("%q", tostring(actual)))
  end
end

-- stmt_summary: "<VERB> <table>"
eq(dataflow.stmt_summary("SELECT * FROM users WHERE id = 1"), "SELECT users", "select from")
eq(dataflow.stmt_summary("select id, name from public.users"), "SELECT users", "lowercase + schema strip")
eq(dataflow.stmt_summary('SELECT * FROM "public"."items"'), "SELECT items", "quoted schema stripped")
eq(dataflow.stmt_summary("INSERT INTO audit_log (a) VALUES (1)"), "INSERT audit_log", "insert into")
eq(dataflow.stmt_summary("INSERT INTO t1 (a) SELECT a FROM t2"), "INSERT t1", "first INTO wins over later FROM")
eq(dataflow.stmt_summary("UPDATE public.items SET stock = 3"), "UPDATE items", "update verb + table")
eq(dataflow.stmt_summary("DELETE FROM carts"), "DELETE carts", "delete from")
eq(dataflow.stmt_summary("CREATE TABLE IF NOT EXISTS public.orders (id int)"), "CREATE orders", "create if not exists")
eq(dataflow.stmt_summary("DROP TABLE IF EXISTS sessions"), "DROP sessions", "drop if exists")
eq(dataflow.stmt_summary("TRUNCATE TABLE public.cache"), "TRUNCATE cache", "truncate table")
eq(dataflow.stmt_summary("CREATE INDEX idx_users ON users (id)"), "CREATE", "no table keyword -> verb only")
eq(dataflow.stmt_summary("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"), "SELECT t1", "first FROM wins over JOIN")
eq(dataflow.stmt_summary("-- lookup\nSELECT 1 FROM x"), "SELECT x", "line comment stripped")
eq(dataflow.stmt_summary("/* seed */ SELECT 1 FROM x"), "SELECT x", "block comment stripped")
eq(dataflow.stmt_summary("run_maintenance now"), "RUN_MAINTENANCE", "no verb -> first word uppercase")
eq(dataflow.stmt_summary("   "), "", "blank -> empty")
eq(dataflow.stmt_summary(nil), "", "nil -> empty")

-- clip_statement: single-spaced, <= 200 chars
eq(dataflow.clip_statement("SELECT\n\t*\n  FROM   users"), "SELECT * FROM users", "whitespace collapsed")
eq(dataflow.clip_statement("  padded  "), "padded", "trimmed")
eq(#dataflow.clip_statement(string.rep("a", 500)), 200, "clipped to 200")
eq(#dataflow.clip_statement(string.rep("a", 200)), 200, "exactly 200 kept intact")
eq(dataflow.clip_statement(nil), "", "nil -> empty")

-- http_span_name: "<METHOD> <host>/<path>"
eq(dataflow.http_span_name("GET", "https://api.example.com/v1/users?id=1"), "GET api.example.com/v1/users", "url with query dropped")
eq(dataflow.http_span_name("POST", "http://localhost:8080/api/v1/ingest"), "POST localhost:8080/api/v1/ingest", "url with port")
eq(dataflow.http_span_name("get", "https://user:secret@example.com/x"), "GET example.com/x", "userinfo stripped, method uppercased")
eq(dataflow.http_span_name("GET", "https://example.com"), "GET example.com", "bare host")
eq(dataflow.http_span_name("GET", "https://example.com/"), "GET example.com", "root path dropped")
eq(dataflow.http_span_name("PUT", "/local/path"), "PUT /local/path", "relative path")
eq(dataflow.http_span_name("GET", "localhost:6379"), "GET localhost:6379", "scheme-less host:port")
eq(dataflow.http_span_name("GET", ""), "GET", "empty url")
eq(dataflow.http_span_name(nil, nil), "", "nil method and url")

-- ---------------------------------------------------------------------------
-- scan_extract: static route extraction (lua-resty-route, dispatch tables,
-- ngx.var.uri guards)

local function routes_repr(routes)
  local out = {}
  for i, r in ipairs(routes) do
    out[i] = r.method .. " " .. r.path .. " -> " .. r.handler
  end
  return table.concat(out, "; ")
end

-- lua-resty-route, flat r:verb calls
local flat = dataflow.scan_extract("app/routes.lua", [==[
local r = require("resty.route").new()
r:get("/api/orders/:id", orders.show)
r:post("/api/orders", orders.create)
r:put("/api/orders/:id", orders.update)
r:delete("/api/orders/:id", orders.destroy)
r:patch("/api/orders/:id", orders.patch)
]==])
eq(#flat, 5, "route flat: count")
eq(routes_repr(flat),
  "GET /api/orders/:id -> orders.show; POST /api/orders -> orders.create; " ..
  "PUT /api/orders/:id -> orders.update; DELETE /api/orders/:id -> orders.destroy; " ..
  "PATCH /api/orders/:id -> orders.patch",
  "route flat: methods, paths, handlers")
eq(flat[1].source_file, "app/routes.lua", "route flat: source_file passthrough")

-- route("/base", function(r) ... end): one level of prefix
local nested = dataflow.scan_extract("app/api.lua", [==[
route("/api", function(r)
  r:get("/users", users.list)
  r:post("/users", users.create)
end)
r:get("/health", health.check)
]==])
eq(routes_repr(nested),
  "GET /api/users -> users.list; POST /api/users -> users.create; GET /health -> health.check",
  "route nested: one-level prefix, restored after end)")

-- dispatch tables (verb-keyed bracket assignments)
local dispatch = dataflow.scan_extract("app/handlers.lua", [==[
local routes = {
  get = {
    ["/items"] = items.list,
    ["/items/:id"] = items.show,
  },
  post = {
    ["/items"] = items.create,
  },
}
]==])
eq(routes_repr(dispatch),
  "GET /items -> items.list; GET /items/:id -> items.show; POST /items -> items.create",
  "dispatch table: multi-line verb blocks")

-- single-line dispatch table
local inline = dataflow.scan_extract("app/inline.lua", [==[
local t = { get = { ["/ping"] = ping.handler } }
]==])
eq(routes_repr(inline), "GET /ping -> ping.handler", "dispatch table: single line")

-- ngx.var.uri guards -> GET with an empty handler, deduplicated
local guard = dataflow.scan_extract("app/status.lua", [==[
if ngx.var.uri == "/status" then
  ngx.say("ok")
end
if ngx.var.uri == "/status" then
  ngx.say("again")
end
]==])
eq(routes_repr(guard), "GET /status -> ", "ngx.var.uri guard: GET, empty handler, dedup")

-- handler forms: quoted, anonymous function, table arg; commented-out
-- routes ignored; non-path first args (e.g. redis) ignored
local forms = dataflow.scan_extract("app/forms.lua", [==[
r:get("/a", "orders.show")
r:get("/b", function(r) ngx.say("hi") end)
r:get("/c", { id = true })
-- r:get("/gone", orders.gone)
red:get("session:" .. sid)
]==])
eq(routes_repr(forms), "GET /a -> orders.show; GET /b -> ; GET /c -> ",
  "handler forms + comment + non-path call ignored")
eq(#dataflow.scan_extract("x.lua", nil), 0, "scan_extract: nil source -> empty, no raise")

-- {id} path params kept as written; path must start with "/"
local braces = dataflow.scan_extract("app/braces.lua", [==[
r:get("/users/{id}/orders", orders.by_user)
r:get("relative", nope.handler)
]==])
eq(routes_repr(braces), "GET /users/{id}/orders -> orders.by_user",
  "brace params kept, non-slash path skipped")

-- ---------------------------------------------------------------------------
-- scan_json: catalog body

local body = dataflow.scan_json({
  service_name = 'serv"ice',
  routes = {
    { method = "GET", path = "/api/orders/:id", handler = "orders.show", source_file = "app/routes.lua" },
    { method = "POST", path = "/x", handler = "", source_file = "b.lua" },
  },
})
eq(body,
  '{"service_name":"serv\\"ice","routes":[' ..
  '{"method":"GET","path":"/api/orders/:id","handler":"orders.show","source_file":"app/routes.lua"},' ..
  '{"method":"POST","path":"/x","handler":"","source_file":"b.lua"}]}',
  "scan_json: stable order, escaping")
eq(dataflow.scan_json(nil), '{"service_name":"","routes":[]}', "scan_json: nil catalog -> empty body")
eq(dataflow.scan_json({ service_name = "s" }), '{"service_name":"s","routes":[]}',
  "scan_json: no routes -> empty array")

print(string.format("dataflow transport tests: %d passed, %d failed", passed, failed))
if failed > 0 then os.exit(1) end
