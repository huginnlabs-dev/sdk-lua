-- tests/test_dataflow.lua — tests for the pure helpers and the crash-capture
-- recording paths.
--
-- Run from the repository root:
--   lua tests/test_dataflow.lua
--
-- Pure helpers are covered directly (stmt_summary, clip_statement, clip_text,
-- http_span_name, scan_extract, scan_json, logs_json); the crash-capture and
-- log-recording paths run via the test-only buffer hooks
-- (dataflow._test_buffer, dataflow._test_log_buffer, plus a counting stub
-- swapped in for dataflow.flush_logs) and a re-configure() with fake
-- DATAFLOW_* values — no server, no outbound curl (the startup manifest is
-- sent once at require time and later configure() calls skip it). The
-- delivery and directory-walking paths need a live process and a reachable
-- server, so they stay out of this runner.

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

-- ---------------------------------------------------------------------------
-- clip_text: generic byte cap (error_message 500 / error.stack 8192)

eq(dataflow.clip_text("hello world", 5), "hello", "clip_text: clipped from the top")
eq(dataflow.clip_text("hi", 5), "hi", "clip_text: short string intact")
eq(dataflow.clip_text("hello", 0), "", "clip_text: zero cap -> empty")
eq(dataflow.clip_text(nil, 5), "", "clip_text: nil -> empty")
eq(dataflow.clip_text(12345, 3), "123", "clip_text: non-string coerced")
eq(#dataflow.clip_text(string.rep("x", 9000), 8192), 8192, "clip_text: 8192 cap")

-- ---------------------------------------------------------------------------
-- capture / capture_or_raise: crash capture with error.stack
--
-- Recording needs an enabled SDK: reconfigure with fake env values (the
-- startup manifest was already sent at require time, so this stays
-- curl-free) and point the event buffer at a test table.

os.getenv = function(k)
  if k == "DATAFLOW_ENDPOINT" then return "http://127.0.0.1:1" end
  if k == "DATAFLOW_API_KEY" then return "test-key" end
  if k:sub(1, 9) == "DATAFLOW_" then return nil end
  return real_getenv(k)
end
dataflow.configure()

local events = dataflow._test_buffer({})

-- error path, no open span: false + original error, synthetic exception span
local ok, err = dataflow.capture(function() error("boom", 0) end)
eq(ok, false, "capture: error -> false")
eq(err, "boom", "capture: original error object forwarded")
eq(#events, 1, "capture: synthetic exception span recorded")
local ev = events[1]
eq(ev:find('"name":"exception"', 1, true) ~= nil, true, "capture: synthetic span named exception")
eq(ev:find('"type":"FUNCTION_CALL"', 1, true) ~= nil, true, "capture: synthetic span is FUNCTION_CALL")
eq(ev:find('"status_code":500', 1, true) ~= nil, true, "capture: status 500")
eq(ev:find('"error_message":"boom"', 1, true) ~= nil, true, "capture: error_message carries the raw error")
eq(ev:find('"error.stack":"', 1, true) ~= nil, true, "capture: error.stack metadata present")
eq(ev:find("stack traceback", 1, true) ~= nil, true, "capture: error.stack carries trace frames")

-- success path: true + all fn results, nothing recorded
local r1, r2, r3 = dataflow.capture(function(a, b) return a + b, "tag" end, 2, 3)
eq(r1, true, "capture: success -> true")
eq(r2, 5, "capture: forwards fn results (first)")
eq(r3, "tag", "capture: forwards fn results (second)")
eq(#events, 1, "capture: success records nothing")

-- capture_or_raise: records, then re-raises at the call site
local cro_ok, cro_err = pcall(dataflow.capture_or_raise, function() error("boom3", 0) end)
eq(cro_ok, false, "capture_or_raise: error propagates to the caller")
eq(tostring(cro_err):find("boom3", 1, true) ~= nil, true, "capture_or_raise: original error text survives")
eq(#events, 2, "capture_or_raise: recorded before raising")

-- disabled SDK: plain xpcall passthrough, nothing recorded
os.getenv = function(k)
  if k == "DATAFLOW_DISABLED" then return "true" end
  if k:sub(1, 9) == "DATAFLOW_" then return nil end
  return real_getenv(k)
end
dataflow.configure()
local dis_ok, dis_err = dataflow.capture(function() error("boom4", 0) end)
eq(dis_ok, false, "capture disabled: still false + err")
eq(dis_err, "boom4", "capture disabled: original error forwarded")
eq(#events, 2, "capture disabled: nothing recorded")

-- recording on the CURRENT span: the open server span is reused and capture
-- neither ends nor replaces it
os.getenv = function(k)
  if k == "DATAFLOW_ENDPOINT" then return "http://127.0.0.1:1" end
  if k == "DATAFLOW_API_KEY" then return "test-key" end
  if k:sub(1, 9) == "DATAFLOW_" then return nil end
  return real_getenv(k)
end
dataflow.configure()
local srv = dataflow.start_server_span("GET /boom")
local cur_ok, cur_err = dataflow.capture(function() error("boom5", 0) end)
eq(cur_ok, false, "capture on current span: false")
eq(cur_err, "boom5", "capture on current span: original error forwarded")
eq(#events, 2, "capture on current span: capture does not end the open span")
srv:end_()
eq(#events, 3, "capture on current span: event ships when the owner ends the span")
local ev2 = events[3]
eq(ev2:find('"name":"GET /boom"', 1, true) ~= nil, true, "capture on current span: keeps the open span's name")
eq(ev2:find('"type":"HTTP_SERVER"', 1, true) ~= nil, true, "capture on current span: keeps the span type")
eq(ev2:find('"status_code":500', 1, true) ~= nil, true, "capture on current span: status 500")
eq(ev2:find('"error_message":"boom5"', 1, true) ~= nil, true, "capture on current span: error_message")
eq(ev2:find('"error.stack":"', 1, true) ~= nil, true, "capture on current span: error.stack metadata")

-- ---------------------------------------------------------------------------
-- log capture: correlation, level normalization, fields, buffer bounds and
-- the threshold flush. Runs against the enabled fake env configured above;
-- dataflow._test_log_buffer points the log buffer at a local table and a
-- counting stub replaces dataflow.flush_logs, so no outbound curl runs.

local logs = dataflow._test_log_buffer({})
local log_flush_calls = 0
local real_flush_logs = dataflow.flush_logs
dataflow.flush_logs = function() log_flush_calls = log_flush_calls + 1 end

-- correlation with the enclosing span + entry JSON shape
dataflow.trace("job.Logging", function()
  dataflow.info("hello log", { user = "u1" })
end)
eq(#logs, 1, "log: entry recorded in the test buffer")
local lg = logs[1]
eq(lg:find('"level":"info"', 1, true) ~= nil, true, "log: level normalized to info")
eq(lg:find('"message":"hello log"', 1, true) ~= nil, true, "log: message recorded")
eq(lg:find('"fields":{"user":"u1"}', 1, true) ~= nil, true, "log: fields encoded as an object")
eq(lg:find('"service_name":"', 1, true) ~= nil, true, "log: service name present")
eq(lg:find('"timestamp":1', 1, true) ~= nil, true, "log: unix-ms timestamp present")
local span_event = events[#events]
local corr_trace = span_event:match('"trace_id":"(%x+)"')
local corr_span = span_event:match('"span_id":"(%x+)"')
eq(lg:find('"trace_id":"' .. corr_trace .. '"', 1, true) ~= nil, true, "log: trace id from the current span")
eq(lg:find('"span_id":"' .. corr_span .. '"', 1, true) ~= nil, true, "log: span id from the current span")
eq(log_flush_calls, 0, "log: below the threshold nothing is flushed")

-- envelope: pure body builder (mirrors scan_json)
eq(dataflow.logs_json({}), '{"logs":[]}', "log: logs_json empty -> empty array")
eq(dataflow.logs_json(nil), '{"logs":[]}', "log: logs_json nil -> empty array")
local envelope = dataflow.logs_json({ lg })
eq(envelope:find('{"logs":[', 1, true), 1, "log: logs_json opens the logs array")
eq(envelope:sub(-2), "]}", "log: logs_json closes the logs array")
eq(envelope:find('"message":"hello log"', 1, true) ~= nil, true, "log: logs_json carries the entry")
local many = {}
for i = 1, 1001 do many[i] = '"e' .. i .. '"' end
local big = dataflow.logs_json(many)
eq(big:find('"e1000"', 1, true) ~= nil, true, "log: logs_json keeps 1000 entries")
eq(big:find('"e1001"', 1, true), nil, "log: logs_json caps the batch at 1000")

-- level normalization: case, aliases, unknown, nil, surrounding whitespace
dataflow.log("WARN", "w")
dataflow.log("warning", "w2")
dataflow.log("ERR", "e")
dataflow.log("critical", "c")
dataflow.log("verbose", "v")
dataflow.log(nil, "n")
dataflow.log(" debug ", "d")
eq(logs[2]:find('"level":"warn"', 1, true) ~= nil, true, "log: WARN -> warn")
eq(logs[3]:find('"level":"warn"', 1, true) ~= nil, true, "log: warning -> warn")
eq(logs[4]:find('"level":"error"', 1, true) ~= nil, true, "log: ERR -> error")
eq(logs[5]:find('"level":"error"', 1, true) ~= nil, true, "log: critical -> error")
eq(logs[6]:find('"level":"info"', 1, true) ~= nil, true, "log: unknown level -> info")
eq(logs[7]:find('"level":"info"', 1, true) ~= nil, true, "log: nil level -> info")
eq(logs[8]:find('"level":"debug"', 1, true) ~= nil, true, "log: whitespace trimmed")

-- convenience wrappers cover all four levels
dataflow.debug("dbg", nil)
dataflow.info("inf", nil)
dataflow.warn("wrn", nil)
dataflow.error("err", nil)
eq(logs[9]:find('"level":"debug"', 1, true) ~= nil, true, "log: dataflow.debug")
eq(logs[10]:find('"level":"info"', 1, true) ~= nil, true, "log: dataflow.info")
eq(logs[11]:find('"level":"warn"', 1, true) ~= nil, true, "log: dataflow.warn")
eq(logs[12]:find('"level":"error"', 1, true) ~= nil, true, "log: dataflow.error")

-- fields: stringified values, 50-entry cap, non-table -> empty object
dataflow.info("with fields", { n = 42, b = true, s = "v" })
local fentry = logs[13]
eq(fentry:find('"n":"42"', 1, true) ~= nil, true, "log: number field stringified")
eq(fentry:find('"b":"true"', 1, true) ~= nil, true, "log: boolean field stringified")
eq(fentry:find('"s":"v"', 1, true) ~= nil, true, "log: string field kept")
local wide = {}
for i = 1, 60 do wide[string.format("f%02d", i)] = i end
dataflow.info("wide", wide)
local wentry = logs[14]
eq(wentry:find('"f50":"50"', 1, true) ~= nil, true, "log: 50th field kept")
eq(wentry:find('"f51"', 1, true), nil, "log: fields capped at 50")
dataflow.info("no fields", nil)
eq(logs[15]:find('"fields":{}', 1, true) ~= nil, true, "log: nil fields -> empty object")

-- threshold: the 50th buffered line fires exactly one flush (stubbed)
for i = 1, 35 do dataflow.info("bulk " .. i, nil) end
eq(#logs, 50, "log: buffer holds every entry below the cap")
eq(log_flush_calls, 1, "log: threshold flush fired once at 50")
eq(dataflow.log_stats().buffered, 50, "log: log_stats counts buffered")
eq(dataflow.log_stats().dropped, 0, "log: nothing dropped so far")

-- drop-oldest at the 1024 cap (counting stub still swallows the flush)
local seeded = {}
for i = 1, 1023 do seeded[i] = '"seed' .. i .. '"' end
dataflow._test_log_buffer(seeded)
dataflow.info("overflow a", nil)  -- buffer reaches 1024
dataflow.error("overflow b", nil) -- oldest line dropped and counted
eq(#seeded, 1024, "log: buffer stays at the 1024 cap")
eq(seeded[1], '"seed2"', "log: drop-oldest removed the oldest line")
eq(seeded[1023]:find('"message":"overflow a"', 1, true) ~= nil, true, "log: order preserved")
eq(seeded[1024]:find('"message":"overflow b"', 1, true) ~= nil, true, "log: newest line at the tail")
eq(dataflow.log_stats().buffered, 1024, "log: log_stats sees the full buffer")
eq(dataflow.log_stats().dropped, 1, "log: drop counter incremented once")

-- disabled SDK: recording and flushing are no-ops (buffer untouched, no curl)
dataflow.flush_logs = real_flush_logs
os.getenv = function(k)
  if k == "DATAFLOW_DISABLED" then return "true" end
  if k:sub(1, 9) == "DATAFLOW_" then return nil end
  return real_getenv(k)
end
dataflow.configure()
dataflow.error("not recorded", nil)
dataflow.log(nil, nil, nil)
dataflow.flush_logs() -- real flush: disabled -> no-op
eq(#seeded, 1024, "log disabled: buffer untouched")
eq(dataflow.log_stats().buffered, 1024, "log disabled: buffered count unchanged")
eq(dataflow.log_stats().dropped, 1, "log disabled: drop counter unchanged")

os.getenv = real_getenv

print(string.format("dataflow transport tests: %d passed, %d failed", passed, failed))
if failed > 0 then os.exit(1) end
