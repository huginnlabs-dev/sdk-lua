-- dataflow.lua — HuginnLabs Dataflow SDK for Lua.
--
-- Runtime tracing for Lua services (game scripting, ETL glue, OpenResty).
-- Explicit span passing (Lua has no per-thread contexts); completed events
-- batch in a table and ship to the REST ingest endpoint via curl (required
-- on PATH).
--
--   local dataflow = require("dataflow")
--   dataflow.configure()
--   dataflow.trace("etl.Transform", function(span)
--       span:data("rows", 42)
--   end)
--   local http = dataflow.http_span("GET", "https://api.example.com/v1/users")
--   -- attach header "X-Dataflow-Trace-Id: " .. http:trace_id() to the request
--   http:set_status(200)
--   http:finish()  -- transport spans must be finished explicitly
--   dataflow.flush()
--
-- Env: DATAFLOW_ENDPOINT (http://host:port), DATAFLOW_API_KEY,
-- DATAFLOW_SERVICE_NAME, DATAFLOW_SAMPLE_RATIO, DATAFLOW_BUFFER_SIZE,
-- DATAFLOW_HTTP_URL (HTTP base for the startup manifest), DATAFLOW_APP_VERSION.
--
-- Payload VALUES ship as plaintext JSON in v0.1 (a warning is printed
-- once); field NAMES also travel in `data.fields` metadata for lineage, so
-- adding client-side encryption later stays transparent to the dashboard.

local M = { _VERSION = "0.3.0" }

-- ---------------------------------------------------------------------------
-- settings

local settings = { endpoint = "", api_key = "", service_name = "", sample_ratio = 1.0, buffer_size = 10000, disabled = false }
local warned_plaintext = false
local seq = 0
local manifest_sent = false
local send_manifest -- defined below; fires once from configure()

local function env(k, fallback)
  local v = os.getenv(k)
  if v == nil or v == "" then return fallback end
  return v
end

function M.configure()
  settings.endpoint = env("DATAFLOW_ENDPOINT", "")
  settings.api_key = env("DATAFLOW_API_KEY", "")
  settings.service_name = env("DATAFLOW_SERVICE_NAME", "")
  settings.sample_ratio = tonumber(env("DATAFLOW_SAMPLE_RATIO", "1.0")) or 1.0
  settings.buffer_size = tonumber(env("DATAFLOW_BUFFER_SIZE", "10000")) or 10000
  settings.disabled = env("DATAFLOW_DISABLED", "false") == "true"
  math.randomseed(os.time() + seq)
  send_manifest()
end

local function enabled()
  return not settings.disabled and settings.endpoint ~= "" and settings.api_key ~= ""
end

local function service_name()
  if settings.service_name ~= "" then return settings.service_name end
  local f = io.open("/etc/hostname", "r")
  if f then local h = f:read("*l") or "unknown"; f:close(); return h end
  return "unknown"
end

local function now_ms() return math.floor(os.time() * 1000) end
local clock = os.clock

local function next_seq() seq = seq + 1; return seq end

local function new_id()
  local out = {}
  for i = 1, 32 do out[i] = string.format("%x", math.random(0, 15)) end
  return table.concat(out)
end

local function json_escape(s)
  s = tostring(s or "")
  s = s:gsub('[%c"\\]', function(c)
    local map = { ['"'] = '\\"', ['\\'] = '\\\\', ['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' }
    return map[c] or string.format("\\u%04x", c:byte())
  end)
  return '"' .. s .. '"'
end

-- ---------------------------------------------------------------------------
-- service manifest (one best-effort POST at startup; mirrors the Go SDK)

-- Pure: derives the /api/v1/manifest body from the runtime. OpenResty is
-- detected via the ngx global; every introspection step is pcall-wrapped so
-- exotic sandboxes degrade to empty strings instead of breaking startup.
local function build_manifest(service, sdk_version)
  local runtime_version = _VERSION or "Lua"
  local framework = ""
  local os_arch = ""
  pcall(function()
    if ngx == nil then return end
    framework = "openresty"
    runtime_version = runtime_version ..
        " / openresty " .. tostring((ngx.config or {}).nginx_version or "")
  end)
  pcall(function()
    os_arch = tostring((ngx.config or {}).ngx_architecture or "")
  end)
  return {
    service_name = service,
    language = "lua",
    sdk_version = sdk_version,
    runtime_version = runtime_version,
    framework = framework,
    os_arch = os_arch,
    app_version = os.getenv("DATAFLOW_APP_VERSION") or "",
    dependencies = {}, -- Lua has no introspectable dependency inventory
  }
end

-- HTTP base for manifest reporting: DATAFLOW_HTTP_URL wins (needed when
-- DATAFLOW_ENDPOINT is a bare host:port); an http(s) endpoint maps directly;
-- otherwise there is no derivable base and reporting is skipped.
local function http_base()
  local via_env = env("DATAFLOW_HTTP_URL", "")
  if via_env ~= "" then
    via_env = (via_env:match("^%s*(.-)%s*$"):gsub("/+$", ""))
    if via_env ~= "" then return via_env end
  end
  if settings.endpoint:find("^https?://") then
    return (settings.endpoint:gsub("/+$", ""))
  end
  return nil
end

-- Field order is stable and matches the server contract.
local function manifest_json(m)
  local deps = {}
  for _, d in ipairs(m.dependencies or {}) do
    if type(d) == "table" then
      deps[#deps + 1] = '{"name":' .. json_escape(d.name) ..
          ',"version":' .. json_escape(d.version) .. "}"
    else
      deps[#deps + 1] = json_escape(d)
    end
  end
  return "{" ..
      '"service_name":' .. json_escape(m.service_name) .. "," ..
      '"language":' .. json_escape(m.language) .. "," ..
      '"sdk_version":' .. json_escape(m.sdk_version) .. "," ..
      '"runtime_version":' .. json_escape(m.runtime_version) .. "," ..
      '"framework":' .. json_escape(m.framework) .. "," ..
      '"os_arch":' .. json_escape(m.os_arch) .. "," ..
      '"app_version":' .. json_escape(m.app_version) .. "," ..
      '"dependencies":[' .. table.concat(deps, ",") .. "]}"
end

-- Once per process, best-effort: same curl style as the ingest flush, any
-- failure silent, tracing never depends on the manifest reaching the server.
send_manifest = function()
  if manifest_sent then return end
  manifest_sent = true
  pcall(function()
    if settings.disabled or settings.api_key == "" then return end
    local base = http_base()
    if not base then return end
    local body = manifest_json(build_manifest(service_name(), M._VERSION))
    local cmd = string.format(
      'curl -s -m 10 -X POST -H "Content-Type: application/json" -H "X-Api-Key: %s" -d %s %s 2>/dev/null',
      settings.api_key, string.format("%q", body), base .. "/api/v1/manifest")
    local f = io.popen(cmd, "r")
    if f then f:close() end
  end)
end

-- ---------------------------------------------------------------------------
-- PII classification (field names only; mirrors the other SDKs)

local categories = {
  { "password", { "password", "passwd", "pwd" } },
  { "secret", { "token", "secret", "apikey", "api_key", "credential", "session", "jwt", "auth" } },
  { "payment", { "card", "pan", "cvv", "cvc", "iban", "expiry" } },
  { "email", { "email", "e_mail", "mail" } },
  { "phone", { "phone", "mobile", "tel", "msisdn" } },
  { "government_id", { "ssn", "passport", "tax_id", "national_id" } },
  { "birth", { "birth", "dob", "age" } },
  { "name", { "first_name", "last_name", "full_name", "surname", "customer_name", "display_name" } },
  { "address", { "street", "zip", "postal", "street_address", "billing_address", "shipping_address", "mailing_address" } },
  { "geo", { "city", "country", "region", "location", "lat", "lon", "lng" } },
  { "ip", { "ip", "ip_address", "client_ip", "remote_addr" } },
  { "device", { "device", "user_agent", "imei", "fingerprint" } },
}

local function classify_pii(fields)
  local seen, order = {}, {}
  for _, field in ipairs(fields) do
    local norm = string.lower(tostring(field)):gsub("[^%w_]", "_")
    local tokens = {}
    for tok in norm:gmatch("[^_]+") do tokens[tok] = true end
    for _, cat in ipairs(categories) do
      if not seen[cat[1]] then
        for _, kw in ipairs(cat[2]) do
          local hit = (kw:find("_", 1, true) and norm:find(kw, 1, true) ~= nil) or tokens[kw] == true
          if hit then
            seen[cat[1]] = true
            order[#order + 1] = cat[1]
            break
          end
        end
      end
    end
  end
  table.sort(order)
  return table.concat(order, ",")
end

-- ---------------------------------------------------------------------------
-- span

local Span = {}
Span.__index = Span

function Span:attr(key, value)
  self.attrs[key] = tostring(value)
  return self
end

-- value: string | number | boolean | M.raw(json)
function Span:data(key, value)
  self.payload[key] = value
  return self
end

function Span:callee(pkg)
  self.callee = tostring(pkg)
  return self
end

function Span:record_error(message)
  if self.error == "" then self.error = tostring(message) else self.error = self.error .. "; " .. tostring(message) end
  if self.status_code == 0 then self.status_code = 500 end
  return self
end

function Span:set_status(code)
  self.status_code = code
  return self
end

local buffer = {}

local function encode_value(v)
  local tv = type(v)
  if tv == "table" and v.__raw then return v.json end
  if tv == "string" then return json_escape(v) end
  if tv == "number" then
    if v % 1 == 0 and math.abs(v) < 2 ^ 53 then return string.format("%d", v) end
    return tostring(v)
  end
  if tv == "boolean" then return tostring(v) end
  return json_escape(tostring(v))
end

function Span:end_()
  if not self.sampled or self.ended then return end
  self.ended = true

  local duration_ms = math.floor((clock() - self.mono_start) * 1000)

  local keys = {}
  for k in pairs(self.payload) do keys[#keys + 1] = k end
  table.sort(keys)

  local meta_sorted = {}
  for k in pairs(self.attrs) do meta_sorted[#meta_sorted + 1] = k end
  table.sort(meta_sorted)
  local meta_parts = {}
  for _, k in ipairs(meta_sorted) do
    meta_parts[#meta_parts + 1] = json_escape(k) .. ":" .. json_escape(self.attrs[k])
  end
  if #keys > 0 then
    meta_parts[#meta_parts + 1] = '"data.fields":' .. json_escape(table.concat(keys, ","))
    local pii = classify_pii(keys)
    if pii ~= "" then meta_parts[#meta_parts + 1] = '"data.pii":' .. json_escape(pii) end
  end

  local payload_json = "null"
  if #keys > 0 then
    if not warned_plaintext then
      warned_plaintext = true
      print("dataflow: warning: lua-sdk v" .. M._VERSION .. " ships payloads as plaintext")
    end
    local parts = {}
    for _, k in ipairs(keys) do
      parts[#parts + 1] = json_escape(k) .. ":" .. encode_value(self.payload[k])
    end
    payload_json = "{" .. table.concat(parts, ",") .. "}"
  end

  buffer[#buffer + 1] = "{" ..
      '"event_id":' .. json_escape(new_id()) .. "," ..
      '"seq":' .. next_seq() .. "," ..
      '"trace_id":' .. json_escape(self.trace_id) .. "," ..
      '"span_id":' .. json_escape(self.span_id) .. "," ..
      '"parent_span_id":' .. json_escape(self.parent_span_id) .. "," ..
      '"type":' .. json_escape(self.kind) .. "," ..
      '"service_name":' .. json_escape(service_name()) .. "," ..
      '"name":' .. json_escape(self.name) .. "," ..
      '"caller_package":' .. json_escape(self.caller) .. "," ..
      '"callee_package":' .. json_escape(self.callee) .. "," ..
      '"function_name":' .. json_escape(self.name) .. "," ..
      '"timestamp":' .. self.start_ms .. "," ..
      '"duration_ms":' .. duration_ms .. "," ..
      '"status_code":' .. self.status_code .. "," ..
      '"error_message":' .. json_escape(self.error) .. "," ..
      '"payload":' .. payload_json .. "," ..
      '"metadata":{' .. table.concat(meta_parts, ",") .. "}}"

  if #buffer >= 25 then M.flush() end
end

-- ---------------------------------------------------------------------------
-- trace API

local current = nil
local pending_incoming = nil

local function make_span(name, kind, parent)
  local span = setmetatable({
    trace_id = (parent and parent.trace_id) or new_id(),
    span_id = new_id(),
    parent_span_id = (parent and parent.span_id) or "",
    -- Caller attribution mirrors the other SDKs: the enclosing span's
    -- package; roots stay empty (no self-edges in the flow graph).
    caller = (parent and parent.callee) or "",
    callee = (kind == "FUNCTION_CALL" and name:match("^([^.]+)%.") or ""),
    name = name,
    kind = kind or "FUNCTION_CALL",
    start_ms = now_ms(),
    mono_start = clock(),
    sampled = settings.sample_ratio >= 1.0 or math.random() < settings.sample_ratio,
    ended = false,
    attrs = {},
    payload = {},
    error = "",
    status_code = 0,
  }, Span)

  if kind == "HTTP_SERVER" then
    for k, v in pairs(M.agent_attrs()) do span.attrs[k] = v end
    if pending_incoming and pending_incoming ~= "" then
      span.trace_id = pending_incoming
    end
    pending_incoming = nil
  end
  return span
end

--- Runs fn(span) inside a named span; ends the span when fn returns.
function M.trace(name, fn, parent)
  parent = parent or current
  local span = make_span(name, "FUNCTION_CALL", parent)
  local prev = current
  current = span
  local ok, err = pcall(fn, span)
  current = prev
  span:end_()
  if not ok then error(err, 0) end
  return span
end

--- Runs fn(span) inside an arbitrary-type span (HTTP_CLIENT etc.).
function M.trace_kind(kind, name, fn, parent)
  parent = parent or current
  local span = make_span(name, kind, parent)
  local prev = current
  current = span
  local ok, err = pcall(fn, span)
  current = prev
  span:end_()
  if not ok then error(err, 0) end
  return span
end

--- Opens an entry-point span; call span:end_() after the response is sent.
--- Adopts the request's X-Dataflow-Trace-Id when provided.
function M.start_server_span(route, incoming_trace_id)
  pending_incoming = incoming_trace_id
  local span = make_span(route, "HTTP_SERVER", nil)
  current = span
  return span
end

--- The span active for this call site (explicit context passing).
function M.current_span() return current end

function M.agent_attrs()
  local sep = package.config:sub(1, 1)
  local os_name = (sep == "\\") and "windows" or "linux"
  return {
    ["agent.os"] = os_name .. "/lua",
    ["agent.runtime"] = _VERSION,
    ["agent.sdk"] = "lua-sdk/" .. M._VERSION,
    ["agent.started"] = tostring(now_ms()),
  }
end

--- Wraps a pre-encoded JSON payload value.
function M.raw(json) return { __raw = true, json = json } end

-- ---------------------------------------------------------------------------
-- transport spans (outgoing HTTP calls + DB queries; no new dependencies)

-- Pure: splits a URL into host (authority, userinfo stripped) and path for
-- span naming and callee attribution. Scheme-less authorities ("host:port")
-- and relative paths ("/health") degrade gracefully; missing parts -> "".
local function parse_url(url)
  local target = tostring(url or ""):match("^%s*(.-)%s*$")
  local host, path = "", ""
  local scheme, tail = target:match("^([a-zA-Z][%w+.-]*)://(.*)$")
  if scheme then
    local authority = tail:match("^([^/?#]*)")
    host = authority:match("^[^@]*@(.*)$") or authority
    path = tail:match("^[^/?#]*(/[^?#]*)") or ""
  else
    local head = target:match("^([^/?#]*)")
    if head:find("[.:]") then
      host = head
      path = target:match("^[^/?#]*(/[^?#]*)") or ""
    else
      path = target:match("^(/[^?#]*)") or ""
    end
  end
  if path == "/" then path = "" end
  return host, path
end

-- Pure: "GET https://u:p@host:8080/v1/x?q=1" -> "GET host:8080/v1/x".
function M.http_span_name(method, url)
  local host, path = parse_url(url)
  method = string.upper(tostring(method or ""))
  if host == "" and path == "" then return method end
  if host == "" then return method .. " " .. path end
  return method .. " " .. host .. path
end

local SQL_VERBS = {
  SELECT = true, INSERT = true, UPDATE = true, DELETE = true,
  CREATE = true, DROP = true, ALTER = true, TRUNCATE = true,
  REPLACE = true, MERGE = true, UPSERT = true, CALL = true,
  EXEC = true, EXECUTE = true, BEGIN = true, COMMIT = true,
  ROLLBACK = true, GRANT = true, REVOKE = true, VACUUM = true,
  ANALYZE = true, EXPLAIN = true, SHOW = true, PRAGMA = true,
}

local SQL_TABLE_KEYWORDS = { FROM = true, INTO = true, UPDATE = true, TABLE = true, JOIN = true }

-- Pure: best-effort comment stripping so summaries don't trip on "-- ..." or
-- /* ... */ prefixes.
local function strip_sql_comments(sql)
  sql = sql:gsub("%-%-.-\n", " ")
  sql = sql:gsub("%-%-.*$", " ")
  sql = sql:gsub("/%*.-%*/", " ")
  return sql
end

-- Pure: "<VERB> <table>" from SQL — uppercase first keyword; table from the
-- first FROM|INTO|UPDATE|TABLE|JOIN (skipping IF [NOT] EXISTS and schema
-- qualifiers, public.items -> items); no known verb -> first word uppercase.
function M.stmt_summary(sql)
  sql = strip_sql_comments(tostring(sql or "")):gsub('["`%[%]]', "")
  local words = {}
  for w in sql:gmatch("[%w_.]+") do words[#words + 1] = w end
  if #words == 0 then return "" end
  local verb = string.upper(words[1])
  if not SQL_VERBS[verb] then return verb end
  local table_name = ""
  for i = 1, #words do
    if SQL_TABLE_KEYWORDS[string.upper(words[i])] then
      local j = i + 1
      if string.upper(words[j] or "") == "IF" then
        j = j + 1
        if string.upper(words[j] or "") == "NOT" then j = j + 1 end
        if string.upper(words[j] or "") == "EXISTS" then j = j + 1 end
      end
      local raw = (words[j] or ""):gsub("^%.+", ""):gsub("%.+$", "")
      table_name = raw:match("%.([%w_]+)$") or raw
      break
    end
  end
  if table_name == "" then return verb end
  return verb .. " " .. table_name
end

-- Pure: single-space the statement and clip to 200 chars for db.statement.
-- Bind values must never be embedded in the statement passed here.
function M.clip_statement(sql)
  local s = tostring(sql or ""):gsub("%s+", " ")
  s = s:match("^%s*(.-)%s*$") or ""
  if #s > 200 then s = s:sub(1, 200) end
  return s
end

-- Transport span handle: wraps the standard span behind an explicit :finish()
-- (Lua GC timing is unreliable; __gc is only a last-resort net where the
-- runtime honors table __gc, e.g. LuaJIT, never a substitute). Finalization
-- is pcall-wrapped and skipped when tracing is disabled, but the trace id
-- stays available either way so callers keep propagating context.
local TransportSpan = {}
TransportSpan.__index = TransportSpan

local function transport_finish(handle)
  if handle.ended then return end
  handle.ended = true
  local inner = handle.inner
  if not inner then return end
  pcall(function() inner:end_() end)
end

TransportSpan.__gc = transport_finish

function TransportSpan:set_status(code)
  if self.inner then self.inner:set_status(code) end
  return self
end

function TransportSpan:record_error(message)
  if self.inner then self.inner:record_error(message) end
  return self
end

function TransportSpan:trace_id() return self.trace_id end

function TransportSpan:finish()
  transport_finish(self)
  return self
end

local function make_transport_span(kind, name, callee, attrs)
  local handle = setmetatable({
    trace_id = new_id(),
    inner = nil,
    ended = false,
  }, TransportSpan)
  -- Disabled/unconfigured: no recording; the handle stays cheap and the
  -- generated trace id still propagates to downstream services.
  if not enabled() then return handle end
  local inner = make_span(name, kind, current)
  inner.callee = callee
  for k, v in pairs(attrs) do inner.attrs[k] = v end
  handle.trace_id = inner.trace_id
  handle.inner = inner
  return handle
end

--- Outgoing HTTP call span (type HTTP_CLIENT): name "METHOD host/path",
--- callee_package = host, metadata http.method / http.url. MUST be finished
--- explicitly: span:finish() records the duration. Record outcomes with
--- :set_status(http_status) / :record_error(msg), and propagate context by
--- sending header "X-Dataflow-Trace-Id: <span:trace_id()>".
function M.http_span(method, url)
  url = tostring(url or "")
  return make_transport_span("HTTP_CLIENT", M.http_span_name(method, url),
    parse_url(url), {
      ["http.method"] = string.upper(tostring(method or "")),
      ["http.url"] = url,
    })
end

--- Database query span (type DB_QUERY) for any backend: name "<VERB> <table>"
--- summarized from the SQL, callee_package = system, metadata db.system /
--- db.statement (single-spaced, clipped to 200 chars). Statements only —
--- never pass bound values. Finished exactly like http_span().
function M.db_span(system, statement)
  statement = tostring(statement or "")
  return make_transport_span("DB_QUERY", M.stmt_summary(statement),
    tostring(system or ""), {
      ["db.system"] = tostring(system or ""),
      ["db.statement"] = M.clip_statement(statement),
    })
end

-- ---------------------------------------------------------------------------
-- delivery (curl on PATH; fire-and-forget in v0.1)

function M.flush()
  if #buffer == 0 or not enabled() then return end
  local body = '{"events":[' .. table.concat(buffer, ",") .. "]}"
  local url = settings.endpoint:gsub("/$", "") .. "/api/v1/ingest"
  local cmd = string.format(
    'curl -s -m 10 -X POST -H "Content-Type: application/json" -H "X-Api-Key: %s" -d %s %s 2>/dev/null',
    settings.api_key, string.format("%q", body), url)
  local f = io.popen(cmd, "r")
  if f then f:close() end
  buffer = {}
end

M.configure()
return M
