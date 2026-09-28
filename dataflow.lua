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
--   dataflow.flush()
--
-- Env: DATAFLOW_ENDPOINT (http://host:port), DATAFLOW_API_KEY,
-- DATAFLOW_SERVICE_NAME, DATAFLOW_SAMPLE_RATIO, DATAFLOW_BUFFER_SIZE.
--
-- Payload VALUES ship as plaintext JSON in v0.1 (a warning is printed
-- once); field NAMES also travel in `data.fields` metadata for lineage, so
-- adding client-side encryption later stays transparent to the dashboard.

local M = { _VERSION = "0.1.0" }

-- ---------------------------------------------------------------------------
-- settings

local settings = { endpoint = "", api_key = "", service_name = "", sample_ratio = 1.0, buffer_size = 10000, disabled = false }
local warned_plaintext = false
local seq = 0

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
      print("dataflow: warning: lua-sdk v0.1 ships payloads as plaintext")
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
