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
-- Static route scanning (see also scan_cli.lua): dataflow.scan(dir, opts)
-- extracts declared HTTP endpoints from Lua sources (lua-resty-route calls,
-- dispatch tables, ngx.var.uri guards) and dataflow.scan_post(catalog)
-- ships them to POST /api/v1/catalog.
--
-- Crash capture: dataflow.capture(fn, ...) / dataflow.capture_or_raise(fn, ...)
-- run a call under xpcall and record status 500, a clipped error_message and
-- a clipped debug.traceback (metadata "error.stack") on the current span
-- (a synthetic "exception" span when none is open) before the error
-- propagates; capture() hands the error back (false, err), capture_or_raise()
-- re-raises at the call site.
--
-- Log capture: dataflow.log(level, message, fields) and the shorthand
-- debug/info/warn/error record application logs with the current span's
-- trace/span ids into a bounded buffer; dataflow.flush_logs() ships them
-- to POST /api/v1/logs (<= 1000/batch) and the 50th buffered line fires
-- it automatically (plain Lua has no timers). Best-effort as everywhere
-- else: never raises, drops on overflow, no-ops when disabled,
-- unconfigured, or without a derivable HTTP base.
--
-- Env: DATAFLOW_ENDPOINT (http://host:port), DATAFLOW_API_KEY,
-- DATAFLOW_SERVICE_NAME, DATAFLOW_SAMPLE_RATIO, DATAFLOW_BUFFER_SIZE,
-- DATAFLOW_HTTP_URL (HTTP base for the startup manifest, the route
-- catalog post and log shipping), DATAFLOW_APP_VERSION.
--
-- Payload VALUES ship as plaintext JSON in v0.1 (a warning is printed
-- once); field NAMES also travel in `data.fields` metadata for lineage, so
-- adding client-side encryption later stays transparent to the dashboard.

local M = { _VERSION = "0.6.0" }

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
-- static route scanning (dataflow.scan; pattern-based, pure Lua — no lpeg,
-- no luasocket). Recognized OpenResty idioms, all best-effort:
--   r:get("/path", handler)          lua-resty-route (post/put/delete/patch too)
--   route("/base", function(r) ... end)  ONE level of prefix, do-end tracked
--   get = { ["/path"] = handler }    dispatch tables (best effort)
--   if ngx.var.uri == "/path" then   heuristic -> GET with an empty handler
-- A route needs a string-literal path starting with "/"; commented-out code
-- is ignored; path params keep their written form (":id" or "{id}").

local SCAN_VERBS = { get = true, post = true, put = true, delete = true, patch = true }
local SCAN_SKIP_DIRS = { [".git"] = true, ["deps"] = true, ["t"] = true }
local SCAN_MAX_ROUTES = 1000

local SCAN_ROUTE_CALL_PAT = '([%a_][%w_]*)%s*:%s*(%a+)%s*%(%s*"(/[^"]*)"%s*,%s*([^)]*)%)'
local SCAN_BRACKET_PAT = '%[%s*["\'](/[^"\']*)["\']%s*%]%s*=%s*([^,}]+)'
local SCAN_INLINE_DISPATCH_PAT = '([%a_][%w_]*)%s*=%s*{%s*%[%s*["\'](/[^"\']*)["\']%s*%]%s*=%s*([^,}]+)'
local SCAN_GUARD_PAT = 'ngx%.var%.uri%s*==%s*["\'](/[^"\']*)["\']'
local SCAN_ROUTE_OPEN_PAT = 'route%s*%(%s*["\'](/[^"\']*)["\']%s*,%s*function%f[%W]'
local SCAN_DISPATCH_OPEN_PAT = "^%s*([%a_][%w_]*)%s*=%s*{%s*$"

local function scan_trim(s)
  return (tostring(s or ""):match("^%s*(.-)%s*$"))
end

-- handler call argument -> identifier string ("" for anonymous functions,
-- tables and anything else we cannot name conservatively).
local function scan_handler_name(arg)
  arg = scan_trim(arg)
  if arg == "" or arg:match("^function%f[%W]") or arg == "function" then return "" end
  local quoted = arg:match('^"([^"]*)"') or arg:match("^'([^']*)'")
  if quoted then return quoted end
  return arg:match("^[%w_.]+") or ""
end

local function scan_join_prefix(prefix, path)
  if not prefix or prefix == "" or prefix == "/" then return path end
  if path == "/" then return prefix end
  return prefix .. path
end

-- Pure: best-effort comment stripping. Block comments are removed with a
-- two-state pass (an unterminated /*-style "--[[" swallows lines until "]]");
-- line comments respect string literals so paths containing "--" survive.
local function scan_strip_block_comments(line, state)
  while true do
    if state.in_block then
      local close = line:find("]]", 1, true)
      if not close then return "", true end
      line = line:sub(close + 2)
      state.in_block = false
    end
    local open = line:find("--[[", 1, true)
    if not open then return line end
    local close = line:find("]]", open + 4, true)
    if not close then
      state.in_block = true
      return line:sub(1, open - 1)
    end
    line = line:sub(1, open - 1) .. " " .. line:sub(close + 2)
  end
end

local function scan_strip_line_comment(line)
  local in_str, i, n = nil, 1, #line
  while i <= n do
    local c = line:sub(i, i)
    if in_str then
      if c == "\\" then i = i + 1
      elseif c == in_str then in_str = nil end
    elseif c == '"' or c == "'" then
      in_str = c
    elseif c == "-" and line:sub(i + 1, i + 1) == "-" then
      return line:sub(1, i - 1)
    end
    i = i + 1
  end
  return line
end

local function scan_count_keyword(line, word)
  local n = 0
  for _ in line:gmatch("%f[%w_]" .. word .. "%f[%W]") do n = n + 1 end
  return n
end

local function scan_brace_delta(line)
  local _, open = line:gsub("{", "")
  local _, close = line:gsub("}", "")
  return open - close
end

-- Pure: extract routes from one source string. Returns a list of
-- { method, path, handler, source_file } in source order; duplicates
-- collapse (first handler wins, unless the first sighting had none).
local function scan_extract_source(filename, source)
  local routes, seen = {}, {}
  local prefix, nest = nil, 0      -- route("/base", function(r) ... end)
  local dispatch, depth = nil, 0   -- verb = { ["/path"] = handler } tables
  local state = { in_block = false }

  local function emit(verb, path, handler)
    verb = string.upper(tostring(verb))
    path = scan_trim(path)
    if path == "" or path:sub(1, 1) ~= "/" then return end
    local key = verb .. " " .. path
    local at = seen[key]
    if at then
      if routes[at].handler == "" and (handler or "") ~= "" then
        routes[at].handler = handler
      end
      return
    end
    seen[key] = #routes + 1
    routes[#routes + 1] = { method = verb, path = path,
      handler = handler or "", source_file = filename }
  end

  for line in (source .. "\n"):gmatch("(.-)\n") do
    line = scan_strip_block_comments(line, state)
    line = scan_strip_line_comment(line)

    -- bracket entries of an open dispatch table: ["/path"] = handler
    if dispatch then
      for path, h in line:gmatch(SCAN_BRACKET_PAT) do
        emit(dispatch.verb, path, scan_handler_name(h))
      end
    end

    -- single-line dispatch tables: { get = { ["/path"] = handler } }
    for verb, path, h in line:gmatch(SCAN_INLINE_DISPATCH_PAT) do
      if SCAN_VERBS[string.lower(verb)] then
        emit(verb, path, scan_handler_name(h))
      end
    end

    -- receiver calls: r:get("/path", handler)
    for _, verb, path, h in line:gmatch(SCAN_ROUTE_CALL_PAT) do
      if SCAN_VERBS[string.lower(verb)] then
        emit(verb, scan_join_prefix(prefix, path), scan_handler_name(h))
      end
    end

    -- ngx.var.uri == "/path" guards (heuristic: GET, no handler)
    for path in line:gmatch(SCAN_GUARD_PAT) do
      emit("GET", path, "")
    end

    -- dispatch table bookkeeping (brace depth, best effort)
    depth = depth + scan_brace_delta(line)
    if dispatch and depth < dispatch.depth then dispatch = nil end
    if not dispatch and depth > 0 then
      local verb = line:match(SCAN_DISPATCH_OPEN_PAT)
      if verb and SCAN_VERBS[string.lower(verb)] then
        dispatch = { verb = string.lower(verb), depth = depth }
      end
    end

    -- route() prefix bookkeeping: one level, do-end tracked
    local rs, re, rbase = line:find(SCAN_ROUTE_OPEN_PAT)
    if rs then
      if not prefix then
        prefix = rbase
        local rest = line:sub(re + 1)
        nest = 1 + scan_count_keyword(rest, "do") + scan_count_keyword(rest, "function")
        nest = nest - scan_count_keyword(rest, "end")
      else
        nest = nest + scan_count_keyword(line, "do")
        nest = nest + scan_count_keyword(line, "function")
        nest = nest - scan_count_keyword(line, "end")
      end
      if nest <= 0 then prefix = nil end
    elseif prefix then
      nest = nest + scan_count_keyword(line, "do")
      nest = nest + scan_count_keyword(line, "function")
      nest = nest - scan_count_keyword(line, "end")
      if nest <= 0 then prefix = nil end
    end
  end

  return routes
end

--- Pure: extract declared routes from one Lua source string. Never raises.
--- Returns a list of { method, path, handler, source_file } tables.
function M.scan_extract(filename, source)
  local ok, routes = pcall(scan_extract_source, tostring(filename or ""), tostring(source or ""))
  if not ok then return {} end
  return routes
end

-- Pure: stable-order JSON body for POST /api/v1/catalog (<= 1000 routes,
-- mirroring the server-side cap).
function M.scan_json(catalog)
  local ok, body = pcall(function()
    local parts = {}
    for i, r in ipairs((type(catalog) == "table" and catalog or {}).routes or {}) do
      if i > SCAN_MAX_ROUTES then break end
      parts[#parts + 1] = "{" ..
          '"method":' .. json_escape(r.method) .. "," ..
          '"path":' .. json_escape(r.path) .. "," ..
          '"handler":' .. json_escape(r.handler) .. "," ..
          '"source_file":' .. json_escape(r.source_file) .. "}"
    end
    local service = type(catalog) == "table" and catalog.service_name or ""
    return "{" ..
        '"service_name":' .. json_escape(service) .. "," ..
        '"routes":[' .. table.concat(parts, ",") .. "]}"
  end)
  if not ok then return '{"service_name":"","routes":[]}' end
  return body
end

local function scan_dir_exists(dir)
  local f = io.open(dir, "r")
  if not f then return false end
  f:close()
  return true
end

local function scan_sh_quote(s)
  return "'" .. tostring(s or ""):gsub("'", "'\\''") .. "'"
end

-- File listing via io.popen (same external-tool approach as curl): `find`
-- on POSIX, `dir /s /b` on Windows; nil when the pipe cannot be opened.
local function scan_list_files(dir)
  local sep = package.config:sub(1, 1)
  local cmd
  if sep == "\\" then
    cmd = string.format('dir /s /b "%s\\*.lua" 2>nul', (tostring(dir):gsub('"', "")))
  else
    cmd = string.format("find %s -type f -name '*.lua' 2>/dev/null", scan_sh_quote(dir))
  end
  local f = io.popen(cmd, "r")
  if not f then return nil end
  local out = f:read("*a") or ""
  f:close()
  local files = {}
  for path in out:gmatch("[^\r\n]+") do
    path = scan_trim(path)
    if path ~= "" then files[#files + 1] = path end
  end
  return files
end

local function scan_rel_path(dir, path)
  local norm = (path:gsub("\\", "/"))
  local base = (tostring(dir):gsub("\\", "/"))
  base = base:gsub("/+$", "")
  if base ~= "" and norm:sub(1, #base + 1) == base .. "/" then
    norm = norm:sub(#base + 2)
  end
  return (norm:gsub("^%./", ""))
end

local function scan_skipped(rel, extra)
  for comp in rel:gmatch("[^/]+") do
    if SCAN_SKIP_DIRS[comp] then return true end
    if extra and extra[comp] then return true end
  end
  return false
end

local function scan_dir_basename(dir)
  local base = (tostring(dir):gsub("\\", "/")):gsub("/+$", "")
  return base:match("[^/]+$") or base
end

local function scan_dir(dir, opts)
  opts = opts or {}
  dir = tostring(dir or "")
  if dir == "" then return nil, "scan: no directory given" end
  if not scan_dir_exists(dir) then return nil, "scan: cannot open directory: " .. dir end
  local files = scan_list_files(dir)
  if not files then return nil, "scan: could not list directory: " .. dir end

  local exclude = {}
  for _, name in ipairs(opts.exclude or {}) do exclude[tostring(name)] = true end

  local routes = {}
  for _, path in ipairs(files) do
    local rel = scan_rel_path(dir, path)
    if not scan_skipped(rel, exclude) then
      local f = io.open(path, "r")
      if f then
        local source = f:read("*a") or ""
        f:close()
        for _, r in ipairs(M.scan_extract(rel, source)) do
          if #routes >= SCAN_MAX_ROUTES then break end
          routes[#routes + 1] = r
        end
      end
    end
    if #routes >= SCAN_MAX_ROUTES then break end
  end

  table.sort(routes, function(a, b)
    if a.source_file ~= b.source_file then return a.source_file < b.source_file end
    if a.method ~= b.method then return a.method < b.method end
    return a.path < b.path
  end)

  local service = opts.service_name
  if service == nil or service == "" then service = env("DATAFLOW_SERVICE_NAME", "") end
  if service == "" then service = scan_dir_basename(dir) end
  return { service_name = service, routes = routes }
end

--- Pure-ish: walk dir for *.lua (skipping .git/, deps/, t/ and opts.exclude
--- entries), extract routes, sort by (source_file, method, path). Returns
--- { service_name = ..., routes = {...} } or nil + reason; never raises.
--- Service name: opts.service_name > DATAFLOW_SERVICE_NAME > dir basename.
--- Directory listing uses io.popen (find / dir), like the curl transport.
function M.scan(dir, opts)
  local ok, res, err = pcall(scan_dir, dir, opts)
  if not ok then return nil, tostring(res) end
  if res == nil then return nil, err end
  return res
end

-- Best-effort catalog post, mirroring the manifest: same curl mechanics,
-- response ignored. Base URL: opts.url > DATAFLOW_HTTP_URL > URL-form
-- DATAFLOW_ENDPOINT (bare host:port cannot be derived -> skip). API key:
-- opts.api_key > configured key > DATAFLOW_API_KEY.
local function scan_post_catalog(catalog, opts)
  opts = opts or {}
  if type(catalog) ~= "table" or scan_trim(tostring(catalog.service_name)) == "" then
    return nil, "scan_post: catalog table with service_name required"
  end
  local base = opts.url
  if base == nil or base == "" then base = http_base() end
  if not base then
    return nil, "scan_post: no HTTP base URL (set DATAFLOW_HTTP_URL or an http(s) DATAFLOW_ENDPOINT)"
  end
  local key = opts.api_key
  if key == nil or key == "" then key = settings.api_key end
  if key == nil or key == "" then key = env("DATAFLOW_API_KEY", "") end
  if key == "" then
    return nil, "scan_post: no API key (set DATAFLOW_API_KEY or pass opts.api_key)"
  end
  local body = M.scan_json(catalog)
  local cmd = string.format(
    'curl -s -m 10 -X POST -H "Content-Type: application/json" -H "X-Api-Key: %s" -d %s %s 2>/dev/null',
    key, string.format("%q", body), base .. "/api/v1/catalog")
  local f = io.popen(cmd, "r")
  if f then f:close() end
  return true, nil
end

--- Posts a scan() catalog to POST /api/v1/catalog (best-effort, like the
--- startup manifest). Returns true, or nil + reason; never raises.
function M.scan_post(catalog, opts)
  local ok, res, err = pcall(scan_post_catalog, catalog, opts)
  if not ok then return nil, tostring(res) end
  if res == nil then return nil, err end
  return true
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
  return M.clip_text(s, 200)
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
-- crash capture (dataflow.capture / dataflow.capture_or_raise)
--
-- Wire convention: on the CURRENT span — or a synthetic "exception" span
-- when none is open — an error records status 500, error_message = the error
-- clipped to ERROR_MESSAGE_MAX bytes and metadata "error.stack" =
-- debug.traceback clipped to ERROR_STACK_MAX bytes from the top.
-- capture() then RETURNS false, err (Lua idiom: the caller decides what to
-- do — nothing is re-raised); capture_or_raise() re-raises at the call site
-- instead.

local ERROR_MESSAGE_MAX = 500
local ERROR_STACK_MAX = 8192

-- Pure: clip s to its first n bytes (nil-safe, non-strings coerced). Generic
-- byte cap for error text; clip_statement (transport spans above) reuses it
-- for the db.statement clip.
function M.clip_text(s, n)
  s = tostring(s or "")
  n = tonumber(n) or 0
  if n < 0 then n = 0 end
  if #s > n then return s:sub(1, n) end
  return s
end

-- pack/unpack keep the Lua 5.1 shape (its xpcall takes no extra arguments);
-- table.unpack covers 5.2+.
local function pack(...)
  return { n = select("#", ...), ... }
end

local unpack = table.unpack or unpack

-- Best-effort recording, pcall-wrapped: a broken span or a failing
-- traceback must never mask the original error. Nothing is recorded when
-- the SDK is unconfigured or disabled (plain xpcall passthrough).
local function record_crash(err, stack)
  if not enabled() then return end
  pcall(function()
    local span = current
    local synthetic = false
    if not span then
      span = make_span("exception", "FUNCTION_CALL", nil)
      synthetic = true
    end
    span:set_status(500)
    span:record_error(M.clip_text(tostring(err), ERROR_MESSAGE_MAX))
    span:attr("error.stack", M.clip_text(stack or "", ERROR_STACK_MAX))
    if synthetic then span:end_() end
  end)
end

--- Runs fn(...) under xpcall with a debug.traceback message handler. On
--- error: records status 500 + error_message + "error.stack" metadata on
--- the current span (a synthetic "exception" span, ended immediately, when
--- none is open), then RETURNS false, err — the original error object is
--- handed back and nothing is re-raised; the caller decides. On success
--- returns true + all of fn's results. Unconfigured/disabled SDK degrades
--- to a plain xpcall passthrough; the recording itself never masks the
--- original error.
function M.capture(fn, ...)
  local args = pack(...)
  local stack = {}
  local out = pack(xpcall(function() return fn(unpack(args, 1, args.n)) end,
    function(e)
      -- pcall-wrapped including the debug table lookup: stripped sandboxes
      -- degrade to an empty stack instead of masking the original error
      local ok_tb, tb = pcall(function() return debug.traceback(tostring(e), 2) end)
      stack[1] = ok_tb and tb or ""
      return e
    end))
  if not out[1] then
    record_crash(out[2], stack[1])
    return false, out[2]
  end
  return unpack(out, 1, out.n)
end

--- Same recording as capture(), then error(err, 2): the crash propagates,
--- attributed to the capture_or_raise call site. For handlers that must not
--- swallow errors (e.g. OpenResty access/content phases that rely on the
--- error page). Success forwards all of fn's results.
function M.capture_or_raise(fn, ...)
  local out = pack(M.capture(fn, ...))
  if not out[1] then error(out[2], 2) end
  return unpack(out, 2, out.n)
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

-- Test hook (tests/test_dataflow.lua): points the event buffer at `b` so
-- pure tests can assert recorded events without a server or curl. Nil or a
-- non-table resets to a fresh buffer. Not part of the public SDK surface.
function M._test_buffer(b)
  buffer = type(b) == "table" and b or {}
  return buffer
end

-- ---------------------------------------------------------------------------
-- application logs (log shipping with trace correlation)
--
-- dataflow.log(level, message, fields) plus debug/info/warn/error record
-- application logs with the CURRENT span's trace/span ids (empty when none
-- is open) into a bounded buffer. Plain Lua has no timers this SDK could
-- rely on, so shipping is threshold-triggered: the 50th buffered line
-- fires a synchronous, best-effort POST of {"logs":[...]} to /api/v1/logs
-- via the same fire-and-forget curl invocation as the ingest flush
-- (response ignored, <= 1000 entries per batch); dataflow.flush_logs()
-- forces the same post. Never raises, drops on overflow, and no-ops when
-- the SDK is disabled/unconfigured or when no HTTP base is derivable
-- (bare host:port endpoint).

local LOG_LEVELS = { debug = true, info = true, warn = true, error = true }
local LOG_LEVEL_ALIASES = { warning = "warn", err = "error", critical = "error", fatal = "error" }
local LOG_FIELDS_MAX = 50
local LOG_BUFFER_MAX = 1024
local LOG_FLUSH_THRESHOLD = 50
local LOG_MAX_BATCH = 1000

local log_buffer = {}
local log_dropped = 0

-- Level -> one of debug|info|warn|error (case/whitespace folded,
-- warning -> warn, err/critical/fatal -> error, anything unknown -> info).
local function normalize_log_level(level)
  local l = string.lower(scan_trim(level))
  if LOG_LEVELS[l] then return l end
  if LOG_LEVEL_ALIASES[l] then return LOG_LEVEL_ALIASES[l] end
  return "info"
end

-- Fields -> JSON object with every value stringified (tostring), capped at
-- LOG_FIELDS_MAX entries and sorted for a stable body (like span payloads).
local function encode_log_fields(fields)
  if type(fields) ~= "table" then return "{}" end
  local keys = {}
  for k in pairs(fields) do
    if #keys < LOG_FIELDS_MAX then keys[#keys + 1] = k end
  end
  table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
  local parts = {}
  for i, k in ipairs(keys) do
    parts[i] = json_escape(tostring(k)) .. ":" .. json_escape(tostring(fields[k]))
  end
  return "{" .. table.concat(parts, ",") .. "}"
end

-- Pure: stable-order JSON body for POST /api/v1/logs (<= LOG_MAX_BATCH
-- entries, mirroring the server-side cap). Entries are the pre-encoded
-- objects the buffer holds; anything else degrades to a JSON string.
function M.logs_json(entries)
  local ok, body = pcall(function()
    local parts = {}
    for i, e in ipairs(type(entries) == "table" and entries or {}) do
      if i > LOG_MAX_BATCH then break end
      parts[#parts + 1] = type(e) == "string" and e or json_escape(e)
    end
    return '{"logs":[' .. table.concat(parts, ",") .. "]}"
  end)
  if not ok then return '{"logs":[]}' end
  return body
end

-- Best-effort record: gated on enabled() plus a derivable HTTP base, never
-- raises (M.log pcall-wraps this), overflow drops the oldest line.
local function record_log(level, message, fields)
  if not enabled() or not http_base() then return end
  local span = current
  local entry = "{" ..
      '"timestamp":' .. now_ms() .. "," ..
      '"level":' .. json_escape(level) .. "," ..
      '"message":' .. json_escape(message) .. "," ..
      '"trace_id":' .. json_escape(span and span.trace_id or "") .. "," ..
      '"span_id":' .. json_escape(span and span.span_id or "") .. "," ..
      '"service_name":' .. json_escape(service_name()) .. "," ..
      '"fields":' .. encode_log_fields(fields) .. "}"
  if #log_buffer >= LOG_BUFFER_MAX then
    table.remove(log_buffer, 1)
    log_dropped = log_dropped + 1
  end
  log_buffer[#log_buffer + 1] = entry
  if #log_buffer >= LOG_FLUSH_THRESHOLD then M.flush_logs() end
end

--- Records an application log; level normalizes to debug|info|warn|error.
--- Correlated with the current span (trace/span ids, empty when none is
--- open). Best-effort: never raises.
function M.log(level, message, fields)
  pcall(function() record_log(normalize_log_level(level), message, fields) end)
end

--- Shorthand for M.log with a fixed level.
function M.debug(message, fields) M.log("debug", message, fields) end
function M.info(message, fields) M.log("info", message, fields) end
function M.warn(message, fields) M.log("warn", message, fields) end
function M.error(message, fields) M.log("error", message, fields) end

-- One fire-and-forget POST of the oldest <= LOG_MAX_BATCH entries; entries
-- leave the buffer before the request, so a missing curl loses at most the
-- batch (same trade-off as the ingest flush).
local function flush_logs_now()
  local n = #log_buffer
  if n == 0 or not enabled() then return end
  local base = http_base()
  if not base then return end
  local take = math.min(n, LOG_MAX_BATCH)
  local batch = {}
  for i = 1, take do batch[i] = log_buffer[i] end
  for i = take + 1, n do log_buffer[i - take] = log_buffer[i] end
  for i = n - take + 1, n do log_buffer[i] = nil end
  local body = M.logs_json(batch)
  local cmd = string.format(
    'curl -s -m 10 -X POST -H "Content-Type: application/json" -H "X-Api-Key: %s" -d %s %s 2>/dev/null',
    settings.api_key, string.format("%q", body), base .. "/api/v1/logs")
  local f = io.popen(cmd, "r")
  if f then f:close() end
end

--- Ships buffered logs to POST /api/v1/logs (<= 1000 per batch, header
--- X-Api-Key). Best-effort: response ignored, never raises. Fired
--- automatically when the buffer reaches LOG_FLUSH_THRESHOLD (50) lines.
function M.flush_logs()
  pcall(flush_logs_now)
end

--- Buffer counts: entries waiting to ship and entries lost to the 1024
--- cap (drop-oldest). Observability + test seam.
function M.log_stats()
  return { buffered = #log_buffer, dropped = log_dropped }
end

-- Test hook (tests/test_dataflow.lua): points the log buffer at `b` and
-- resets the drop counter so overflow tests start from a known state. Nil
-- or a non-table resets to a fresh buffer. Not part of the public surface.
function M._test_log_buffer(b)
  log_buffer = type(b) == "table" and b or {}
  log_dropped = 0
  return log_buffer
end

M.configure()
return M
