-- scan_cli.lua — CLI for the Dataflow static route scanner.
--
--   lua scan_cli.lua --dir . [--service name] [--url base] [--api-key key] [--print]
--
-- Extracts declared HTTP endpoints from the Lua sources under --dir
-- (dataflow.scan) and posts them to POST <base>/api/v1/catalog
-- (dataflow.scan_post). Pure Lua; posting mirrors the SDK's curl transport.
--
-- Exit codes: 0 on success and on a *skip* (no base URL / API key — reported
-- on stderr, nothing posted), 1 on a real failure (bad directory, post
-- error), 2 on a usage error.

local script_dir = (arg and arg[0] or ""):match("^(.*)[/\\][^/\\]+$") or "."
package.path = script_dir .. "/?.lua;" .. package.path

local dataflow = require("dataflow")

local USAGE = [[
usage: lua scan_cli.lua [--dir DIR] [--service NAME] [--url BASE] [--api-key KEY] [--print]

  --dir DIR       directory to scan for *.lua (default ".")
  --service NAME  service name (default DATAFLOW_SERVICE_NAME or DIR basename)
  --url BASE      Dataflow HTTP base (default DATAFLOW_HTTP_URL, then an
                  http(s) DATAFLOW_ENDPOINT; bare host:port cannot be used)
  --api-key KEY   API key (default DATAFLOW_API_KEY)
  --print         print the catalog JSON body and skip the post
]]

local function resolve_base(explicit)
  local base = explicit
  if base == nil or base == "" then base = os.getenv("DATAFLOW_HTTP_URL") or "" end
  if base == nil or base == "" then
    local ep = os.getenv("DATAFLOW_ENDPOINT") or ""
    if ep:find("^https?://") then base = ep end
  end
  if base == nil or base == "" then return nil end
  base = (tostring(base):match("^%s*(.-)%s*$"))
  base = (base:gsub("/+$", ""))
  if base == "" then return nil end
  return base
end

local function main()
  local dir, service, url, api_key = ".", nil, nil, nil
  local print_only = false
  local args = arg or {}
  local i = 1
  while i <= #args do
    local a = args[i]
    if a == "--dir" then
      i = i + 1; dir = args[i] or "."
    elseif a == "--service" then
      i = i + 1; service = args[i]
    elseif a == "--url" then
      i = i + 1; url = args[i]
    elseif a == "--api-key" then
      i = i + 1; api_key = args[i]
    elseif a == "--print" then
      print_only = true
    elseif a == "--help" or a == "-h" then
      print(USAGE)
      return 0
    else
      io.stderr:write("dataflow-scan: unknown argument: " .. tostring(a) .. "\n" .. USAGE)
      return 2
    end
    i = i + 1
  end

  local catalog, err = dataflow.scan(dir, { service_name = service })
  if not catalog then
    io.stderr:write("dataflow-scan: " .. tostring(err) .. "\n")
    return 1
  end

  if print_only then
    print(dataflow.scan_json(catalog))
    return 0
  end

  local base = resolve_base(url)
  if not base then
    io.stderr:write("dataflow-scan: no HTTP base URL (--url, DATAFLOW_HTTP_URL, or an " ..
      "http(s) DATAFLOW_ENDPOINT); skipping catalog post\n")
    return 0
  end

  if api_key == nil or api_key == "" then api_key = os.getenv("DATAFLOW_API_KEY") or "" end
  if api_key == "" then
    io.stderr:write("dataflow-scan: no API key (--api-key or DATAFLOW_API_KEY); " ..
      "skipping catalog post\n")
    return 0
  end

  local ok, perr = dataflow.scan_post(catalog, { url = base, api_key = api_key })
  if not ok then
    io.stderr:write("dataflow-scan: " .. tostring(perr) .. "\n")
    return 1
  end
  print(string.format("dataflow-scan: %s: %d route(s) -> %s/api/v1/catalog",
    catalog.service_name, #catalog.routes, base))
  return 0
end

os.exit(main())
