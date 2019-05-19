-- -*- Lua -*-
-- Copyright (c) 2006 - 2019 omobus-ark authors, see the included COPYRIGHT file.

local config = require 'config'
local V = require 'version'
local scgi = require 'scgi'
local mime = require 'mime'
local uri = require 'url'
local log = require 'log'
local core = require 'core'
local triggers = require 'triggers'
local ldap = require 'bind_ldap'

local function REF(arg)
    return '/' .. V.package_code .. arg
end

local function istid(username)
    for w in string.gmatch(username, '[^a-zA-Z0-9-_]') do
	return false
    end
    return true
end

local function containsAttr(ar, v)
    return type(ar) == 'table' and core.contains(ar, v) or ar == v
end

local function getDBID(tid, ip)
    local a = nil
    local ld, err = ldap.open_simple(config.ldap.uri, config.ldap.bind_dn, config.ldap.bind_pw, config.ldap.tls)

    if ld == nil or err then
	log.w(string.format("%s:%d %s", debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline, err));
    else
	s = config.ldap.search.user_credits
	if s.filter ~= nil then
	    s.filter = s.filter:replace("%1", tid)
	    s.filter = s.filter:replace("%tid", tid)
	    s.filter = s.filter:replace("%2", ip)
	    s.filter = s.filter:replace("%ip", ip)
	end
	for dn, attrs in ld:search(s) do
	    if containsAttr(attrs.arkToken, tid) and containsAttr(attrs.arkAcceptedHost, ip) then
		a = attrs.ErpCode
		break
	    end
	end
	ld:close()
    end

    return a
end

function websvc_main()
    return {
	request_handler = function(env, content_size, content, res) -- request handler
	    assert(env.QUERY_STRING ~= nil, "invalid request. QUERY_STRING is unavailable.")
	    assert(env.REQUEST_METHOD ~= nil, "invalid request. REQUEST_METHOD is unavailable.")

	    local script = env.PATH_INFO or env.SCRIPT_NAME
	    local params = uri.parseQuery(env.QUERY_STRING)

	    if script == nil or script == REF('/about:echo') then
		scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
		scgi.writeBody(res, json.encode(env))
	    elseif script == REF("/") then
		scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
		scgi.writeBody(res, "{\"msg\":\"" .. V.package_name .. " " .. V.package_version .. "\"}")
	    elseif params.tid == nil or #params.tid == 0 or not istid(params.tid) then
		scgi.writeHeader(res, 401, {["Content-Type"] = mime.json .. "; charset=utf-8"})
		scgi.writeBody(res, "{\"msg\":\"Not permitted\"}")
	    else
		local db_id = getDBID(params.tid, env.REMOTE_ADDR)
		if db_id == null or #db_id == 0 then
		    scgi.writeHeader(res, 401, {["Content-Type"] = mime.json .. "; charset=utf-8"})
		    scgi.writeBody(res, "{\"msg\":\"Not permitted\"}")
		    log.w(string.format("[audit] tokenid %s from %s -> permission denied -> %s", params.tid, env.REMOTE_ADDR, script))
		else
		    local ptr = triggers[ string.sub(script, #V.package_code + 3) ]
		    if ptr ~= nil then
			log.w(string.format("[audit] tokenid %s from %s -> permission granted -> %s", params.tid, env.REMOTE_ADDR, script))
			ptr.main(res, db_id, env.REQUEST_METHOD, params, content, env.CONTENT_TYPE, env.REMOTE_ADDR)
		    else
			scgi.writeHeader(res, 404, {["Content-Type"] = mime.json .. "; charset=utf-8"})
			scgi.writeBody(res, "{\"msg\":\"Not Found\"}")
			log.w(string.format("[audit] tokenid %s from %s -> not found -> %s", params.tid, env.REMOTE_ADDR, script))
		    end
		end
	    end

	    return 0
	end
    }
end
