-- -*- Lua -*-
-- This file is a part of the omobus-console project.
-- Copyright (c) 2006 - 2018 ak-obs, Ltd. <info@omobus.net>.
-- Author: Igor Artemov <i_artemov@omobus.net>.

local M = {} -- public interface

local config = require 'config'
local mime = require 'mime'
local scgi = require 'scgi'
local validate = require 'validate'
local stor = require 'stor'

function M.main(res, db_id, method, params)
    if not validate.isuid(params.ref) then
	scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	scgi.writeBody(res, "{\"msg\":\"Invalid [ref] parameter.\"}")
    else
	stor.init()
	local tb, err = stor.get(function(tran, func_execute) return func_execute(tran,
[[
select thumb from thumbnails where db_id = %db_id% and ref_id = %ref_id%
]]
	    , "//thumb", {db_id = db_id, ref_id = params.ref})
	end)
	stor.cleanup()

	if err then
	    scgi.writeHeader(res, 500, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Internal server error\"}")
	elseif tb == nil or #tb ~= 1 or tb[1].thumb == nil then
	    scgi.writeHeader(res, 404, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Not Found\"}")
	else
	    scgi.writeHeader(res, 200, {["Content-Type"] = mime.jpeg})
	    scgi.writeBody(res, tb[1].thumb)
	end
    end
end

return M
