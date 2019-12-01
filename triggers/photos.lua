-- -*- Lua -*-
-- This file is a part of the omobus-console project.
-- Copyright (c) 2006 - 2018 ak-obs, Ltd. <info@omobus.net>.
-- Author: Igor Artemov <i_artemov@omobus.net>.

local M = {} -- public interface

local config = require 'config'
local mime = require 'mime'
local scgi = require 'scgi'
local validate = require 'validate'
local core = require 'core'
local stor = require 'stor'
local zlib = require 'zlib'
local log = require 'log'


local function compress(tb)
    return zlib.deflate(6):finish(tb)
end

function M.main(res, db_id, method, params)
    if params.year ~= nil and type(params.year) ~= 'number' then
	params.year = tonumber(params.year)
    end

    if params.account_id ~= nil then
	if params.year == nil then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [year] parameter.\"}")
	elseif not validate.isuid(params.account_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [account_id] parameter.\"}")
	elseif params.placement_id ~= nil and not validate.isuid(params.placement_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [placement_id] parameter.\"}")
	elseif params.brand_id ~= nil and not validate.isuid(params.brand_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [brand_id] parameter.\"}")
	elseif params.photo_type_id ~= nil and not validate.isuid(params.photo_type_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [photo_type_id] parameter.\"}")
	else
	    local tb, err = {}, nil
	    tb._request = {}
	    tb._request.year = params.year
	    tb._request.account_id = params.account_id

	    stor.init()
	    tb.photos, err = stor.get(function(tran, func_execute, engine_code) return func_execute(tran, 
[[
select
    x.doc_id, x.fix_month, left(x.fix_dt,10) fix_date, x.placement_id, x.brand_id, x.photo_type_id, x.photo ref_id, x.doc_note, 
    [public].uids_out(x.photo_param_ids) photo_param_ids, case when r.doc_id is null then null else 1 end revoked
from photos x
    left join revocations r on r.db_id = x.db_id and r.doc_id = x.doc_id and r.hidden = 0
where x.db_id = %db_id% and x.fix_year = %year% and x.account_id = %account_id%
    and (%placement_id% is null or x.placement_id = %placement_id%)
    and (%brand_id% is null or x.brand_id = %brand_id%)
    and (%photo_type_id% is null or x.photo_type_id = %photo_type_id%)
order by fix_dt desc, doc_id
]]
		, "//photos/blobs", {db_id = db_id, year = params.year, account_id = params.account_id,
			placement_id = params.placement_id == null and stor.NULL or params.placement_id,
			brand_id = params.brand_id == null and stor.NULL or params.brand_id,
			photo_type_id = params.photo_type_id == null and stor.NULL or params.photo_type_id
		    })
	    end)
	    stor.cleanup()

	    if err then
		scgi.writeHeader(res, 500, {["Content-Type"] = mime.json .. "; charset=utf-8"})
		scgi.writeBody(res, "{\"msg\":\"Internal server error\"}")
	    else
		if tb.photos ~= nil and #tb.photos > 0 then
		    for _, x in ipairs(tb.photos) do
			if x.photo_param_ids ~= nil then
			    x.photo_param_ids = core.split(x.photo_param_ids, ',')
			end
		    end
		end
		scgi.writeHeader(res, 200, {["Content-Type"] = mime.json .. "; charset=utf-8", ["Content-Encoding"] = "deflate"})
		scgi.writeBody(res, compress(json.encode(tb)))
	    end
	end
    elseif params.year ~= nil then
	if params.placement_id ~= nil and not validate.isuid(params.placement_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [placement_id] parameter.\"}")
	elseif params.brand_id ~= nil and not validate.isuid(params.brand_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [brand_id] parameter.\"}")
	elseif params.photo_type_id ~= nil and not validate.isuid(params.photo_type_id) then
	    scgi.writeHeader(res, 400, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Invalid [photo_type_id] parameter.\"}")
	else
	    stor.init()
	    local tb, err = stor.get(function(tran, func_execute)
		local tb, err = {}, nil
		tb.objects = {}
		tb._request = {}
		tb._request.year = params.year
		tb._request.placement_id = params.placement_id
		tb._request.brand_id = params.brand_id
		tb._request.photo_type_id = params.photo_type_id
		tb.rows, err = func_execute(tran,
[[
select 
    a.account_id, a.code, a.descr, a.address, a.region_id, a.city_id, a.rc_id, a.chan_id, a.poten_id, a.cash_register, a.hidden, 
    p.m1, p.m2, p.m3, p.m4, p.m5, p.m6, p.m7, p.m8, p.m9, p.m10, p.m11, p.m12
from (
    select db_id, account_id, 
	sum(case fix_month when 1 then 1 else 0 end) m1, sum(case fix_month when 2 then 1 else 0 end) m2, sum(case fix_month when 3 then 1 else 0 end) m3, 
	sum(case fix_month when 4 then 1 else 0 end) m4, sum(case fix_month when 5 then 1 else 0 end) m5, sum(case fix_month when 6 then 1 else 0 end) m6, 
	sum(case fix_month when 7 then 1 else 0 end) m7, sum(case fix_month when 8 then 1 else 0 end) m8, sum(case fix_month when 9 then 1 else 0 end) m9,
	sum(case fix_month when 10 then 1 else 0 end) m10, sum(case fix_month when 11 then 1 else 0 end) m11, sum(case fix_month when 12 then 1 else 0 end) m12
    from photos
	where db_id = %db_id% and fix_year = %year%
	    and (%placement_id% is null or placement_id = %placement_id%)
	    and (%brand_id% is null or brand_id = %brand_id%)
	    and (%photo_type_id% is null or photo_type_id = %photo_type_id%)
    group by db_id, account_id
) p, accounts a
    where p.db_id = a.db_id and p.account_id = a.account_id
order by descr, address, account_id
]]
		    , "//photos/accounts", {
			db_id = db_id, 
			year = params.year, 
			placement_id = params.placement_id == null and stor.NULL or params.placement_id,
			brand_id = params.brand_id == null and stor.NULL or params.brand_id,
			photo_type_id = params.photo_type_id == null and stor.NULL or params.photo_type_id
		    })
		if err == nil or err == false then
		    tb.objects.regions, err = func_execute(tran,
[[
select region_id, descr, country_id, hidden from regions
    where db_id = %db_id%
order by descr
]]
			, "//photos/regions", {db_id = db_id})
		end
		if err == nil or err == false then
		    tb.objects.cities, err = func_execute(tran,
[[
select city_id, pid, ftype, descr, country_id, hidden from cities
    where db_id = %db_id%
order by descr
]]
			, "//photos/cities", {db_id = db_id})
		end
		if err == nil or err == false then
		    tb.objects.retail_chains, err = func_execute(tran,
[[
select rc_id, descr, ka_code, hidden from retail_chains
    where db_id = %db_id%
order by descr
]]
			, "//photos/retail_chains", {db_id = db_id})
		end
		if err == nil or err == false then
		    tb.objects.channels, err = func_execute(tran,
[[
select chan_id, descr, hidden from channels
    where db_id = %db_id%
order by descr
]]
			, "//photos/channels", {db_id = db_id})
		end
		if err == nil or err == false then
		    tb.objects.potentials, err = func_execute(tran,
[[
select poten_id, descr, hidden from potentials
    where db_id = %db_id%
order by descr
]]
			, "//photos/potentials", {db_id = db_id})
		end
		if err == nil or err == false then
		    tb.objects.placements, err = func_execute(tran,
[[
select placement_id, descr, hidden from placements
    where db_id = %db_id% and placement_id in (select distinct placement_id from photos where db_id = %db_id% and fix_year = %year%)
order by hidden, row_no, descr
]]
			, "//photos/placements", {db_id = db_id, year = params.year})
		end
		if err == nil or err == false then
		    tb.objects.brands, err = func_execute(tran,
[[
select
    b.brand_id, b.descr, m.descr manuf, m.competitor, b.hidden
from brands b
    left join manufacturers m on m.db_id = b.db_id and m.manuf_id = b.manuf_id
where b.db_id = %db_id% and b.brand_id in (select distinct brand_id from photos where db_id = %db_id% and fix_year = %year%)
order by /*m.competitor nulls first,*/ b.row_no, b.descr
]]
			, "//photos/brands", {db_id = db_id, year = params.year})
		end
		if err == nil or err == false then
		    tb.objects.photo_types, err = func_execute(tran,
[[
select photo_type_id, descr, hidden from photo_types
    where db_id = %db_id% and photo_type_id in (select distinct photo_type_id from photos where db_id = %db_id% and fix_year = %year%)
order by hidden, row_no, descr
]]
			, "//photos/photo_types", {db_id = db_id, year = params.year})
		end
		if err == nil or err == false then
		    tb.objects.photo_params, err = func_execute(tran,
[[
select photo_param_id, descr, hidden from photo_params
    where db_id = %db_id%
order by hidden, row_no, descr
]]
			, "//photos/photo_params", {db_id = db_id, year = params.year})
		end
		if err == nil or err == false then
		    tb._sys, err = func_execute(tran,
[[
select 'data_ts' param_id, param_value from sysparams where param_id = concat(%db_id%,':TS')
    union
select param_id, param_value from sysparams where param_id = 'db:id'
]]
			, "//photos/_sys", {db_id = db_id})
		end
		return tb, err
	    end)
	    stor.cleanup()
	    if err then
		scgi.writeHeader(res, 500, {["Content-Type"] = mime.json .. "; charset=utf-8"})
		scgi.writeBody(res, "{\"msg\":\"Internal server error\"}")
	    else
		local idx_cities = {}
		local idx_regions = {}
		local idx_rcs = {}
		local idx_channels = {}
		local idx_potentials = {}

		for i, v in ipairs(tb.rows or {}) do
		    if v.region_id ~= nil then idx_regions[v.region_id] = 1; end
		    if v.city_id ~= nil then idx_cities[v.city_id] = 1; end
		    if v.rc_id ~= nil then idx_rcs[v.rc_id] = 1; end
		    if v.chan_id ~= nil then idx_channels[v.chan_id] = 1; end
		    if v.poten_id ~= nil then idx_potentials[v.poten_id] = 1; end
		    v.photos = {v.m1, v.m2, v.m3, v.m4, v.m5, v.m6, v.m7, v.m8, v.m9, v.m10, v.m11, v.m12}
		    v.m1 = nil; v.m2 = nil; v.m3 = nil; v.m4 = nil; v.m5 = nil;
		    v.m6 = nil; v.m7 = nil; v.m8 = nil; v.m9 = nil; v.m10 = nil; v.m11 = nil; v.m12 = nil;
		    v.row_no = i
		end

		tb.objects.regions = core.reduce(tb.objects.regions, 'region_id', idx_regions)
		tb.objects.cities = core.reduce(tb.objects.cities, 'city_id', idx_cities)
		tb.objects.retail_chains = core.reduce(tb.objects.retail_chains, 'rc_id', idx_rcs)
		tb.objects.channels = core.reduce(tb.objects.channels, 'chan_id', idx_channels)
		tb.objects.potentials = core.reduce(tb.objects.potentials, 'poten_id', idx_potentials)

		for _, v in ipairs(tb._sys) do
		    if v.param_id == 'data_ts' then
			tb.data_ts = v.param_value
		    end
		    if v.param_id == 'db:id' then
			tb.storage = v.param_value
		    end
		end
		tb._sys = nil

		scgi.writeHeader(res, 200, {["Content-Type"] = mime.json .. "; charset=utf-8", ["Content-Encoding"] = "deflate"})
		scgi.writeBody(res, compress(json.encode(tb)))
	    end
	end
    else
	local tb, err
	stor.init()
	tb, err = stor.get(function(tran, func_execute) return func_execute(tran,
[[
select distinct fix_year from photos
    where db_id = %db_id%
order by 1 desc
]]
	    , "//photos/blobs", {db_id = db_id, year = params.year, account_id = params.account_id})
	end)
	stor.cleanup()

	if err then
	    scgi.writeHeader(res, 500, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "{\"msg\":\"Internal server error\"}")
	elseif tb == nil or #tb == 0 then
	    scgi.writeHeader(res, 200, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, "[]")
	else
	    local tmp = {}
	    for i, v in ipairs(tb) do
		table.insert(tmp, v.fix_year)
	    end
	    scgi.writeHeader(res, 200, {["Content-Type"] = mime.json .. "; charset=utf-8"})
	    scgi.writeBody(res, json.encode(tmp))
	end
    end
end

return M
