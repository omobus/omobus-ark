-- -*- Lua -*-
-- Copyright (c) 2006 - 2021 omobus-ark authors, see the included COPYRIGHT file.

local M = {} -- public interface

local V = require 'version'
local log = require 'log'
local tds = require 'bind_tds'

local READ_TRAN = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT;\nBEGIN TRANSACTION"
local WRITE_TRAN = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\nBEGIN TRANSACTION"
local COMMIT_TRAN = "COMMIT TRANSACTION"
local ROLLBACK_TRAN = "ROLLBACK TRANSACTION"
local TDS_VERSION = {
    "unknown",
    "2.0/pre 4.0 SQL Server",
    "3.4/Microsoft SQL Server (3.0)",
    "4.0/SQL Server",
    "4.2/SQL Server",
    "2.0/OpenServer and 4.6 SQL Server",
    "4.9.5/(NCR) SQL Server",
    "5.0/SQL Server",
    "7.0/Microsoft SQL Server 7.0",
    "7.1/Microsoft SQL Server 2000",
    "7.2/Microsoft SQL Server 2005"
}

local function isnull(arg)
    return type(arg) == "table" and arg.null == true;
end

local function gettdsversion(tds)
    return TDS_VERSION[(tds >= 1 and tds <= 10) and (tds + 1) or 1]
end

local function dumpparams(query_id, params)
    local f = false
    local str = query_id
    if params ~= nil then
	for k, v in pairs(params) do
	    if f then
		str = str .. "&"
	    else
		str = str .. "?"
		f = true
	    end
	    str = str .. k .. "=" .. (isnull(v) and 'null' or
		(type(v) == "table" and dumparray(v) or v))
	end
    end
    return str
end

local function shielding(arg)
    return arg:replace("\'", "\'\'")
end

local function escape(arg)
    return type(arg) == 'string' and ("'" .. shielding(arg) .. "'") or tostring(arg)
end

local function sqlexec(dbproc, query, params)
    dbproc:dbfreebuf()
    if params ~= nil then
	local tmp = query
	for k,v in pairs(params) do
	    if isnull(v) then
		tmp = tmp:replace(string.format("%%%s%%", k), 'null')
	    else
		tmp = tmp:replace(string.format("%%%s%%", k), escape(v))
	    end
	end
	query = tmp
    end
    return dbproc:dbcmd(query) == tds.SUCCEED and dbproc:dbsqlexec() == tds.SUCCEED
end

local function write_largeobject(dbproc, blob)
    assert(false, "writing BLOB's not implemented")
end

local function putdata(dbproc, colname, coltype, len, ptr, tb)
    if tb == nil or colname == nil or ptr == nil or len == 0 then
	return
    end
    if coltype == tds.SYBCHAR or coltype == tds.SYBTEXT then
	tb[colname] = ptr
    elseif coltype == tds.SYBIMAGE or coltype == tds.SYBBINARY then
	tb[colname] = ptr
    else
	tb[colname] = dbproc:dbconvert(coltype, ptr)
    end
end


-- *** stor_tds interface: begin ***

function M.init()
    tds.dbinit()
    tds.dbmsghandle(function(msgno, msgstate, severity, msgtext, srvname, procname, line)
	    local x = severity > 0 and log.i or log.d
	    x(string.format("%s:%d Msg %d, Level %d, State %d, Server %s. %s",
		debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline, 
		msgno, severity, msgstate, srvname, msgtext));
	end
    )
    tds.dberrhandle(function(severity, dberr, oserr, dberrstr, oserrstr)
	    if dberrstr ~= nil then
		log.e(string.format("%s:%d DB-Library error: %s", 
		    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline, 
		    dberrstr))
		M._errflag = true;
	    end
	    if oserrstr ~= nil then
		log.e(string.format("%s:%d operating-system: %s", 
		    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline, 
		    oserrstr))
		M._errflag = true;
	    end
	    if oserr ~= tds.DBNOERR then
		M._errflag = true;
	    end
	end
    )
    if M.freetdsconf ~= nil then tds.dbsetifile(M.freetdsconf) end
end

function M.cleanup()
    tds.dbexit()
end

function M.connect(server, storage, user, password)
    assert(server, "uninitialized 'server' variable!")
    assert(storage, "uninitialized 'storage' variable!")
    assert(user, "uninitialized 'user' variable!")
    assert(password, "uninitialized 'password' variable!")

    local tb = {}

    tb.login = tds.dblogin()
    if tb.login == nil then
	log.e(string.format("%s:%d unable to create allocate login information with dblogin().", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline))
	return nil
    end
    tb.login:dbsetluser(user)
    tb.login:dbsetlpwd(password)
    tb.login:dbsetlapp(V.package_name .. ":" .. V.package_version)
    tb.dbproc = tb.login:dbopen(server)
    if tb.dbproc == nil then 
	log.e(string.format("%s:%d unable to connect to database %s@%s/%s.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    user, server, storage))
	tb.login:dbfreelogin()
	return nil
    end
    if tb.dbproc:dbuse(storage) ~= tds.SUCCEED then
	log.e(string.format("%s:%d unable to open database %s@%s/%s.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    user, server, storage))
	td.dbproc:dbclose()
	tb.login:dbfreelogin()
	return nil
    end

    log.d(string.format("database %s@%s/%s connected. TDS %s.", 
	user, server, storage, gettdsversion(tb.dbproc:dbtds())))

    return tb
end

function M.disconnect(tb)
    assert(tb, "incorrect connection context")
    tb.dbproc:dbclose()
    tb.login:dbfreelogin()
    tb = nil
    log.d("database disconnected.")
end

function M.begin_tran(tb, readonly)
    assert(tb, "incorrect connection context")
    if not sqlexec(tb.dbproc, readonly and READ_TRAN or WRITE_TRAN) then
	log.e(string.format("%s:%d unable to begin %s transaction.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    readonly and "read" or "write"))
	return nil
    end
    tb.dbproc:dbresults()
    log.d(string.format("%s:%d begin %s transaction.", 
	debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	readonly and "read" or "write"))
    return tb.dbproc
end

local function end_tran(dbproc, commit)
    assert(dbproc, "incorrect transaction context")
    if not sqlexec(dbproc, commit and COMMIT_TRAN or ROLLBACK_TRAN) then
	log.e(string.format("%s:%d unable to %s transaction.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    commit and "commit" or "rollback"))
    else
	dbproc:dbresults()
	log.d(string.format("%s:%d %s transaction.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    commit and "commit" or "rollback"))
    end
end

function M.commit_tran(tran)
    end_tran(tran, true)
end

function M.rollback_tran(tran)
    end_tran(tran, false)
end

function M.execute(dbproc, query, query_id, params, blob)
    assert(dbproc, "incorrect transaction context")
    assert(query and #query>0, "invalid query string")
    assert(query_id, "invalid query_id")

    local restb, v, results, columns, rowinfo, i, row, len

    query = query:replace("[public]", "dbo")

    if blob ~= nil and #blob > 0 then
	local blob_oid = write_largeobject(dbproc, blob)
	if blob_id == nil then
	    log.e(string.format("%s:%d unable to write blob data (size=%d).", 
		debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
		#blob))
	    return nil, true
	end
	query = query:replace("%1:blob%", blob_oid)
    else
	query = query:replace("%1:blob%", "")
    end

    if not sqlexec(dbproc, query, params) then
	log.e(string.format("%s:%d unable to execute query %s.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    dumpparams(query_id, params)))
	return nil, true
    end
    M._errflag = false
    results = dbproc:dbresults()
    while results ~= tds.NO_MORE_RESULTS and results ~= tds.FAIL and not M._errflag do
	columns = dbproc:dbnumcols()
	if columns > 0 then
	    restb = {}
	    row = 1
	    rowinfo = dbproc:dbnextrow()
	    while rowinfo ~= tds.NO_MORE_ROWS and rowinfo ~= tds.FAIL and not M._errflag do
		v = {}
		for i = 1, columns do
		    if rowinfo == tds.REG_ROW then
			len = dbproc:dbdatlen(i)
			if len > 0 then 
			    putdata(dbproc, string.lower(dbproc:dbcolname(i)), dbproc:dbcoltype(i), 
				len, dbproc:dbdata(i), v)
			end
		    else
			len = dbproc:dbadlen(rowinfo, i)
			if len > 0 then 
			    putdata(dbproc, string.lower(dbproc:dbcolname(i)), dbproc:dbalttype(rowinfo, i), 
				len, dbproc:dbadata(rowinfo, i), v)
			end
		    end
		end
		restb[row] = v
		row = row + 1
		rowinfo = dbproc:dbnextrow()
	    end
	end
	results = dbproc:dbresults()
    end
    if results == tds.FAIL or rowinfo == tds.FAIL or M._errflag then
	log.w(string.format("%s:%d unable to get query %s rows.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    dumpparams(query_id, params)))
    else
	log.d(string.format("%s:%d query %s executed successfully. Rows: %d.", 
	    debug.getinfo(1,'S').short_src, debug.getinfo(1, 'l').currentline,
	    dumpparams(query_id, params), restb == nil and 0 or #restb))
    end

    return restb, results == tds.FAIL or rowinfo == tds.FAIL or M._errflag
end

-- *** stor_tds interface: end ***

M.freetdsconf 	= "/freetds.conf"

return M
