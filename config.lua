-- -*- Lua -*-
-- Copyright (c) 2006 - 2022 omobus-ark authors, see the included COPYRIGHT file.

local M = {} -- public interface

-- *** LDAP server parameters ***
M.ldap		= {
    uri		= "ldap://127.0.0.1:389",
    bind_dn	= "uid=omobus-scgid,ou=services,dc=omobus,dc=local",
    bind_pw	= "0",
    tls		= false,
    search	= {
	user_credits = { 
	    base	= "ou=lts-db,dc=omobus,dc=local",
	    scope 	= "subtree",
	    filter	= "(&(objectClass=omobusArk)(arkStatus=enabled)(arkToken=%1)(arkAcceptedHost=%2))",
	    attrs	= {"cn", "ErpCode", "arkStatus", "arkToken", "arkAcceptedHost"}
	}
    }
}

-- *** LTS storage parameters ***
M.data 		= {
    --_LIB 	= require 'stor_tds',
    server	= "hostaddr=127.0.0.1 port=5432 application_name=omobus-ark",
    storage	= "omobus-lts-db",
    user	= "omobus",
    password	= "omobus"
}

return M
