package main

import (
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/resp"
	Store "github.com/codecrafters-io/redis-starter-go/store"
)

func XADD(args []string, conn net.Conn, store *Store.Store) {
	if len(args) < 5 || len(args)%2 == 0 {
		resp.WriteError(conn, "XADD command requires at least 5 arguments and an even number of arguments")
		return
	}
	key, id, fields, err := Store.GetXAddArgs(args)
	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	entryID, err := store.XAdd(key, id, fields)
	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	resp.WriteBulkString(conn, entryID)
}

func XRANGE(args []string, conn net.Conn, store *Store.Store) {
	if len(args) < 2 {
		resp.WriteError(conn, "XRANGE command requires at least 2 arguments")
		return
	}
	var key, start, end string

	if len(args) >= 2 {
		key = args[1]
		key = strings.TrimSpace(key)
	}
	if len(args) >= 3 {
		start = args[2]
		start = strings.TrimSpace(start)
	} else {
		start = ""
	}
	if len(args) >= 4 {
		end = args[3]
		end = strings.TrimSpace(end)
	} else {
		end = ""
	}

	entries, err := store.XRange(key, start, end)
	logger.Printf("XRANGE result for key: %s, start: %s, end: %s is %v with error: %v", key, start, end, entries, err)
	if err == nil {
		resp.WriteStreamEntries(conn, entries)
	} else {
		logger.Printf("Error retrieving stream entries: %v", err)
		resp.WriteArray(conn, []string{})
	}
}
func XREAD(args []string, conn net.Conn, store *Store.Store) {
	if len(args) < 4 {
		resp.WriteError(conn, "XREAD command requires at least 4 arguments")
		return
	}
	xreadArgs, err := parseXReadArgs(args[1:])
	if err != nil {
		resp.WriteError(conn, err.Error())
	}

	if !xreadArgs.options.block {
		// if not blocking
		entries, err := store.XRead(xreadArgs.keys, xreadArgs.ids)
		logger.Printf("XREAD result for keys: %v, ids: %v is %v with error: %v", xreadArgs.keys, xreadArgs.ids, entries, err)
		if err == nil {
			logger.Printf("Error reading stream entries: %v", err)
			resp.WriteStreamResults(conn, xreadArgs.keys, entries)
		} else {
			resp.WriteArray(conn, []string{})
		}

	} else {
		entries, err := store.XReadBlocking(xreadArgs.options.timeout, xreadArgs.keys, xreadArgs.ids)
		if err != nil {
			resp.WriteNullArray(conn)
			logger.Printf("Error reading stream entries: %v", err)
		} else {
			resp.WriteStreamResults(conn, xreadArgs.keys, entries)
		}

	}
}
