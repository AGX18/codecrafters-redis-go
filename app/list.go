package main

import (
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/resp"
	Store "github.com/codecrafters-io/redis-starter-go/store"
)

func RPUSH(args []string, conn net.Conn, store *Store.Store) {
	if len(args) < 3 {
		resp.WriteError(conn, "RPUSH command requires at least 2 arguments")
	} else {
		length := store.RPush(args[1], args[2:])
		resp.WriteInteger(conn, length)
	}

}
func LPUSH(args []string, conn net.Conn, store *Store.Store) {
	if len(args) < 3 {
		resp.WriteError(conn, "RPUSH command requires at least 2 arguments")
	} else {
		length := store.RPush(args[1], args[2:])
		resp.WriteInteger(conn, length)
	}
}

func LRANGE(args []string, conn net.Conn, store *Store.Store) {
	if len(args) != 4 {
		resp.WriteError(conn, "LRANGE command requires exactly 3 arguments")
	}

	start, err1 := strconv.Atoi(args[2])
	if err1 != nil {
		resp.WriteError(conn, "Invalid start index")
		return
	}

	stop, err2 := strconv.Atoi(args[3])
	if err2 != nil {
		resp.WriteError(conn, "Invalid stop index")
		return
	}

	LRangeResult, exists := store.LRange(args[1], start, stop)
	if exists {
		resp.WriteArray(conn, LRangeResult)
	} else {
		resp.WriteArray(conn, []string{})
	}
}
func LLEN(args []string, conn net.Conn, store *Store.Store) {
	if len(args) != 2 {
		resp.WriteError(conn, "LLEN command requires exactly 1 argument")
	} else {
		length := store.LLEN(args[1])
		resp.WriteInteger(conn, length)
	}
}

func LPOP(args []string, conn net.Conn, store *Store.Store) {
	if len(args) < 2 || len(args) > 3 {
		resp.WriteError(conn, "LPOP command requires 1 or 2 arguments")
		return
	}

	if len(args) == 3 {
		var err error
		count, err := strconv.Atoi(args[2])
		if err != nil {
			resp.WriteError(conn, "Invalid count")
			return
		}
		poppedValues, exists := store.LPOPArray(args[1], count)
		if exists {
			resp.WriteArray(conn, poppedValues)
		} else {
			resp.WriteNull(conn)
		}
	} else {
		poppedValue, exists := store.LPOP(args[1])
		if exists {
			resp.WriteBulkString(conn, poppedValue)
		} else {
			resp.WriteNull(conn)
		}
	}
}

func BLPOP(args []string, conn net.Conn, store *Store.Store) {
	if len(args) != 3 {
		resp.WriteError(conn, "BLPOP command requires exactly 2 arguments")
		return
	} else {
		timeout, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			logger.Printf("Invalid timeout value: %s", args[2])
			resp.WriteError(conn, "Invalid timeout")
			return
		}
		value, exists := store.BLPOP(args[1], timeout)
		if exists {
			resp.WriteArray(conn, []string{args[1], value})
		} else {
			resp.WriteNullArray(conn)
		}
	}
}

func TYPE(args []string, conn net.Conn, store *Store.Store) {
	if len(args) != 2 {
		resp.WriteError(conn, "TYPE command requires exactly 1 argument")
	} else {
		dataType := store.KeyType(args[1])
		resp.WriteSimpleString(conn, string(dataType))
	}
}
