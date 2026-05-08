package main

import (
	"net"

	"github.com/codecrafters-io/redis-starter-go/resp"
	Store "github.com/codecrafters-io/redis-starter-go/store"
)

func INCR(args []string, conn net.Conn, store *Store.Store) {
	result, err := store.INCR(args[1])

	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	resp.WriteInteger(conn, result)
}
