package main

import (
	"fmt"
	"net"
	"strconv"
)

func writeSimpleString(conn net.Conn, value string) {
	conn.Write([]byte("+" + value + "\r\n"))
}

func writeError(conn net.Conn, message string) {
	conn.Write([]byte("-ERR " + message + "\r\n"))
}

func writeBulkString(conn net.Conn, value string) {
	conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
}

func writeNull(conn net.Conn) {
	conn.Write([]byte("$-1\r\n"))
}

func writeInteger(conn net.Conn, value int) {
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", value)))
}
