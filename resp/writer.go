package resp

import (
	"fmt"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/store"
)

func WriteSimpleString(conn net.Conn, value string) {
	conn.Write([]byte("+" + value + "\r\n"))
}

func WriteError(conn net.Conn, message string) {
	conn.Write([]byte("-ERR " + message + "\r\n"))
}

func WriteBulkString(conn net.Conn, value string) {
	conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
}

func WriteNull(conn net.Conn) {
	conn.Write([]byte("$-1\r\n"))
}

func WriteInteger(conn net.Conn, value int) {
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", value)))
}

func WriteArray(conn net.Conn, values []string) {
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(values))))
	for _, v := range values {
		WriteBulkString(conn, v)
	}
}

func WriteNullArray(conn net.Conn) {
	conn.Write([]byte("*-1\r\n"))
}

func WriteStreamEntries(conn net.Conn, entries []store.StreamEntry) {
	fmt.Fprintf(conn, "*%d\r\n", len(entries))
	for _, entry := range entries {
		conn.Write([]byte("*2\r\n"))
		WriteBulkString(conn, entry.ID())
		fields := []string{}
		for k, v := range entry.Fields() {
			fields = append(fields, k, v)
		}
		WriteArray(conn, fields)
	}
}
