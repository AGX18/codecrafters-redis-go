package resp

import (
	"fmt"
	"net"
	"strconv"
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
