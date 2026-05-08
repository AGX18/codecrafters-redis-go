package main

import (
	"bytes"
	"net"
)

type bufConn struct {
	net.Conn
	bytes.Buffer
}

func (b *bufConn) Write(p []byte) (int, error) {
	return b.Buffer.Write(p)
}

func (b *bufConn) Read(p []byte) (int, error) {
	return b.Buffer.Read(p)
}
