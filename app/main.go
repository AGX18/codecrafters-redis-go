package main

import (
	"bufio"
	"log"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	logger := log.New(os.Stderr, "DEBUG: ", log.LstdFlags)

	logger.Println("Starting the Program")
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		logger.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Println("Error accepting connection:", err)
			continue
		}
		logger.Println("Client connected:", conn.RemoteAddr())
		// reading and parsing RESP commands from the client and responding with a simple PONG message
		conn.Write([]byte(parseRESP(bufio.NewReader(conn))))
		conn.Close()
	}
}

func parseRESP(reader *bufio.Reader) string {
	_, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}

	return "+PONG\r\n"
}
