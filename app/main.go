package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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
		HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	args, err := parseRESP(reader)

	if err != nil {
		conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
		return
	}

	if len(args) == 0 {
		conn.Write([]byte("-ERR No command provided\r\n"))
		return
	}

	conn.Write([]byte("+PONG\r\n"))
}

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)

	if line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %s", line)
	}

	count, _ := strconv.Atoi(line[1:])
	args := make([]string, 0, count)

	for i := 0; i < count; i++ {
		// Read the $N line
		reader.ReadString('\n')

		// Read the actual value
		value, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args = append(args, strings.TrimSpace(value))
	}

	return args, nil
}
