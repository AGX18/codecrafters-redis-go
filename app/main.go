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

var logger = log.New(os.Stderr, "DEBUG: ", log.LstdFlags)

func main() {

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
		go HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := parseRESP(reader)
		logger.Printf("Parsed arguments: %v", args)
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

}

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	logger.Printf("Received line: %s", line)

	if line[0] != '*' {
		return nil, fmt.Errorf("expected array, got %s", line)
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %s", line[1:])
	}
	args := make([]string, 0, count)

	for range count {
		// Read the $N line
		logger.Printf("Reading argument %d", len(args)+1)
		n, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		logger.Printf("Received length: %s", n)

		// Read the actual value
		value, err := reader.ReadString('\n')
		logger.Printf("Received argument: %s", value)
		if err != nil {
			return nil, err
		}
		args = append(args, strings.TrimSpace(value))
	}

	return args, nil
}
