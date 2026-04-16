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
	store := &Store{}
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
		go HandleConnection(conn, store)
	}
}

func HandleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := parseRESP(reader)
		logger.Printf("Parsed arguments: %v", args)
		if err != nil {
			writeError(conn, err.Error())
			return
		}
		if len(args) == 0 {
			writeError(conn, "No Command Provided")
			return
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			writeSimpleString(conn, "PONG")
		case "ECHO":
			if len(args) > 1 {
				writeBulkString(conn, strings.TrimSpace(strings.Join(args[1:], " ")))
			} else {
				writeError(conn, "ECHO command requires an argument")
			}
		case "SET":
			if len(args) != 3 {
				writeError(conn, "SET command requires exactly 2 arguments")
			} else {
				store.Set(args[1], args[2])
				writeSimpleString(conn, "OK")
			}
		case "GET":
			if len(args) != 2 {
				writeError(conn, "GET command requires exactly 1 argument")
			} else {
				value, exists := store.Get(args[1])
				if exists {
					writeBulkString(conn, value)
				} else {
					writeNull(conn)
				}
			}
		default:
			writeError(conn, "Unknown Command: "+args[0])
		}

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
