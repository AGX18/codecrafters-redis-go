package main

import (
	"bufio"
	"container/list"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var logger = log.New(os.Stderr, "DEBUG: ", log.LstdFlags)

func main() {
	store := &Store{
		data:  make(map[string]Entry),
		lists: make(map[string]*list.List),
	}
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
			if len(args) < 3 {
				writeError(conn, "SET command requires at least 2 arguments")
			} else {
				if len(args) == 5 {
					multipliers := map[string]time.Duration{
						"PX": time.Millisecond,
						"EX": time.Second,
					}

					unit := strings.ToUpper(args[3])
					multiplier, ok := multipliers[unit]
					if !ok {
						writeError(conn, "Invalid expiry option")
						return
					}

					duration, err := strconv.Atoi(args[4])
					if err != nil {
						writeError(conn, "Invalid duration")
						return
					}

					store.SetWithExpiry(args[1], args[2], time.Duration(duration)*multiplier)
					writeSimpleString(conn, "OK")
					continue
				} else if len(args) == 3 { // No expiry
					store.Set(args[1], args[2])
					writeSimpleString(conn, "OK")
					continue
				} else { // Invalid number of arguments
					writeError(conn, "Invalid number of arguments for SET command")
					continue
				}
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
		case "RPUSH":
			// RPUSH mylist a b c
			if len(args) < 3 {
				writeError(conn, "RPUSH command requires at least 2 arguments")
			} else {
				length := store.RPush(args[1], args[2:])
				writeInteger(conn, length)
			}
		case "LPUSH":
			// LPUSH mylist a b c
			if len(args) < 3 {
				writeError(conn, "LPUSH command requires at least 2 arguments")
			} else {
				length := store.LPUSH(args[1], args[2:])
				writeInteger(conn, length)
			}
		case "LRANGE":
			// The LRANGE command is used to retrieve elements from a list using a start index and a stop index.
			// LRANGE mylist start stop
			if len(args) != 4 {
				writeError(conn, "LRANGE command requires exactly 3 arguments")
			}

			start, err1 := strconv.Atoi(args[2])
			if err1 != nil {
				writeError(conn, "Invalid start index")
				continue
			}

			stop, err2 := strconv.Atoi(args[3])
			if err2 != nil {
				writeError(conn, "Invalid stop index")
				continue
			}

			LRangeResult, exists := store.LRange(args[1], start, stop, conn)
			if exists {
				writeArray(conn, LRangeResult)
			} else {
				writeArray(conn, []string{})
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
