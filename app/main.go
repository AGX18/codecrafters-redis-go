package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	Store "github.com/codecrafters-io/redis-starter-go/store"
)

var logger = log.New(os.Stderr, "DEBUG: ", log.LstdFlags)

type XReadOptions struct {
	block   bool
	timeout time.Duration
	count   int // 0 means no limit
}

type XReadArgs struct {
	options XReadOptions
	keys    []string
	ids     []string
}

func parseXReadArgs(args []string) (XReadArgs, error) {
	result := XReadArgs{}
	i := 0

	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "COUNT":
			i++
			count, err := strconv.Atoi(args[i])
			if err != nil {
				return result, fmt.Errorf("invalid COUNT value")
			}
			result.options.count = count
		case "BLOCK":
			i++
			ms, err := strconv.Atoi(args[i])
			if err != nil {
				return result, fmt.Errorf("invalid BLOCK value")
			}
			result.options.block = true
			result.options.timeout = time.Duration(ms) * time.Millisecond
		case "STREAMS":
			i++
			remaining := args[i:]
			if len(remaining)%2 != 0 {
				return result, fmt.Errorf("unbalanced keys and ids")
			}
			mid := len(remaining) / 2
			result.keys = remaining[:mid]
			result.ids = remaining[mid:]
			return result, nil
		default:
			return result, fmt.Errorf("unknown option: %s", args[i])
		}
		i++
	}

	return result, fmt.Errorf("missing STREAMS keyword")
}

func main() {
	store := Store.NewStore(logger)

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
