package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/resp"
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

func HandleConnection(conn net.Conn, store *Store.Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := resp.ParseRESP(reader)
		logger.Printf("Parsed arguments: %v", args)
		if err != nil {
			resp.WriteError(conn, err.Error())
			return
		}
		if len(args) == 0 {
			resp.WriteError(conn, "No Command Provided")
			return
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			resp.WriteSimpleString(conn, "PONG")
		case "ECHO":
			if len(args) > 1 {
				resp.WriteBulkString(conn, strings.TrimSpace(strings.Join(args[1:], " ")))
			} else {
				resp.WriteError(conn, "ECHO command requires an argument")
			}
		case "SET":
			if len(args) < 3 {
				resp.WriteError(conn, "SET command requires at least 2 arguments")
			} else {
				if len(args) == 5 {
					multipliers := map[string]time.Duration{
						"PX": time.Millisecond,
						"EX": time.Second,
					}

					unit := strings.ToUpper(args[3])
					multiplier, ok := multipliers[unit]
					if !ok {
						resp.WriteError(conn, "Invalid expiry option")
						return
					}

					duration, err := strconv.Atoi(args[4])
					if err != nil {
						resp.WriteError(conn, "Invalid duration")
						return
					}

					store.SetWithExpiry(args[1], args[2], time.Duration(duration)*multiplier)
					resp.WriteSimpleString(conn, "OK")
					continue
				} else if len(args) == 3 { // No expiry
					store.Set(args[1], args[2])
					resp.WriteSimpleString(conn, "OK")
					continue
				} else { // Invalid number of arguments
					resp.WriteError(conn, "Invalid number of arguments for SET command")
					continue
				}
			}
		case "GET":
			if len(args) != 2 {
				resp.WriteError(conn, "GET command requires exactly 1 argument")
			} else {
				value, exists := store.Get(args[1])
				if exists {
					resp.WriteBulkString(conn, value)
				} else {
					resp.WriteNull(conn)
				}
			}
		case "RPUSH":
			// RPUSH mylist a b c
			if len(args) < 3 {
				resp.WriteError(conn, "RPUSH command requires at least 2 arguments")
			} else {
				length := store.RPush(args[1], args[2:])
				resp.WriteInteger(conn, length)
			}
		case "LPUSH":
			// LPUSH mylist a b c
			if len(args) < 3 {
				resp.WriteError(conn, "LPUSH command requires at least 2 arguments")
			} else {
				length := store.LPUSH(args[1], args[2:])
				resp.WriteInteger(conn, length)
			}
		case "LRANGE":
			// The LRANGE command is used to retrieve elements from a list using a start index and a stop index.
			// LRANGE mylist start stop
			if len(args) != 4 {
				resp.WriteError(conn, "LRANGE command requires exactly 3 arguments")
			}

			start, err1 := strconv.Atoi(args[2])
			if err1 != nil {
				resp.WriteError(conn, "Invalid start index")
				continue
			}

			stop, err2 := strconv.Atoi(args[3])
			if err2 != nil {
				resp.WriteError(conn, "Invalid stop index")
				continue
			}

			LRangeResult, exists := store.LRange(args[1], start, stop)
			if exists {
				resp.WriteArray(conn, LRangeResult)
			} else {
				resp.WriteArray(conn, []string{})
			}

		case "LLEN":
			// The LLEN command is used to get the length of a list.
			// LLEN mylist
			if len(args) != 2 {
				resp.WriteError(conn, "LLEN command requires exactly 1 argument")
			} else {
				length := store.LLEN(args[1])
				resp.WriteInteger(conn, length)
			}

		case "LPOP":
			// The LPOP command is used to remove and return the first element of a list.
			// LPOP mylist [count]
			if len(args) < 2 || len(args) > 3 {
				resp.WriteError(conn, "LPOP command requires 1 or 2 arguments")
				continue
			}

			if len(args) == 3 {
				var err error
				count, err := strconv.Atoi(args[2])
				if err != nil {
					resp.WriteError(conn, "Invalid count")
					continue
				}
				poppedValues, exists := store.LPOPArray(args[1], count)
				if exists {
					resp.WriteArray(conn, poppedValues)
				} else {
					resp.WriteNull(conn)
				}
			} else {
				poppedValue, exists := store.LPOP(args[1])
				if exists {
					resp.WriteBulkString(conn, poppedValue)
				} else {
					resp.WriteNull(conn)
				}
			}
		case "BLPOP":
			// The BLPOP command is used to remove and return the first element of a list, or block until one is available.
			// BLPOP mylist timeout
			if len(args) != 3 {
				resp.WriteError(conn, "BLPOP command requires exactly 2 arguments")
				continue
			} else {
				timeout, err := strconv.ParseFloat(args[2], 64)
				if err != nil {
					logger.Printf("Invalid timeout value: %s", args[2])
					resp.WriteError(conn, "Invalid timeout")
					continue
				}
				value, exists := store.BLPOP(args[1], timeout)
				if exists {
					resp.WriteArray(conn, []string{args[1], value})
				} else {
					resp.WriteNullArray(conn)
				}
			}

		case "TYPE":
			// The TYPE command is used to determine the type of the value stored at a key.
			// TYPE mykey
			if len(args) != 2 {
				resp.WriteError(conn, "TYPE command requires exactly 1 argument")
			} else {
				dataType := store.KeyType(args[1])
				resp.WriteSimpleString(conn, string(dataType))
			}

		case "XADD":
			// The XADD command is used to append a new entry to a stream.
			// XADD mystream * field1 value1 field2 value2
			if len(args) < 5 || len(args)%2 == 0 {
				resp.WriteError(conn, "XADD command requires at least 5 arguments and an even number of arguments")
				continue
			}
			key, id, fields, err := Store.GetXAddArgs(args)
			if err != nil {
				resp.WriteError(conn, err.Error())
				continue
			}
			entryID, err := store.XAdd(key, id, fields)
			if err != nil {
				resp.WriteError(conn, err.Error())
				continue
			}
			resp.WriteBulkString(conn, entryID)

		case "XRANGE":
			// The XRANGE command is used to retrieve a range of entries from a stream.
			// XRANGE mystream start end
			if len(args) < 2 {
				resp.WriteError(conn, "XRANGE command requires at least 2 arguments")
				continue
			}
			var key, start, end string

			if len(args) >= 2 {
				key = args[1]
				key = strings.TrimSpace(key)
			}
			if len(args) >= 3 {
				start = args[2]
				start = strings.TrimSpace(start)
			} else {
				start = ""
			}
			if len(args) >= 4 {
				end = args[3]
				end = strings.TrimSpace(end)
			} else {
				end = ""
			}

			entries, err := store.XRange(key, start, end)
			logger.Printf("XRANGE result for key: %s, start: %s, end: %s is %v with error: %v", key, start, end, entries, err)
			if err == nil {
				resp.WriteStreamEntries(conn, entries)
			} else {
				logger.Printf("Error retrieving stream entries: %v", err)
				resp.WriteNullArray(conn)
			}

		case "XREAD":
			// The XREAD command is used to read data from one or more streams, blocking until data is available.
			// XREAD BLOCK timeout STREAMS key [key ...] id [id ...]
			if len(args) < 4 {
				resp.WriteError(conn, "XREAD command requires at least 4 arguments")
				continue
			}
			xreadArgs, err := parseXReadArgs(args[1:])
			if err != nil {
				resp.WriteError(conn, err.Error())
			}

			if !xreadArgs.options.block {
				// if not blocking
				entries, err := store.XRead(xreadArgs.keys, xreadArgs.ids)
				logger.Printf("XREAD result for keys: %v, ids: %v is %v with error: %v", xreadArgs.keys, xreadArgs.ids, entries, err)
				if err == nil {
					logger.Printf("Error reading stream entries: %v", err)
					resp.WriteStreamResults(conn, xreadArgs.keys, entries)
				} else {
					resp.WriteArray(conn, []string{})
				}

			} else {
				entries, err := store.XReadBlocking(xreadArgs.options.timeout, xreadArgs.keys, xreadArgs.ids)
				if err != nil {
					resp.WriteArray(conn, []string{})
					logger.Printf("Error reading stream entries: %v", err)
				} else {
					resp.WriteStreamResults(conn, xreadArgs.keys, entries)
				}

			}
		default:
			resp.WriteError(conn, "Unknown Command: "+args[0])
		}

	}

}
