package main

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/resp"
	Store "github.com/codecrafters-io/redis-starter-go/store"
)

type Command struct {
	Name string   // e.g. "SET"
	Args []string // e.g. ["SET", "key1", "value1"]
}

type Client struct {
	inTransaction bool
	queue         []Command
	conn          net.Conn
	responses     [][]byte
}

func Set(args []string, conn net.Conn, store *Store.Store) {
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
			return
		} else if len(args) == 3 { // No expiry
			store.Set(args[1], args[2])
			resp.WriteSimpleString(conn, "OK")
			return
		} else { // Invalid number of arguments
			resp.WriteError(conn, "Invalid number of arguments for SET command")
			return
		}

	}
}

func Get(args []string, conn net.Conn, store *Store.Store) {
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
}

func HandleConnection(client *Client, store *Store.Store) {
	defer client.conn.Close()
	reader := bufio.NewReader(client.conn)
	for {
		args, err := resp.ParseRESP(reader)
		logger.Printf("Parsed arguments: %v", args)
		if err != nil {
			resp.WriteError(client.conn, err.Error())
			return
		}
		if len(args) == 0 {
			resp.WriteError(client.conn, "No Command Provided")
			return
		}

		executeCommand(client, client.conn, Command{Args: args[:], Name: args[0]}, store)

	}

}

func executeCommand(client *Client, conn net.Conn, cmd Command, store *Store.Store) {
	args := cmd.Args
	if client.inTransaction && strings.ToLower(cmd.Name) != "exec" {
		client.queue = append(client.queue, Command{Args: args[:], Name: args[0]})
		logger.Printf("%v", client.queue) // print the queue of the client
		resp.WriteSimpleString(conn, "QUEUED")
		return
	}
	switch strings.ToUpper(cmd.Name) {
	case "PING":
		resp.WriteSimpleString(conn, "PONG")
	case "ECHO":
		if len(args) > 1 {
			resp.WriteBulkString(conn, strings.TrimSpace(strings.Join(args[1:], " ")))
		} else {
			resp.WriteError(conn, "ECHO command requires an argument")
		}
	case "SET":
		Set(args, conn, store)
	case "GET":
		Get(args, conn, store)
	case "RPUSH":
		// RPUSH mylist a b c
		RPUSH(args, conn, store)
	case "LPUSH":
		// LPUSH mylist a b c
		LPUSH(args, conn, store)
	case "LRANGE":
		// The LRANGE command is used to retrieve elements from a list using a start index and a stop index.
		// LRANGE mylist start stop
		LRANGE(args, conn, store)

	case "LLEN":
		// The LLEN command is used to get the length of a list.
		// LLEN mylist
		LLEN(args, conn, store)

	case "LPOP":
		// The LPOP command is used to remove and return the first element of a list.
		// LPOP mylist [count]
		LPOP(args, conn, store)

	case "BLPOP":
		// The BLPOP command is used to remove and return the first element of a list, or block until one is available.
		// BLPOP mylist timeout
		BLPOP(args, conn, store)

	case "TYPE":
		// The TYPE command is used to determine the type of the value stored at a key.
		// TYPE mykey
		TYPE(args, conn, store)

	case "XADD":
		// The XADD command is used to append a new entry to a stream.
		// XADD mystream * field1 value1 field2 value2
		XADD(args, conn, store)

	case "XRANGE":
		// The XRANGE command is used to retrieve a range of entries from a stream.
		// XRANGE mystream start end
		XRANGE(args, conn, store)

	case "XREAD":
		// The XREAD command is used to read data from one or more streams, blocking until data is available.
		// XREAD BLOCK timeout STREAMS key [key ...] id [id ...]
		XREAD(args, conn, store)

	case "INCR":
		INCR(args, conn, store)

	case "MULTI":
		client.inTransaction = true
		resp.WriteSimpleString(conn, "OK")
	case "EXEC":
		client.inTransaction = false
		logger.Println("executing all commands")
		for _, cmd := range client.queue {
			buf := &bufConn{Conn: client.conn}
			executeCommand(client, buf, cmd, store)
			client.responses = append(client.responses, buf.Bytes())

		}
		client.queue = []Command{} // clear the queue
		resp.WriteRawArray(client.conn, client.responses)

	default:
		resp.WriteError(client.conn, "Unknown Command: "+args[0])
	}
}
