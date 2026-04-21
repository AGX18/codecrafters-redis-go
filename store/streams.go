package store

import (
	"fmt"
	"strconv"
	"strings"
)

type StreamEntry struct {
	id     string
	fields map[string]string
}

type Stream struct {
	entries []StreamEntry
}

func GetXAddArgs(args []string) (string, string, map[string]string, error) {
	// XADD mystream * field1 value1 field2 value2
	if len(args) < 5 || len(args)%2 == 0 {
		return "", "", nil, fmt.Errorf("XADD command requires at least 5 arguments and an even number of arguments")
	}

	key := args[1]
	id := args[2]

	fields := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		fields[field] = value
	}

	return key, id, fields, nil
}

func (s *Store) XAdd(key string, ID string, fields map[string]string) (string, error) {
	// check if key exists and is a stream, if not return an error
	if t := s.KeyType(key); t != None && t != StreamDT {
		return "", nil
	}
	// Validate the ID
	millis, seq, err := parseID(ID)
	if millis < 0 || seq < 0 || (millis == 0 && seq == 0) {
		return "", fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	if err != nil {
		return "", err
	}
	// get the last entry's ID and compare with the new ID
	stream, exists := s.streams[key]
	if exists && len(stream.entries) > 0 {
		lastEntryID := stream.entries[len(stream.entries)-1].id
		lastMillis, lastSeq, err := parseID(lastEntryID)
		if err != nil {
			return "", fmt.Errorf("invalid ID format in existing entry: %s", lastEntryID)
		}
		if millis < lastMillis || (millis == lastMillis && seq <= lastSeq) {
			return "", fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}
	// Auto-generate ID parts if *
	// Check ID is greater than last entry's ID

	// check if stream exists, if not create it
	stream, exists = s.streams[key]
	if !exists {
		stream = &Stream{}
		s.streams[key] = stream
	}

	// Append entry to stream
	stream.entries = append(stream.entries, StreamEntry{
		id:     ID,
		fields: fields,
	})

	// Return the ID as a bulk string
	return ID, nil
}

func parseID(ID string) (int64, int64, error) {
	parts := strings.Split(ID, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid ID format: %s", ID)
	}

	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid millis part in ID: %s", parts[0])
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sequence part in ID: %s", parts[1])
	}

	return millis, seq, nil
}
