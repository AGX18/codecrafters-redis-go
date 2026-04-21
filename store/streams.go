package store

import "fmt"

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
	if t := s.KeyType(key); t != None && t != Stream_DT {
		return "", nil
	}
	// Validate the ID
	// Auto-generate ID parts if *
	// Check ID is greater than last entry's ID

	// check if stream exists, if not create it
	stream, exists := s.streams[key]
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
