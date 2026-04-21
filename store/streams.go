package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type StreamEntry struct {
	id     string
	fields map[string]string
}

func (e StreamEntry) ID() string {
	return e.id
}

func (e StreamEntry) Fields() map[string]string {
	return e.fields
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
	s.mu.Lock()
	defer s.mu.Unlock()
	// Validate the ID
	s.logger.Printf("validating ID: %s for stream: %s", ID, key)
	var millis, seq int64
	var err error
	var lastMillis, lastSeq int64
	// get the last entry's ID and compare with the new ID
	Laststream, exists := s.streams[key]
	if exists && len(Laststream.entries) > 0 {
		lastEntryID := Laststream.entries[len(Laststream.entries)-1].id
		lastMillis, lastSeq, err = parseID(lastEntryID)
		if err != nil {
			return "", fmt.Errorf("Invalid last entry ID format: %s", lastEntryID)
		}
	} else {
		lastMillis, lastSeq = 0, 0
	}

	if !strings.Contains(ID, "*") {
		millis, seq, err = parseID(ID)
		if err != nil {
			return "", fmt.Errorf("Invalid ID format: %s", ID)
		}
		if err := ValidateID(millis, seq, lastMillis, lastSeq); err != nil {
			return "", err
		}
	} else {
		// Auto-generate ID parts if *
		if ID == "*" {
			millis = time.Now().UnixMilli()
			if lastMillis == millis {
				seq = lastSeq + 1
			} else {
				seq = 0
			}
		} else {
			parts := strings.Split(ID, "-")
			if len(parts) != 2 {
				return "", fmt.Errorf("Invalid ID format: %s", ID)
			}
			if parts[0] == "*" { // Auto-generate millis part
				millis = time.Now().UnixMilli()
			} else { // else parse it
				millis, err = strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					return "", fmt.Errorf("Invalid millis part in ID: %s", parts[0])
				}
			}
			if parts[1] == "*" { // Auto-generate seq part
				if lastMillis == millis {
					seq = lastSeq + 1
				} else {
					seq = 0
				}
			}
		}
		ID = fmt.Sprintf("%d-%d", millis, seq)
		s.logger.Printf("Auto-generated ID: %s", ID)
	}

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

func ValidateID(millis, seq int64, lastMillis, lastSeq int64) error {
	if millis < 0 || seq < 0 || (millis == 0 && seq == 0) {
		return fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}
	// get the last entry's ID and compare with the new ID
	if millis < lastMillis || (millis == lastMillis && seq <= lastSeq) {
		return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return nil
}

func (s *Store) XRange(key string, startID, endID string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stream, exists := s.streams[key]
	if !exists {
		return []StreamEntry{}, fmt.Errorf("Stream does not exist")
	}

	if startID == "-" {
		// If the start ID is "-", we want to include all entries from the beginning of the stream, so we set startID to the ID of the first entry in the stream
		startID = stream.entries[0].id
	} else if len(strings.Split(startID, "-")) == 1 {
		startID += "-0"
	}

	if endID == "+" {
		// If the end ID is "+", we want to include all entries up to the last one, so we set endID to the ID of the last entry in the stream
		endID = stream.entries[len(stream.entries)-1].id
	} else if len(strings.Split(endID, "-")) == 1 {
		endID += "-0"
	}

	s.logger.Printf("XRANGE called with key: %s, startID: %s, endID: %s", key, startID, endID)

	// Validate the IDs
	if err := XRANGEValidation(startID, endID); err != nil {
		return []StreamEntry{}, fmt.Errorf("Invalid ID range: %s - %s. Error: %s", startID, endID, err.Error())
	}
	result := []StreamEntry{}
	for i, entry := range stream.entries {
		if entry.id >= startID {
			result = append(result, stream.entries[i])
			if stream.entries[i].id == endID {
				return result, nil
			}
		}
	}
	return []StreamEntry{}, fmt.Errorf("Start ID not found")

}

func XRANGEValidation(startID, endID string) error {
	// Validate startID and endID format
	sMilli, sSeq, err := parseID(startID)
	if err != nil {
		return fmt.Errorf("Invalid start ID format: %s", startID)
	}
	eMilli, eSeq, err := parseID(endID)
	if err != nil {
		return fmt.Errorf("Invalid end ID format: %s", endID)
	}

	if sMilli > eMilli || (sMilli == eMilli && sSeq > eSeq) {
		return fmt.Errorf("Start ID must be less than or equal to End ID")
	}
	return nil
}

func (s *Store) XRead(keys string, startID string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := []StreamEntry{}

	stream, exists := s.streams[keys]
	if !exists {
		return nil, fmt.Errorf("Stream does not exist")
	}

	for i, entry := range stream.entries {
		if entry.id > startID {
			result = append(result, stream.entries[i])
		}
	}
	return result, nil
}
