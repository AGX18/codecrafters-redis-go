package store

import (
	"errors"
	"fmt"
	"strconv"
)

func (s *Store) INCR(key string) (int, error) {
	s.stringsMu.RLock()
	defer s.stringsMu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		s.data[key] = Entry{value: "1"}
		return 1, nil
	}
	v, err := strconv.Atoi(entry.value)

	if err == nil {
		s.data[key] = Entry{value: fmt.Sprintf("%d", v+1), expiresAt: entry.expiresAt, hasExpiry: entry.hasExpiry}
	} else {
		return 0, errors.New("value is not an integer or out of range")
	}

	return v + 1, nil
}
