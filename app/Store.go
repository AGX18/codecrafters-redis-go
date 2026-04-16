package main

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.data == nil {
		return "", false
	}
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]string)
	}
	s.data[key] = value
}
