package main

import (
	"sync"
	"time"
)

type Entry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

func (entry *Entry) IsExpired() bool {
	if !entry.hasExpiry {
		return false
	}
	return time.Now().After(entry.expiresAt)
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.data == nil {
		return "", false
	}
	entry, exists := s.data[key]
	if !exists {
		return "", false
	}
	// lazy cleanup of expired entries on access
	if entry.IsExpired() {
		delete(s.data, key) // Clean up expired entry
		return "", false
	}
	return entry.value, exists
}

func (s *Store) SetWithExpiry(key, value string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]Entry)
	}
	s.data[key] = Entry{
		value:     value,
		expiresAt: time.Now().Add(duration),
		hasExpiry: true,
	}
}

func (s *Store) Set(key, value string, expiry ...time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]Entry)
	}
	s.data[key] = Entry{
		value:     value,
		expiresAt: time.Time{}, // No expiry
		hasExpiry: false,
	}
}
