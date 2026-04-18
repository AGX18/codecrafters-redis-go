package main

import (
	"container/list"
	"net"
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
	mu    sync.RWMutex
	data  map[string]Entry
	lists map[string]*list.List
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

func (s *Store) RPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.lists[key]; !ok {
		s.lists[key] = list.New()
	}

	for _, v := range values {
		s.lists[key].PushBack(v)
	}

	return s.lists[key].Len()
}

func (s *Store) LRange(key string, start, stop int, conn net.Conn) ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, exists := s.lists[key]
	if !exists {
		return nil, false
	}

	if start < 0 {
		start += list.Len()
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += list.Len()
		if stop < 0 {
			return []string{}, true
		}
	}

	if start > stop {
		writeArray(conn, []string{})
		return []string{}, true
	}

	if stop >= list.Len() {
		stop = list.Len() - 1
	}

	result := []string{}
	i := 0
	for e := list.Front(); e != nil; e = e.Next() {
		if i > stop {
			break
		}
		if i >= start {
			result = append(result, e.Value.(string))
		}
		i++
	}

	return result, true
}

func (s *Store) LPUSH(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.lists[key]; !ok {
		s.lists[key] = list.New()
	}

	for _, v := range values {
		s.lists[key].PushFront(v)
	}

	return s.lists[key].Len()
}
