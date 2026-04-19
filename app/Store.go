package main

import (
	"container/list"
	"context"
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
	mu      sync.RWMutex
	data    map[string]Entry
	lists   map[string]*list.List
	waiters map[string][]chan string // key -> waiting clients
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
	logger.Printf("RPUSH called with key: %s, values: %v", key, values)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.lists[key]; !ok {
		s.lists[key] = list.New()
	}
	listLength := s.lists[key].Len()

	for _, v := range values {
		// if there are clients waiting for this key, we should send the value directly to the first waiting client
		// instead of pushing it to the list
		waiters, hasWaiters := s.waiters[key]
		if hasWaiters && len(waiters) > 0 {
			logger.Printf("RPUSH notifying waiter for key: %s with value: %s", key, v)
			// notify the first waiter in the queue
			waiter := waiters[0]
			waiter <- v
			// remove this waiter from the list of waiters for this key
			s.waiters[key] = s.waiters[key][1:]
		} else {
			logger.Printf("RPUSH adding value: %s for key: %s", v, key)
			s.lists[key].PushBack(v)
		}
	}

	return listLength + len(values)
}

func (s *Store) LRange(key string, start, stop int) ([]string, bool) {
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
	listLength := s.lists[key].Len()

	for _, v := range values {
		// if there are clients waiting for this key, we should send the value directly to the first waiting client
		// instead of pushing it to the list
		waiters, hasWaiters := s.waiters[key]
		if hasWaiters && len(waiters) > 0 {
			// notify the first waiter in the queue
			waiter := waiters[0]
			waiter <- v
			// remove this waiter from the list of waiters for this key
			s.waiters[key] = s.waiters[key][1:]
		} else {
			s.lists[key].PushFront(v)
		}
	}

	return listLength + len(values)
}

func (s *Store) LLEN(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, exists := s.lists[key]
	if !exists {
		return 0
	}
	return list.Len()
}

func (s *Store) LPOP(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, exists := s.lists[key]
	if !exists || list.Len() == 0 {
		return "", false
	}

	if list.Len() == 0 {
		return "", false
	}

	front := list.Front()
	value := front.Value.(string)
	list.Remove(front)
	return value, true
}

func (s *Store) LPOPArray(key string, len int) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, exists := s.lists[key]
	if !exists || list.Len() == 0 {
		return []string{}, false
	}

	result := []string{}
	for i := 0; i < len && list.Len() > 0; i++ {
		front := list.Front()
		value := front.Value.(string)
		list.Remove(front)
		result = append(result, value)
	}
	return result, true
}

func (s *Store) BLPOP(key string, timeout int) (string, bool) {
	logger.Printf("BLPOP called with key: %s, timeout: %d", key, timeout)
	s.mu.Lock()
	if _, ok := s.lists[key]; !ok {
		s.lists[key] = list.New()
	}
	if s.lists[key].Len() > 0 {
		s.mu.Unlock() // Unlock before calling LPOP to avoid deadlock
		return s.LPOP(key)
	}

	//  if the list is empty, we need to wait for a value to be pushed
	if _, ok := s.waiters[key]; !ok {
		s.waiters[key] = []chan string{}
	}
	// we create a new channel for this waiting client and add it to the list of waiters for this key
	waiter := make(chan string, 1) // buffered channel to avoid blocking the producer
	s.waiters[key] = append(s.waiters[key], waiter)

	s.mu.Unlock()

	var ctx context.Context
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Duration(timeout)*time.Second))
	if timeout == 0 {
		ctx = context.Background() // wait indefinitely
	}
	defer cancel()

	select {
	case value := <-waiter:
		// got a value from a producer, return it to the client
		logger.Printf("BLPOP returning value: %s for key: %s", value, key)
		return value, true
	case <-ctx.Done():
		// timeout expired, return null
		logger.Printf("BLPOP timeout expired for key: %s", key)
		return "", false
	}
}
