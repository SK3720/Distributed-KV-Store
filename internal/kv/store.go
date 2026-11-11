package kv

import "sync"

type Store struct {
	// RWMutex lets many goroutines READ at the same time, but only one goroutine WRITE at a time.
	// The apply loop writes to the store while RPC handlers read from it, so without this lock
	// two goroutines could touch the map at once, which crashes Go programs ("concurrent map writes").
	mu   sync.RWMutex
	data map[string]string
}

// The constructor for each new store instance. It ALLOCATES memory for the map, and returns a POINTER to the new store instance.
// This is important because we want to modify the same store instance across different functions.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (p *Store) Put(key string, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// defer runs Unlock when the function returns, so we can't forget to release the lock.

	p.data[key] = value
}

// Go also returns a second value (the boolean) which indicates if the key was found in the map.
func (p *Store) Get(key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// RLock is a "read lock": other readers can hold it at the same time, writers have to wait.

	name, ok := p.data[key]
	return name, ok
}

func (p *Store) Delete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.data, key)
}
