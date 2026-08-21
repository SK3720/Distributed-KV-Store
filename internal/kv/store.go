package kv

type Store struct {
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
	p.data[key] = value
}

// Go also returns a second value (the boolean) which indicates if the key was found in the map.
func (p *Store) Get(key string) (string, bool) {
	name, ok := p.data[key]
	return name, ok
}

func (p *Store) Delete(key string) {
	delete(p.data, key)
}
