package kv

type Store struct {
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (p *Store) Put(key string, value string) {
	p.data[key] = value
}

func (p *Store) Get(key string) (string, bool) {
	name, ok := p.data[key]
	return name, ok
}

func (p *Store) Delete(key string) {
	delete(p.data, key)
}
