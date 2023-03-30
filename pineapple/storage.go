package pineapple

type Storage interface {
	Get(key []byte) (rev uint64, value []byte)
	Peek(key []byte) uint64
	Set(key []byte, rev uint64, value []byte)
}

type tag struct {
	value    []byte
	revision uint64
}

type storage struct {
	backing map[string]tag
}

func (s *storage) Get(key []byte) (rev uint64, value []byte) {
	var tag = s.backing[string(key)]
	return tag.revision, tag.value
}

func (s *storage) Peek(key []byte) uint64 {
	return s.backing[string(key)].revision
}

func (s *storage) Set(key []byte, rev uint64, value []byte) {
	s.backing[string(key)] = tag{value, rev}
}

func NewStorage() Storage {
	return &storage{make(map[string]tag)}
}
