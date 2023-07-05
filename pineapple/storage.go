package pineapple

import (
	"sync"
)

type Storage interface {
	Get(key []byte) (tag Tag, value []byte)
	Peek(key []byte) Tag
	Set(key []byte, tag Tag, value []byte)
}

type entry struct {
	value []byte
	tag   Tag
}

type storage struct {
	backing map[string]entry
	lock    sync.RWMutex
}

func (s *storage) Get(key []byte) (Tag, []byte) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var tag = s.backing[string(key)]
	return tag.tag, tag.value
}

func (s *storage) Peek(key []byte) Tag {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.backing[string(key)].tag
}

func (s *storage) Set(key []byte, tag Tag, value []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.backing[string(key)] = entry{value, tag}
}

func NewStorage() Storage {
	return &storage{backing: make(map[string]entry)}
}

type Tag = uint64

const NONE = 0

func GetRevision(tag Tag) uint64 {
	return tag >> 8
}
func GetIdentifier(tag Tag) uint8 {
	return uint8(tag)
}
func NewTag(revision uint64, identifier uint8) Tag {
	if identifier == 0 {
		panic("Invalid tag, identifier too small!")
	}
	return (revision << 8) | uint64(identifier)
}
