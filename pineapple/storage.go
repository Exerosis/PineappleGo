package pineapple

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
}

func (s *storage) Get(key []byte) (Tag, []byte) {
	var tag = s.backing[string(key)]
	return tag.tag, tag.value
}

func (s *storage) Peek(key []byte) Tag {
	return s.backing[string(key)].tag
}

func (s *storage) Set(key []byte, tag Tag, value []byte) {
	s.backing[string(key)] = entry{value, tag}
}

func NewStorage() Storage {
	return &storage{make(map[string]entry)}
}

type Tag = uint64

func GetRevision(tag Tag) uint64 {
	// Mask the first 16 bits of the uint64 to get the tag number
	return tag >> 48
}
func GetIdentifier(tag Tag) uint8 {
	// Mask the last 8 bits of the uint64 to get the identifier
	return uint8(tag)
}
func NewTag(revision uint64, identifier uint8) Tag {
	// Shift the tag number left by 48 bits and add the identifier
	return (revision << 48) | uint64(identifier)
}
