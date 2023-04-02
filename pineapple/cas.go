package pineapple

import (
	"bytes"
	"encoding/binary"
)

type Cas struct {
	expected []byte
	next     []byte
}

func NewCas(expected []byte, next []byte) Cas {
	return Cas{expected, next}
}

func (cas Cas) Modify(value []byte) []byte {
	if bytes.Equal(cas.expected, value) {
		return cas.next
	}
	return value
}
func (cas Cas) Marshal() ([]byte, error) {
	var expected = len(cas.expected)
	var next = len(cas.next)
	var buffer = make([]byte, expected+next+8)
	binary.LittleEndian.PutUint32(buffer[0:], uint32(expected))
	copy(buffer[4:], cas.expected)
	binary.LittleEndian.PutUint32(buffer[4+expected:], uint32(len(cas.next)))
	copy(buffer[8+expected:], cas.next)
	return buffer, nil
}
func (cas Cas) Unmarshal(buffer []byte) error {
	var expected = binary.LittleEndian.Uint32(buffer[0:])
	cas.expected = make([]byte, expected)
	copy(cas.expected, buffer[4:4+expected])
	var next = binary.LittleEndian.Uint32(buffer[4+expected:])
	cas.next = make([]byte, next)
	copy(cas.next, buffer[8+expected:])
	return nil
}
