package main

type Temp struct {
}

func (t Temp) Modify([]byte) []byte {
	panic("implement me")
}
func (t Temp) Marshal() ([]byte, error) {
	panic("implement me")
}
func (t Temp) Unmarshal([]byte) error {
	panic("implement me")
}
