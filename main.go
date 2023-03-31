//go:generate sh ./generate.sh
package main

import (
	"errors"
	"fmt"
	"github.com/exerosis/PineappleGo/pineapple"
	"net"
	"strings"
)

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

func run() error {
	interfaces, reason := net.Interfaces()
	if reason != nil {
		return reason
	}
	var network net.Interface
	var device net.Addr
	for _, i := range interfaces {
		addresses, reason := i.Addrs()
		if reason != nil {
			return reason
		}
		for _, d := range addresses {
			if strings.Contains(d.String(), "192.168.1.") {
				device = d
				network = i
			}
		}
	}
	if device == nil {
		return errors.New("couldn't find interface")
	}

	fmt.Printf("Interface: %s\n", network.Name)
	fmt.Printf("Address: %s\n", device)

	var address = strings.Split(device.String(), "/")[0]
	var addresses = []string{
		"192.168.1.1:2000",
		"192.168.1.2:2000",
		"192.168.1.3:2000",
	}

	var storage = pineapple.NewStorage()
	var local = fmt.Sprintf("%s:%d", address, 2000)
	var node = pineapple.NewNode[Temp](storage, local, addresses)
	go func() {
		reason := node.Run()
		if reason != nil {
			panic(reason)
		}
	}()

	reason = node.Connect()
	if reason != nil {
		return reason
	}
	println("Connected")

	reason = node.Write([]byte("hello"), []byte("world"))
	if reason != nil {
		println("error writing")
		return reason
	}
	value, reason := node.Read([]byte("hello"))
	if reason != nil {
		return reason
	}
	println("Got: ", string(value))
	return nil
}

func main() {
	reason := run()
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
}
