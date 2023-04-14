//go:generate sh ./generate.sh
package main

import (
	"errors"
	"fmt"
	"github.com/exerosis/PineappleGo/pineapple"
	"net"
	"strings"
	"sync"
	"time"
)

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
	var node = pineapple.NewNode[pineapple.Cas](storage, local, addresses)
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

	if strings.Contains(address, "192.168.1.1") {
		var start = time.Now()
		var count = 100_000
		var group sync.WaitGroup
		var pipes = pineapple.NewSemaphore(1)
		group.Add(count)
		for i := 0; i < count; i++ {
			go func() {
				pipes.Acquire()
				defer pipes.Release()
				defer group.Done()
				reason := node.Write([]byte("world"), []byte("universe"))
				if reason != nil {
					panic(reason)
				}
				//var cas = pineapple.NewCas([]byte("world"), []byte("universe"))
				//reason = node.ReadModifyWrite([]byte("hello"), cas)
				//if reason != nil {
				//	panic(reason)
				//}
			}()
		}
		group.Wait()
		var took = float64(count) / time.Since(start).Seconds()
		fmt.Printf("%0.2f ops/s\n", took)
	}

	time.Sleep(48 * time.Hour)
	return nil
}

func main() {
	reason := run()
	if reason != nil {
		fmt.Println("failed: ", reason)
	}
}
