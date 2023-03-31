package pineapple

import (
	"context"
	"fmt"
	. "github.com/exerosis/PineappleGo/rpc"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Modification interface {
	Modify(value []byte) []byte

	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type Node[Type Modification] interface {
	Read(key []byte) ([]byte, error)
	Write(key []byte, value []byte) error
	ReadModifyWrite(key []byte, modification Type) error
	Run() error
	Connect() error
}

type server struct {
	UnimplementedNodeServer
	Storage
	log       []*ModifyRequest
	committed uint64
	highest   int64
	rmw       func([]byte, []byte) error
}
type node[Type Modification] struct {
	address string
	others  []string
	leader  bool
	server  *server
	clients []NodeClient

	slot     uint64
	majority uint16
}

func NewNode[Type Modification](storage Storage, address string, addresses []string) Node[Type] {
	var others []string
	for _, other := range addresses {
		if other != address {
			others = append(others, other)
		}
	}
	var node = &node[Type]{
		address,
		others,
		addresses[0] == address,
		nil,
		make([]NodeClient, len(others)),
		uint64(0),
		uint16(2345),
	}
	node.server = &server{
		Storage: storage,
		log:     make([]*ModifyRequest, 65536),
		highest: -1,
		rmw: func(key []byte, request []byte) error {
			var modification Type
			var reason = modification.Unmarshal(request)
			if reason != nil {
				return reason
			}
			return node.ReadModifyWrite(key, modification)
		},
	}
	return node
}

func query[Type Modification, Result any](
	node *node[Type],
	parent context.Context,
	operation func(client NodeClient, cancellable context.Context) (Result, error),
) ([]Result, error) {
	var group sync.WaitGroup
	var lock = sync.Mutex{}
	group.Add(len(node.clients))
	var reasons error
	cancellable, cancel := context.WithCancel(parent)
	var responses []Result
	for i, client := range node.clients {
		go func(i int, client NodeClient) {
			var response, reason = operation(client, cancellable)
			lock.Lock()
			if reason == nil {
				responses = append(responses, response)
				if len(responses) >= int(node.majority) {
					cancel()
				}
			} else {
				reasons = multierr.Append(reasons, reason)
				cancel()
			}
			lock.Unlock()
			group.Done()
		}(i, client)
	}
	group.Wait()
	cancel()
	if reasons != nil {
		return nil, reasons
	}
	return responses, nil
}

func (node *node[Type]) Read(key []byte) ([]byte, error) {
	var request = &ReadRequest{Key: key}
	responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*ReadResponse, error) {
		return client.Read(ctx, request)
	})
	if reason != nil {
		return nil, reason
	}
	localRevision, localValue := node.server.Storage.Get(key)
	responses = append(responses, &ReadResponse{
		Revision: localRevision,
		Value:    localValue,
	})
	var max = uint64(0)
	var value = 0
	for i, response := range responses {
		if response.Revision > max {
			max = response.Revision
			value = i
		}
	}

	var write = &WriteRequest{Key: key, Revision: max, Value: responses[value].Value}
	_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*WriteResponse, error) {
		return client.Write(ctx, write)
	})
	if reason != nil {
		return nil, reason
	}
	return write.Value, nil
}
func (node *node[Type]) Write(key []byte, value []byte) error {
	var request = &PeekRequest{Key: key}
	responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*PeekResponse, error) {
		return client.Peek(ctx, request)
	})
	if reason != nil {
		return reason
	}

	responses = append(responses, &PeekResponse{
		Revision: node.server.Storage.Peek(key),
	})
	var max = uint64(0)
	for _, response := range responses {
		if response.Revision > max {
			max = response.Revision
		}
	}

	var write = &WriteRequest{Key: key, Revision: max, Value: value}
	_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*WriteResponse, error) {
		return client.Write(ctx, write)
	})
	if reason != nil {
		return reason
	}
	return nil
}

func (node *node[Type]) ReadModifyWrite(key []byte, modification Type) error {
	if node.leader {
		var readRequest = &ReadRequest{Key: key}
		responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*ReadResponse, error) {
			return client.Read(ctx, readRequest)
		})
		if reason != nil {
			return reason
		}
		localRevision, localValue := node.server.Storage.Get(key)
		responses = append(responses, &ReadResponse{
			Revision: localRevision,
			Value:    localValue,
		})
		var max = uint64(0)
		var value = 0
		for i, response := range responses {
			if response.Revision > max {
				max = response.Revision
				value = i
			}
		}
		var next = modification.Modify(responses[value].Value)
		var request = &ModifyRequest{Key: key, Revision: max, Value: next, Slot: atomic.AddUint64(&node.slot, 1)}
		_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*ModifyResponse, error) {
			return client.Modify(ctx, request)
		})
		return reason
	} else {
		serialized, reason := modification.Marshal()
		if reason != nil {
			return reason
		}
		_, reason = node.server.Propose(context.Background(), &ProposeRequest{Key: key, Request: serialized})
		return reason
	}
}

func (node *node[Type]) Run() error {
	var group sync.WaitGroup
	go func() {
		for {
			time.Sleep(time.Millisecond)
			var highest = atomic.LoadInt64(&node.server.highest)
			for i := atomic.LoadUint64(&node.server.committed); int64(i) <= highest; i++ {
				var slot = i % uint64(len(node.server.log))
				var proposal = node.server.log[slot]
				if proposal == nil {
					highest = int64(i)
					//if we hit the first unfilled slot stop
					break
				}
				var revision, _ = node.server.Storage.Get(proposal.Key)
				if revision < proposal.Revision {
					node.server.Storage.Set(proposal.Key, proposal.Revision, proposal.Value)
				}
				node.server.log[slot] = nil
			}
			atomic.StoreUint64(&node.server.committed, uint64(highest+1))
		}
	}()
	listener, reason := net.Listen("tcp", node.address)
	if reason != nil {
		return fmt.Errorf("failed to listen: %v", reason)
	}
	server := grpc.NewServer()
	RegisterNodeServer(server, node.server)
	if reason := server.Serve(listener); reason != nil {
		return fmt.Errorf("failed to serve: %v", reason)
	}

	group.Wait()
	return nil
}
func (node *node[Type]) Connect() error {
	//Connect to other nodes
	var options = grpc.WithTransportCredentials(insecure.NewCredentials())
	for i, other := range node.others {
		for {
			connection, reason := grpc.Dial(other, options)
			if reason == nil {
				node.clients[i] = NewNodeClient(connection)
				break
			} else {
				println("Timed out!")
			}
		}
	}
	for i, client := range node.clients {
		fmt.Printf("%d: %d", i, client)
	}
	return nil
}

func (server *server) Read(_ context.Context, request *ReadRequest) (*ReadResponse, error) {
	revision, value := server.Get(request.Key)
	return &ReadResponse{Revision: revision, Value: value}, nil
}
func (server *server) Peek(_ context.Context, request *PeekRequest) (*PeekResponse, error) {
	return &PeekResponse{Revision: server.Storage.Peek(request.Key)}, nil
}
func (server *server) Write(_ context.Context, request *WriteRequest) (*WriteResponse, error) {
	server.Set(request.Key, request.Revision, request.Value)
	return &WriteResponse{}, nil
}
func (server *server) Modify(_ context.Context, request *ModifyRequest) (*ModifyResponse, error) {
	//have to figure out where to mark slots as empty safely.
	var committed = atomic.LoadUint64(&server.committed)
	//have to wait here until the next slot has been consumed
	if request.Slot-committed >= uint64(len(server.log)) {
		for request.Slot-committed >= uint64(len(server.log)) {
			committed = atomic.LoadUint64(&server.committed)
		}
		println("Thank you! I was turbo wrapping :(")
	}
	server.log[request.Slot%uint64(len(server.log))] = request
	var value = atomic.LoadInt64(&server.highest)
	for value < int64(request.Slot) && !atomic.CompareAndSwapInt64(&server.highest, value, int64(request.Slot)) {
		value = atomic.LoadInt64(&server.highest)
	}
	return &ModifyResponse{}, nil
}
func (server *server) Propose(_ context.Context, request *ProposeRequest) (*ProposeResponse, error) {
	var reason = server.rmw(request.Key, request.Request)
	if reason != nil {
		return nil, reason
	}
	return &ProposeResponse{}, nil
}
