package pineapple

import (
	"bytes"
	"context"
	"fmt"
	. "github.com/exerosis/PineappleGo/rpc"
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
	identifier uint8
	rmw        func([]byte, []byte) error
}
type client struct {
	server *server
}

func (c *client) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadResponse, error) {
	return c.server.Read(ctx, in)
}
func (c *client) Peek(ctx context.Context, in *PeekRequest, opts ...grpc.CallOption) (*PeekResponse, error) {
	return c.server.Peek(ctx, in)
}
func (c *client) Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error) {
	return c.server.Write(ctx, in)
}
func (c *client) Modify(ctx context.Context, in *ModifyRequest, opts ...grpc.CallOption) (*ModifyResponse, error) {
	return c.server.Modify(ctx, in)
}

type node[Type Modification] struct {
	address   string
	others    []string
	leader    *sync.Mutex
	server    *server
	clients   []NodeClient
	client    *client
	majority  uint16
	index     int32
	connected atomic.Bool
}

func NewNode[Type Modification](storage Storage, address string, addresses []string, factory func() Type) Node[Type] {
	var others []string
	var identifier = 0
	for i, other := range addresses {
		if other != address {
			others = append(others, other)
		} else {
			identifier = i + 1
		}
	}
	var leader *sync.Mutex = nil
	if addresses[0] == address {
		leader = &sync.Mutex{}
	}
	var node = &node[Type]{
		address,
		others,
		leader,
		nil,
		make([]NodeClient, len(others)+1),
		nil,
		uint16((len(addresses) / 2) + 1),
		0,
		atomic.Bool{},
	}
	node.server = &server{
		Storage:    storage,
		identifier: uint8(identifier),
		rmw: func(key []byte, request []byte) error {
			for !node.connected.Load() {
				time.Sleep(time.Millisecond)
			}
			var modification = factory()
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
	var responses = make([]*Result, len(node.clients))
	group.Add(int(node.majority))
	var count = uint32(0)
	for i := 0; i < len(node.clients); i++ {
		go func(i int, client NodeClient) {
			var response, reason = operation(client, parent)
			if reason != nil {
				panic(reason)
			}
			responses[i] = &response
			if atomic.AddUint32(&count, 1) <= uint32(node.majority) {
				group.Done()
			}
		}(i, node.clients[i])
	}
	group.Wait()
	var filtered []Result
	//do we need to limit this to majorty.
	for _, response := range responses {
		if response != nil {
			filtered = append(filtered, *response)
		}
	}
	return filtered, nil
}

func max[Type any, Extracted any](
	values []Type,
	compare func(Extracted, Extracted) bool,
	extract func(Type) Extracted,
) Type {
	var seed = extract(values[0])
	var max = 0
	for i, value := range values[1:] {
		var extracted = extract(value)
		if compare(seed, extracted) {
			seed = extracted
			max = i
		}
	}
	return values[max]
}

func GreaterTag(first Tag, second Tag) bool {
	if GetRevision(second) > GetRevision(first) {
		return true
	}
	return GetIdentifier(second) > GetIdentifier(first)
}

func (node *node[Type]) Read(key []byte) ([]byte, error) {
	var request = &ReadRequest{Key: key}
	responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*ReadResponse, error) {
		return client.Read(ctx, request)
	})
	if reason != nil {
		return nil, reason
	}
	//FIXME swap to one iteration
	var first = responses[0]
	for i := 1; i < len(responses); i++ {
		if responses[i].Tag != first.Tag {
			var max = max(responses, GreaterTag, (*ReadResponse).GetTag)
			var write = &WriteRequest{Key: key, Tag: max.Tag, Value: max.Value}
			_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*WriteResponse, error) {
				return client.Write(ctx, write)
			})
			if reason != nil {
				return nil, reason
			}
			return write.Value, nil
		}
	}
	return first.Value, nil
}
func (node *node[Type]) Write(key []byte, value []byte) error {
	var request = &PeekRequest{Key: key}
	//println("\nWrite start")
	responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*PeekResponse, error) {
		return client.Peek(ctx, request)
	})
	//println("First RTT")
	//_, _ = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*PeekResponse, error) {
	//	return client.Peek(ctx, request)
	//})
	if reason != nil {
		return reason
	}

	var max = max(responses, GreaterTag, (*PeekResponse).GetTag)
	var tag = NewTag(GetRevision(max.Tag)+1, node.server.identifier)
	//println("Got tag")
	var write = &WriteRequest{Key: key, Tag: tag, Value: value}
	_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*WriteResponse, error) {
		return client.Write(ctx, write)
	})
	//println("Second RTT")
	if reason != nil {
		return reason
	}

	//println("Write Completed")
	return nil
}

func (node *node[Type]) ReadModifyWrite(key []byte, modification Type) error {
	if node.leader != nil {
		var readRequest = &ReadRequest{Key: key}
		responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*ReadResponse, error) {
			return client.Read(ctx, readRequest)
		})
		if reason != nil {
			//node.leader.Unlock()
			return reason
		}
		node.leader.Lock()
		t2, v2 := node.server.Storage.Get(key)
		responses = append(responses, &ReadResponse{Tag: t2, Value: v2})
		var max = max(responses, GreaterTag, (*ReadResponse).GetTag)

		var next = modification.Modify(max.Value)
		//hyper-speed path
		if bytes.Equal(max.Value, next) {
			node.leader.Unlock()
			return nil
		}
		var tag = NewTag(GetRevision(max.Tag)+1, node.server.identifier)
		var request = &WriteRequest{Key: key, Tag: tag, Value: next}
		//FIXME we can look at this when wer get here.
		node.server.Storage.Set(key, tag, next)
		//we can let the next RMW get handled once we have done a storage set.
		node.leader.Unlock()
		//Method cannot return until we get the response here.
		_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*WriteResponse, error) {
			return client.Write(ctx, request)
		})
		return reason
	} else {
		serialized, reason := modification.Marshal()
		if reason != nil {
			return reason
		}
		_, reason = node.clients[0].Modify(context.Background(), &ModifyRequest{Key: key, Request: serialized})
		return reason
	}
}

func (node *node[Type]) Run() error {
	var group sync.WaitGroup
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
	var options = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	//grpc.WaitForReady(true)
	for i, other := range node.others {
		connection, reason := grpc.Dial(other, options...)
		if reason != nil {
			return reason
		}
		node.clients[i] = NewNodeClient(connection)
	}
	node.clients[len(node.others)] = &client{node.server}
	node.connected.Store(true)
	return nil
}

func (server *server) Read(_ context.Context, request *ReadRequest) (*ReadResponse, error) {
	tag, value := server.Get(request.Key)
	if tag == NONE {
		tag = NewTag(0, server.identifier)
	}
	return &ReadResponse{Tag: tag, Value: value}, nil
}
func (server *server) Peek(_ context.Context, request *PeekRequest) (*PeekResponse, error) {
	var tag = server.Storage.Peek(request.Key)
	if tag == NONE {
		tag = NewTag(0, server.identifier)
	}
	return &PeekResponse{Tag: tag}, nil
}
func (server *server) Write(_ context.Context, request *WriteRequest) (*WriteResponse, error) {
	if request.Tag == NONE {
		panic("Writing data without a tag.")
	}
	var current = server.Storage.Peek(request.Key)
	if GreaterTag(current, request.Tag) {
		server.Set(request.Key, request.Tag, request.Value)
	}
	return &WriteResponse{}, nil
}
func (server *server) Modify(_ context.Context, request *ModifyRequest) (*ModifyResponse, error) {
	var reason = server.rmw(request.Key, request.Request)
	if reason != nil {
		return nil, reason
	}
	return &ModifyResponse{}, nil
}
