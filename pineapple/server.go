package pineapple

import (
	"bytes"
	"context"
	"fmt"
	. "github.com/exerosis/PineappleGo/rpc"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
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
	rmw func([]byte, []byte) error
}
type node[Type Modification] struct {
	address    string
	others     []string
	leader     *sync.Mutex
	server     *server
	clients    []NodeClient
	identifier uint8
	majority   uint16
}

func NewNode[Type Modification](storage Storage, address string, addresses []string) Node[Type] {
	var others []string
	var identifier = 0
	for i, other := range addresses {
		if other != address {
			others = append(others, other)
		} else {
			identifier = i
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
		make([]NodeClient, len(others)),
		uint8(identifier),
		uint16((len(addresses) / 2) + 1),
	}
	node.server = &server{
		Storage: storage,
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
	localRevision, localValue := node.server.Storage.Get(key)
	responses = append(responses, &ReadResponse{
		Tag:   NewTag(localRevision, node.identifier),
		Value: localValue,
	})
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
func (node *node[Type]) Write(key []byte, value []byte) error {
	var request = &PeekRequest{Key: key}
	responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*PeekResponse, error) {
		return client.Peek(ctx, request)
	})
	if reason != nil {
		return reason
	}

	responses = append(responses, &PeekResponse{
		Tag: node.server.Storage.Peek(key),
	})
	var max = max(responses, GreaterTag, (*PeekResponse).GetTag)
	var tag = NewTag(GetRevision(max.Tag)+1, node.identifier)
	var write = &WriteRequest{Key: key, Tag: tag, Value: value}
	_, reason = query(node, context.Background(), func(client NodeClient, ctx context.Context) (*WriteResponse, error) {
		return client.Write(ctx, write)
	})
	if reason != nil {
		return reason
	}
	return nil
}

func (node *node[Type]) ReadModifyWrite(key []byte, modification Type) error {
	if node.leader != nil {
		node.leader.Lock()
		var readRequest = &ReadRequest{Key: key}
		responses, reason := query(node, context.Background(), func(client NodeClient, ctx context.Context) (*ReadResponse, error) {
			return client.Read(ctx, readRequest)
		})
		if reason != nil {
			node.leader.Unlock()
			return reason
		}
		//eventually optimize this to try the lock and keep local atomic count.
		//that's effectively "pipelining" for abd
		localRevision, localValue := node.server.Storage.Get(key)
		responses = append(responses, &ReadResponse{
			Tag:   NewTag(localRevision, node.identifier),
			Value: localValue,
		})
		var max = max(responses, GreaterTag, (*ReadResponse).GetTag)
		var next = modification.Modify(max.Value)
		if !bytes.Equal(max.Value, next) {
			println("DIDN'T CHANGE")
		}
		var tag = NewTag(GetRevision(max.Tag)+1, node.identifier)
		var request = &WriteRequest{Key: key, Tag: tag, Value: next}
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
	return nil
}

func (server *server) Read(_ context.Context, request *ReadRequest) (*ReadResponse, error) {
	tag, value := server.Get(request.Key)
	return &ReadResponse{Tag: tag, Value: value}, nil
}
func (server *server) Peek(_ context.Context, request *PeekRequest) (*PeekResponse, error) {
	return &PeekResponse{Tag: server.Storage.Peek(request.Key)}, nil
}
func (server *server) Write(_ context.Context, request *WriteRequest) (*WriteResponse, error) {
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
