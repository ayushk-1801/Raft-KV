package transport

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/ayushk-1801/raft-kv/internal/raft"
)

type NetworkTransport struct {
	mu            sync.Mutex
	peerAddresses map[int]string
	connections   map[int]*rpc.Client
}

func NewNetworkTransport(peers map[int]string) *NetworkTransport {
	return &NetworkTransport{
		peerAddresses: peers,
		connections:   make(map[int]*rpc.Client),
	}
}

func (nt *NetworkTransport) getClient(peerId int) (*rpc.Client, error) {
	nt.mu.Lock()
	defer nt.mu.Unlock()

	if client, ok := nt.connections[peerId]; ok {
		return client, nil
	}

	address := nt.peerAddresses[peerId]
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return nil, err
	}

	nt.connections[peerId] = client
	return client, nil
}

func (nt *NetworkTransport) SendRequestVote(peerId int, args raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	client, err := nt.getClient(peerId)
	if err != nil {
		return err
	}

	call := client.Go("RPCProxy.RequestVote", args, reply, make(chan *rpc.Call, 1))
	select {
	case <-time.After(100 * time.Millisecond):
		return fmt.Errorf("timeout")
	case resp := <-call.Done:
		if resp.Error != nil {
			nt.mu.Lock()
			delete(nt.connections, peerId)
			nt.mu.Unlock()
		}
		return resp.Error
	}
}

func (nt *NetworkTransport) SendAppendEntries(peerId int, args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	client, err := nt.getClient(peerId)
	if err != nil {
		return err
	}

	call := client.Go("RPCProxy.AppendEntries", args, reply, make(chan *rpc.Call, 1))
	select {
	case <-time.After(100 * time.Millisecond):
		return fmt.Errorf("timeout")
	case resp := <-call.Done:
		if resp.Error != nil {
			nt.mu.Lock()
			delete(nt.connections, peerId)
			nt.mu.Unlock()
		}
		return resp.Error
	}
}

func (nt *NetworkTransport) SendInstallSnapshot(peerId int, args raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	client, err := nt.getClient(peerId)
	if err != nil {
		return err
	}

	call := client.Go("RPCProxy.InstallSnapshot", args, reply, make(chan *rpc.Call, 1))
	select {
	case <-time.After(500 * time.Millisecond):
		return fmt.Errorf("timeout")
	case resp := <-call.Done:
		if resp.Error != nil {
			nt.mu.Lock()
			delete(nt.connections, peerId)
			nt.mu.Unlock()
		}
		return resp.Error
	}
}

type RPCProxy struct {
	cm *raft.ConsensusModule
}

func NewRPCProxy(cm *raft.ConsensusModule) *RPCProxy {
	return &RPCProxy{cm: cm}
}

func (p *RPCProxy) RequestVote(args raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	return p.cm.RequestVote(args, reply)
}

func (p *RPCProxy) AppendEntries(args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	return p.cm.AppendEntries(args, reply)
}

func (p *RPCProxy) InstallSnapshot(args raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	return p.cm.InstallSnapshot(args, reply)
}
