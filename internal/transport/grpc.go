package transport

import (
	"bytes"
	"context"
	"encoding/gob"
	"net"
	"sync"
	"time"

	"github.com/ayushk-1801/raft-kv/internal/raft"
	"github.com/ayushk-1801/raft-kv/internal/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrpcTransport struct {
	raftpb.UnimplementedRaftServer
	mu            sync.Mutex
	peerAddresses map[int]string
	connections   map[int]raftpb.RaftClient
	cm            *raft.ConsensusModule
}

func NewGrpcTransport(peers map[int]string) *GrpcTransport {
	return &GrpcTransport{
		peerAddresses: peers,
		connections:   make(map[int]raftpb.RaftClient),
	}
}

func (gt *GrpcTransport) SetCM(cm *raft.ConsensusModule) {
	gt.cm = cm
}

func (gt *GrpcTransport) getClient(peerId int) (raftpb.RaftClient, error) {
	gt.mu.Lock()
	defer gt.mu.Unlock()

	if client, ok := gt.connections[peerId]; ok {
		return client, nil
	}

	address := gt.peerAddresses[peerId]
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftClient(conn)
	gt.connections[peerId] = client
	return client, nil
}

func (gt *GrpcTransport) SendRequestVote(peerId int, args raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	client, err := gt.getClient(peerId)
	if err != nil {
		return err
	}

	pbArgs := &raftpb.RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateID),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	pbReply, err := client.RequestVote(ctx, pbArgs)
	if err != nil {
		return err
	}

	reply.Term = int(pbReply.Term)
	reply.VoteGranted = pbReply.VoteGranted
	return nil
}

func (gt *GrpcTransport) SendAppendEntries(peerId int, args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	client, err := gt.getClient(peerId)
	if err != nil {
		return err
	}

	pbEntries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, entry := range args.Entries {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(entry.Command); err != nil {
			return err
		}
		pbEntries[i] = &raftpb.LogEntry{
			Term:    int32(entry.Term),
			Command: buf.Bytes(),
		}
	}

	pbArgs := &raftpb.AppendEntriesArgs{
		Term:         int32(args.Term),
		LeaderId:     int32(args.LeaderID),
		PrevLogIndex: int32(args.PrevLogIndex),
		PrevLogTerm:  int32(args.PrevLogTerm),
		Entries:      pbEntries,
		LeaderCommit: int32(args.LeaderCommit),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	pbReply, err := client.AppendEntries(ctx, pbArgs)
	if err != nil {
		return err
	}

	reply.Term = int(pbReply.Term)
	reply.Success = pbReply.Success
	reply.ConflictIndex = int(pbReply.ConflictIndex)
	reply.ConflictTerm = int(pbReply.ConflictTerm)
	return nil
}

func (gt *GrpcTransport) SendInstallSnapshot(peerId int, args raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	client, err := gt.getClient(peerId)
	if err != nil {
		return err
	}

	pbArgs := &raftpb.InstallSnapshotArgs{
		Term:              int32(args.Term),
		LeaderId:          int32(args.LeaderId),
		LastIncludedIndex: int32(args.LastIncludedIndex),
		LastIncludedTerm:  int32(args.LastIncludedTerm),
		Data:              args.Data,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pbReply, err := client.InstallSnapshot(ctx, pbArgs)
	if err != nil {
		return err
	}

	reply.Term = int(pbReply.Term)
	return nil
}

func (gt *GrpcTransport) RequestVote(ctx context.Context, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	raftArgs := raft.RequestVoteArgs{
		Term:         int(args.Term),
		CandidateID:  int(args.CandidateId),
		LastLogIndex: int(args.LastLogIndex),
		LastLogTerm:  int(args.LastLogTerm),
	}
	var raftReply raft.RequestVoteReply
	err := gt.cm.RequestVote(raftArgs, &raftReply)
	if err != nil {
		return nil, err
	}
	return &raftpb.RequestVoteReply{
		Term:        int32(raftReply.Term),
		VoteGranted: raftReply.VoteGranted,
	}, nil
}

func (gt *GrpcTransport) AppendEntries(ctx context.Context, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	raftEntries := make([]raft.LogEntry, len(args.Entries))
	for i, entry := range args.Entries {
		var command interface{}
		dec := gob.NewDecoder(bytes.NewBuffer(entry.Command))
		if err := dec.Decode(&command); err != nil {
			return nil, err
		}
		raftEntries[i] = raft.LogEntry{
			Term:    int(entry.Term),
			Command: command,
		}
	}

	raftArgs := raft.AppendEntriesArgs{
		Term:         int(args.Term),
		LeaderID:     int(args.LeaderId),
		PrevLogIndex: int(args.PrevLogIndex),
		PrevLogTerm:  int(args.PrevLogTerm),
		Entries:      raftEntries,
		LeaderCommit: int(args.LeaderCommit),
	}
	var raftReply raft.AppendEntriesReply
	err := gt.cm.AppendEntries(raftArgs, &raftReply)
	if err != nil {
		return nil, err
	}
	return &raftpb.AppendEntriesReply{
		Term:          int32(raftReply.Term),
		Success:       raftReply.Success,
		ConflictIndex: int32(raftReply.ConflictIndex),
		ConflictTerm:  int32(raftReply.ConflictTerm),
	}, nil
}

func (gt *GrpcTransport) InstallSnapshot(ctx context.Context, args *raftpb.InstallSnapshotArgs) (*raftpb.InstallSnapshotReply, error) {
	raftArgs := raft.InstallSnapshotArgs{
		Term:              int(args.Term),
		LeaderId:          int(args.LeaderId),
		LastIncludedIndex: int(args.LastIncludedIndex),
		LastIncludedTerm:  int(args.LastIncludedTerm),
		Data:              args.Data,
	}
	var raftReply raft.InstallSnapshotReply
	err := gt.cm.InstallSnapshot(raftArgs, &raftReply)
	if err != nil {
		return nil, err
	}
	return &raftpb.InstallSnapshotReply{
		Term: int32(raftReply.Term),
	}, nil
}

func (gt *GrpcTransport) Serve(lis net.Listener) error {
	s := grpc.NewServer()
	raftpb.RegisterRaftServer(s, gt)
	return s.Serve(lis)
}
