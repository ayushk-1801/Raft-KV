package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type MockStorage struct {
	mu   sync.Mutex
	data map[string][]byte
}

func NewMockStorage() *MockStorage {
	return &MockStorage{data: make(map[string][]byte)}
}

func (ms *MockStorage) AppendLog(entry []byte) error                  { return nil }
func (ms *MockStorage) Sync() error                                   { return nil }
func (ms *MockStorage) ReadLogRange(start, end int) ([][]byte, error) { return nil, nil }
func (ms *MockStorage) SaveSnapshot(meta, data []byte) error {
	ms.mu.Lock()
	ms.data["raft_state"] = meta
	ms.data["raft_snapshot"] = data
	ms.mu.Unlock()
	return nil
}
func (ms *MockStorage) SaveMeta(meta []byte) error {
	ms.mu.Lock()
	ms.data["raft_state"] = meta
	ms.mu.Unlock()
	return nil
}
func (ms *MockStorage) ClearLog() error { return nil }
func (ms *MockStorage) ReadState() (meta []byte, snap []byte, logs [][]byte, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.data["raft_state"], ms.data["raft_snapshot"], nil, nil
}
func (ms *MockStorage) Set(key string, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.data[key] = value
	return nil
}
func (ms *MockStorage) Get(key string) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.data[key], nil
}
func (ms *MockStorage) HasData() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.data) > 0
}

type MockCluster struct {
	mu          sync.Mutex
	nodes       map[int]*ConsensusModule
	dead        map[int]bool
	brokenLinks map[int]map[int]bool
	ready       chan struct{}
}

func NewMockCluster(n int) *MockCluster {
	mc := &MockCluster{
		nodes:       make(map[int]*ConsensusModule),
		dead:        make(map[int]bool),
		brokenLinks: make(map[int]map[int]bool),
		ready:       make(chan struct{}),
	}

	for i := range n {
		mc.brokenLinks[i] = make(map[int]bool)

		var peerIds []int
		for j := range n {
			if i != j {
				peerIds = append(peerIds, j)
			}
		}
		mc.nodes[i] = NewConsensusModule(i, peerIds, mc, NewMockStorage(), make(chan ApplyMsg, 100))
	}

	close(mc.ready)
	return mc
}

func (mc *MockCluster) waitReady() {
	<-mc.ready
}

func (mc *MockCluster) Shutdown() {
	mc.mu.Lock()
	nodes := make([]*ConsensusModule, 0, len(mc.nodes))
	for _, node := range mc.nodes {
		nodes = append(nodes, node)
	}
	mc.mu.Unlock()
	for _, node := range nodes {
		node.Kill()
	}
}

func (mc *MockCluster) SetLinkStatus(from, to int, broken bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.brokenLinks[from] == nil {
		mc.brokenLinks[from] = make(map[int]bool)
	}
	mc.brokenLinks[from][to] = broken
}

func (mc *MockCluster) CrashNode(id int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.dead[id] = true
}

func (mc *MockCluster) ReviveNode(id int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.dead[id] = false
}

func (mc *MockCluster) SendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error {
	mc.waitReady()

	mc.mu.Lock()
	dead := mc.dead[peerId] || mc.dead[args.CandidateID]
	broken := mc.brokenLinks[args.CandidateID][peerId] || mc.brokenLinks[peerId][args.CandidateID]
	targetNode := mc.nodes[peerId]
	mc.mu.Unlock()

	if dead || broken {
		return fmt.Errorf("network error")
	}
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	return targetNode.RequestVote(args, reply)
}

func (mc *MockCluster) SendAppendEntries(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	mc.waitReady()

	mc.mu.Lock()
	dead := mc.dead[peerId] || mc.dead[args.LeaderID]
	broken := mc.brokenLinks[args.LeaderID][peerId] || mc.brokenLinks[peerId][args.LeaderID]
	targetNode := mc.nodes[peerId]
	mc.mu.Unlock()

	if dead || broken {
		return fmt.Errorf("network error")
	}
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	return targetNode.AppendEntries(args, reply)
}

func (mc *MockCluster) SendInstallSnapshot(peerId int, args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	mc.waitReady()

	mc.mu.Lock()
	dead := mc.dead[peerId] || mc.dead[args.LeaderId]
	broken := mc.brokenLinks[args.LeaderId][peerId] || mc.brokenLinks[peerId][args.LeaderId]
	targetNode := mc.nodes[peerId]
	mc.mu.Unlock()

	if dead || broken {
		return fmt.Errorf("network error")
	}
	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	return targetNode.InstallSnapshot(args, reply)
}

func (mc *MockCluster) CheckSingleLeader() (int, int) {
	mc.waitReady()

	leader, term := -1, -1
	for i := 0; i < len(mc.nodes); i++ {
		mc.mu.Lock()
		if mc.dead[i] {
			mc.mu.Unlock()
			continue
		}
		node := mc.nodes[i]
		mc.mu.Unlock()

		node.mu.Lock()
		if node.state == Leader {
			if leader != -1 {
				node.mu.Unlock()
				return -1, -1
			}
			leader, term = node.id, node.currentTerm
		}
		node.mu.Unlock()
	}
	return leader, term
}

func TestInitialElection(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)
	if l, _ := cluster.CheckSingleLeader(); l == -1 {
		t.Fatalf("Expected leader")
	}
}

func TestLeaderCrashReElection(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)
	l1, t1 := cluster.CheckSingleLeader()
	cluster.nodes[l1].Kill()
	cluster.CrashNode(l1)
	time.Sleep(2 * time.Second)
	l2, t2 := cluster.CheckSingleLeader()
	if l2 == -1 || l2 == l1 || t2 <= t1 {
		t.Fatalf("Re-election failed")
	}
}

func TestPartitionedLeader(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)
	leader, term := cluster.CheckSingleLeader()
	cluster.nodes[leader].PauseElectionTimer(true)
	for _, p := range cluster.nodes[leader].peerIds {
		cluster.SetLinkStatus(leader, p, true)
	}
	idx, _, _ := cluster.nodes[leader].Submit("p-write")
	time.Sleep(1 * time.Second)
	cluster.nodes[leader].mu.Lock()
	if int(cluster.nodes[leader].commitIndex.Load()) >= idx {
		t.Fatalf("Committed without majority")
	}
	cluster.nodes[leader].mu.Unlock()
	cluster.nodes[leader].PauseElectionTimer(false)
	for _, p := range cluster.nodes[leader].peerIds {
		cluster.SetLinkStatus(leader, p, false)
	}
	time.Sleep(2 * time.Second)
	nl, nt := cluster.CheckSingleLeader()
	if nl == -1 || nl == leader || nt <= term {
		t.Fatalf("New election failed")
	}
}
