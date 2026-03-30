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

func (ms *MockStorage) Set(key string, value []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.data[key] = value
	return nil
}

func (ms *MockStorage) Get(key string) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if val, ok := ms.data[key]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("not found")
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
}

func NewMockCluster(n int) *MockCluster {
	mc := &MockCluster{
		nodes:       make(map[int]*ConsensusModule),
		dead:        make(map[int]bool),
		brokenLinks: make(map[int]map[int]bool),
	}

	for i := range n {
		var peerIds []int
		for j := range n {
			if i != j {
				peerIds = append(peerIds, j)
			}
		}
		storage := NewMockStorage()
		applyCh := make(chan ApplyMsg, 100)
		mc.nodes[i] = NewConsensusModule(i, peerIds, mc, storage, applyCh)
		go mc.nodes[i].runElectionTimer()
	}
	return mc
}

func (mc *MockCluster) Shutdown() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, node := range mc.nodes {
		node.Kill()
		node.applyCond.Broadcast()
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

func (mc *MockCluster) ReviveNode(id int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.dead[id] = false
}

func (mc *MockCluster) CrashNode(id int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.dead[id] = true
}

func (mc *MockCluster) SendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error {
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
	leader := -1
	term := -1

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
			leader = node.id
			term = node.currentTerm
		}
		node.mu.Unlock()
	}
	return leader, term
}

func TestInitialElection(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()

	time.Sleep(1 * time.Second)

	leaderId, term := cluster.CheckSingleLeader()
	if leaderId == -1 {
		t.Fatalf("Expected exactly 1 leader")
	}
	fmt.Printf("Elected Node %d for Term %d\n", leaderId, term)
}

func TestHeartbeatsSuppressElections(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()

	time.Sleep(1 * time.Second)
	l1, t1 := cluster.CheckSingleLeader()

	time.Sleep(2 * time.Second)

	l2, t2 := cluster.CheckSingleLeader()
	if l1 != l2 || t1 != t2 {
		t.Fatalf("Leader changed unnecessarily")
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
	fmt.Printf("New Leader: Node %d, Term %d\n", l2, t2)
}

func TestLogDivergenceAndResolution(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)

	leader, _ := cluster.CheckSingleLeader()
	cluster.nodes[leader].Submit("cmd1")
	time.Sleep(500 * time.Millisecond)

	var followers []int
	for i := range 3 {
		if i != leader {
			followers = append(followers, i)
			cluster.CrashNode(i)
		}
	}

	cluster.nodes[leader].Submit("cmd2")
	cluster.nodes[leader].Submit("cmd3")
	time.Sleep(200 * time.Millisecond)

	cluster.CrashNode(leader)
	cluster.ReviveNode(followers[0])
	cluster.ReviveNode(followers[1])
	time.Sleep(1 * time.Second)

	newLeader, _ := cluster.CheckSingleLeader()
	cluster.nodes[newLeader].Submit("cmd4")
	time.Sleep(500 * time.Millisecond)

	cluster.ReviveNode(leader)
	time.Sleep(2 * time.Second)

	for i := range 3 {
		cluster.nodes[i].mu.Lock()
		if len(cluster.nodes[i].log) != 3 {
			t.Fatalf("Node %d incorrect log length: %d", i, len(cluster.nodes[i].log))
		}
		if cluster.nodes[i].log[2].Command.(string) != "cmd4" {
			t.Fatalf("Node %d incorrect last command", i)
		}
		cluster.nodes[i].mu.Unlock()
	}
}

func TestPersistence(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)

	leader, term := cluster.CheckSingleLeader()
	cluster.nodes[leader].Submit("p1")
	cluster.nodes[leader].Submit("p2")
	time.Sleep(500 * time.Millisecond)

	cluster.CrashNode(leader)
	storage := cluster.nodes[leader].storage

	var peerIds []int
	for j := range 3 {
		if leader != j {
			peerIds = append(peerIds, j)
		}
	}

	applyCh := make(chan ApplyMsg, 100)
	rebooted := NewConsensusModule(leader, peerIds, cluster, storage, applyCh)
	cluster.nodes[leader] = rebooted
	cluster.ReviveNode(leader)
	go rebooted.runElectionTimer()

	rebooted.mu.Lock()
	defer rebooted.mu.Unlock()
	if rebooted.currentTerm != term || len(rebooted.log) != 3 {
		t.Fatalf("Persistence failed")
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
	if cluster.nodes[leader].commitIndex >= idx {
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

func TestStats(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)

	leader, _ := cluster.CheckSingleLeader()
	cluster.nodes[leader].Submit("s-cmd")
	time.Sleep(500 * time.Millisecond)

	stats := cluster.nodes[leader].GetStats()
	if stats.State != "Leader" || stats.LogLen < 2 {
		t.Fatalf("Invalid stats: %+v", stats)
	}
}

func TestSnapshotting(t *testing.T) {
	cluster := NewMockCluster(3)
	defer cluster.Shutdown()
	time.Sleep(1 * time.Second)

	leader, _ := cluster.CheckSingleLeader()
	for i := 1; i <= 100; i++ {
		cluster.nodes[leader].Submit(fmt.Sprintf("cmd-%d", i))
	}
	time.Sleep(2 * time.Second)

	for i := range 3 {
		cluster.nodes[i].Snapshot(100, []byte("snap"))
	}

	cluster.nodes[leader].mu.Lock()
	if cluster.nodes[leader].lastIncludedIndex < 100 {
		t.Fatalf("Leader not snapshotted")
	}
	cluster.nodes[leader].mu.Unlock()

	follower := (leader + 1) % 3
	cluster.CrashNode(follower)
	cluster.nodes[follower].Kill()

	for i := 101; i <= 110; i++ {
		cluster.nodes[leader].Submit(fmt.Sprintf("cmd-%d", i))
	}
	time.Sleep(500 * time.Millisecond)

	var peerIds []int
	for j := range 3 {
		if follower != j {
			peerIds = append(peerIds, j)
		}
	}

	storage := cluster.nodes[follower].storage
	rebooted := NewConsensusModule(follower, peerIds, cluster, storage, make(chan ApplyMsg, 100))
	cluster.nodes[follower] = rebooted
	cluster.ReviveNode(follower)
	go rebooted.runElectionTimer()

	time.Sleep(3 * time.Second)
	rebooted.mu.Lock()
	if rebooted.lastIncludedIndex < 100 || rebooted.commitIndex < 110 {
		t.Fatalf("Follower catch up failed")
	}
	rebooted.mu.Unlock()
}
