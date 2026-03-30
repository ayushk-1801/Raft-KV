package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Leader
	Candidate
)

type Transporter interface {
	SendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error
	SendAppendEntries(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) error
	SendInstallSnapshot(peerId int, args InstallSnapshotArgs, reply *InstallSnapshotReply) error
}

type Storage interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	HasData() bool
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type persistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	Data         []byte
}

type ConsensusModule struct {
	mu sync.Mutex

	id        int
	peerIds   []int
	server    Transporter
	storage   Storage
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	state     State

	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastElectionReset time.Time

	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	dead                int32
	electionTimerPaused bool
	leaderId            int

	lastIncludedIndex int
	lastIncludedTerm  int
}

type RaftStats struct {
	ID                int
	Term              int
	State             string
	CommitIndex       int
	LastApplied       int
	LogLen            int
	LastIncludedIndex int
	LastIncludedTerm  int
	NextIndex         map[int]int
	MatchIndex        map[int]int
	LeaderID          int
}

func (cm *ConsensusModule) GetStats() RaftStats {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	stateStr := "Follower"
	switch cm.state {
	case Candidate:
		stateStr = "Candidate"
	case Leader:
		stateStr = "Leader"
	}

	nextIndexCopy := make(map[int]int)
	for k, v := range cm.nextIndex {
		nextIndexCopy[k] = v
	}

	matchIndexCopy := make(map[int]int)
	for k, v := range cm.matchIndex {
		matchIndexCopy[k] = v
	}

	return RaftStats{
		ID:                cm.id,
		Term:              cm.currentTerm,
		State:             stateStr,
		CommitIndex:       cm.commitIndex,
		LastApplied:       cm.lastApplied,
		LogLen:            cm.lastIncludedIndex + len(cm.log),
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
		NextIndex:         nextIndexCopy,
		MatchIndex:        matchIndexCopy,
		LeaderID:          cm.leaderId,
	}
}

func (cm *ConsensusModule) Kill() {
	atomic.StoreInt32(&cm.dead, 1)
}

func (cm *ConsensusModule) isKilled() bool {
	return atomic.LoadInt32(&cm.dead) == 1
}

func (cm *ConsensusModule) PauseElectionTimer(paused bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.electionTimerPaused = paused
	if !paused {
		cm.lastElectionReset = time.Now()
	}
}

func (cm *ConsensusModule) GetState() (int, int, bool, int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader, cm.leaderId
}

func (cm *ConsensusModule) Snapshot(index int, snapshot []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if index <= cm.lastIncludedIndex || index > cm.lastApplied {
		return
	}

	cm.dlog("Snapshotting at index %d", index)
	
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: cm.log[index-cm.lastIncludedIndex].Term})
	newLog = append(newLog, cm.log[index-cm.lastIncludedIndex+1:]...)

	cm.lastIncludedTerm = cm.log[index-cm.lastIncludedIndex].Term
	cm.lastIncludedIndex = index
	cm.log = newLog

	cm.persistToStorage()
	cm.storage.Set("raft_snapshot", snapshot)
}

func (cm *ConsensusModule) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	reply.Term = cm.currentTerm
	if args.Term < cm.currentTerm {
		return nil
	}

	if args.Term > cm.currentTerm {
		cm.currentTerm = args.Term
		cm.votedFor = -1
		cm.state = Follower
		cm.persistToStorage()
	}

	cm.leaderId = args.LeaderId
	cm.lastElectionReset = time.Now()

	if args.LastIncludedIndex <= cm.lastIncludedIndex {
		return nil
	}

	if args.LastIncludedIndex < cm.lastIncludedIndex+len(cm.log) && 
	   cm.log[args.LastIncludedIndex-cm.lastIncludedIndex].Term == args.LastIncludedTerm {
		cm.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, cm.log[args.LastIncludedIndex-cm.lastIncludedIndex+1:]...)
	} else {
		cm.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	cm.lastIncludedIndex = args.LastIncludedIndex
	cm.lastIncludedTerm = args.LastIncludedTerm
	
	cm.commitIndex = max(cm.commitIndex, args.LastIncludedIndex)
	cm.lastApplied = max(cm.lastApplied, args.LastIncludedIndex)

	cm.persistToStorage()
	cm.storage.Set("raft_snapshot", args.Data)

	msg := ApplyMsg{
		CommandValid: false,
		Command:      "SNAPSHOT",
		Data:         args.Data,
		CommandIndex: args.LastIncludedIndex,
		CommandTerm:  args.LastIncludedTerm,
	}
	
	go func() {
		cm.applyCh <- msg
	}()

	return nil
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	var stateStr string
	switch cm.state {
	case Follower:
		stateStr = "FOLL"
	case Candidate:
		stateStr = "CAND"
	case Leader:
		stateStr = "LEAD"
	}

	prefix := fmt.Sprintf("[N%d][T%d][%s] ", cm.id, cm.currentTerm, stateStr)
	log.Printf(prefix+format, args...)
}

func NewConsensusModule(id int, peerIds []int, server Transporter, storage Storage, applyCh chan ApplyMsg) *ConsensusModule {
	cm := &ConsensusModule{
		id:          id,
		peerIds:     peerIds,
		server:      server,
		storage:     storage,
		applyCh:     applyCh,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		leaderId:    -1,
	}
	cm.applyCond = sync.NewCond(&cm.mu)
	if cm.storage.HasData() {
		cm.restoreFromStorage()
		cm.commitIndex = cm.lastIncludedIndex
		cm.lastApplied = cm.lastIncludedIndex
		
		if cm.lastIncludedIndex > 0 {
			snapshotData, err := cm.storage.Get("raft_snapshot")
			if err == nil {
				msg := ApplyMsg{
					CommandValid: false,
					Command:      "SNAPSHOT",
					Data:         snapshotData,
					CommandIndex: cm.lastIncludedIndex,
					CommandTerm:  cm.lastIncludedTerm,
				}
				go func() {
					cm.applyCh <- msg
				}()
			}
		}
	} else {
		cm.currentTerm = 0
		cm.votedFor = -1
		cm.log = make([]LogEntry, 1)
		cm.log[0] = LogEntry{Term: 0}
		cm.lastIncludedIndex = 0
		cm.lastIncludedTerm = 0
	}
	
	go cm.runElectionTimer()
	go cm.applier()
	return cm
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if args.Term > cm.currentTerm {
		cm.dlog("Term mismatch. Stepping down to Follower")
		cm.currentTerm = args.Term
		cm.votedFor = -1
		cm.state = Follower
		cm.persistToStorage()
	}

	reply.Term = cm.currentTerm
	reply.VoteGranted = false

	if args.Term < cm.currentTerm {
		return nil
	}

	lastLogIndex := cm.lastIncludedIndex + len(cm.log) - 1
	lastLogTerm := cm.log[len(cm.log)-1].Term

	logIsUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (cm.votedFor == -1 || cm.votedFor == args.CandidateID) && logIsUpToDate {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateID
		cm.lastElectionReset = time.Now()
		cm.persistToStorage()
	}

	return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if args.Term < cm.currentTerm {
		reply.Term = cm.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > cm.currentTerm || cm.state == Candidate {
		cm.dlog("Stepping down to Follower (term %d)", args.Term)
		cm.currentTerm = args.Term
		cm.state = Follower
		cm.votedFor = -1
		cm.persistToStorage()
	}

	cm.leaderId = args.LeaderID
	cm.lastElectionReset = time.Now()
	reply.Term = cm.currentTerm
	reply.Success = false

	if args.PrevLogIndex < cm.lastIncludedIndex {
		reply.ConflictIndex = cm.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return nil
	}

	if args.PrevLogIndex >= cm.lastIncludedIndex+len(cm.log) {
		reply.ConflictIndex = cm.lastIncludedIndex + len(cm.log)
		reply.ConflictTerm = -1
		return nil
	}

	if cm.log[args.PrevLogIndex-cm.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = cm.log[args.PrevLogIndex-cm.lastIncludedIndex].Term
		var i int
		for i = args.PrevLogIndex; i > cm.lastIncludedIndex && cm.log[i-cm.lastIncludedIndex].Term == reply.ConflictTerm; i-- {
		}
		reply.ConflictIndex = i + 1
		return nil
	}

	insertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if insertIndex >= cm.lastIncludedIndex+len(cm.log) || newEntriesIndex >= len(args.Entries) {
			break
		}
		if cm.log[insertIndex-cm.lastIncludedIndex].Term != args.Entries[newEntriesIndex].Term {
			break
		}
		insertIndex++
		newEntriesIndex++
	}

	if newEntriesIndex < len(args.Entries) {
		cm.log = append(cm.log[:insertIndex-cm.lastIncludedIndex], args.Entries[newEntriesIndex:]...)
		cm.persistToStorage()
	}

	if args.LeaderCommit > cm.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		cm.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		cm.applyCond.Broadcast() 
	}

	reply.Success = true
	return nil
}

func (cm *ConsensusModule) Submit(command interface{}) (int, int, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state != Leader {
		return -1, -1, false
	}

	index := cm.lastIncludedIndex + len(cm.log)
	term := cm.currentTerm

	cm.log = append(cm.log, LogEntry{
		Term:    term,
		Command: command,
	})

	cm.persistToStorage()
	return index, term, true
}

func (cm *ConsensusModule) persistToStorage() {
	state := persistentState{
		CurrentTerm:       cm.currentTerm,
		VotedFor:          cm.votedFor,
		Log:               cm.log,
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		panic("Encode error: " + err.Error())
	}
	cm.storage.Set("raft_state", buf.Bytes())
}

func (cm *ConsensusModule) restoreFromStorage() {
	data, err := cm.storage.Get("raft_state")
	if err != nil {
		panic("Read error: " + err.Error())
	}

	var state persistentState
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&state); err != nil {
		panic("Decode error: " + err.Error())
	}

	cm.currentTerm = state.CurrentTerm
	cm.votedFor = state.VotedFor
	cm.log = state.Log
	cm.lastIncludedIndex = state.LastIncludedIndex
	cm.lastIncludedTerm = state.LastIncludedTerm
}

func (cm *ConsensusModule) applier() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for !cm.isKilled() {
		for cm.lastApplied >= cm.commitIndex && !cm.isKilled() {
			cm.applyCond.Wait()
		}
		if cm.isKilled() {
			return
		}

		if cm.commitIndex <= cm.lastIncludedIndex {
			cm.lastApplied = max(cm.lastApplied, cm.commitIndex)
			continue
		}

		commitIndex := cm.commitIndex
		lastApplied := max(cm.lastApplied, cm.lastIncludedIndex)
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, cm.log[lastApplied+1-cm.lastIncludedIndex:commitIndex+1-cm.lastIncludedIndex])

		cm.mu.Unlock()

		for i, entry := range entries {
			cm.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + i + 1,
				CommandTerm:  entry.Term,
			}
		}

		cm.mu.Lock()
		if commitIndex > cm.lastApplied {
			cm.lastApplied = commitIndex
		}
	}
}
