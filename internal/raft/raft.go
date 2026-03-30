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
	AppendLog(entry []byte) error
	Sync() error
	ReadLogRange(start, end int) ([][]byte, error)
	SaveSnapshot(metadata, data []byte) error
	SaveMeta(meta []byte) error
	ClearLog() error
	HasData() bool
	ReadState() (meta []byte, snap []byte, logs [][]byte, err error)
}

type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type persistentMeta struct {
	CurrentTerm       int
	VotedFor          int
	CommitIndex       int
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

	id      int
	peerIds []int
	server  Transporter
	storage Storage
	applyCh chan ApplyMsg

	applyCond *sync.Cond
	state     State

	currentTerm       int
	votedFor          int
	log               []LogEntry
	lastElectionReset time.Time

	commitIndex atomic.Int64
	lastApplied atomic.Int64

	nextIndex  map[int]int
	matchIndex map[int]int

	replicationCond map[int]*sync.Cond

	// WAL persistence state
	persistedIndex int
	persistCond    *sync.Cond
	syncCond       *sync.Cond
	walTruncated   bool

	// Lease-based read state
	recentAcks map[int]time.Time

	dead                int32
	electionTimerPaused bool
	leaderId            int
	triggerCh           chan struct{}

	lastIncludedIndex int
	lastIncludedTerm  int

	// Pending snapshot for ordered delivery in applier
	pendingSnapshot *ApplyMsg
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
		CommitIndex:       int(cm.commitIndex.Load()),
		LastApplied:       int(cm.lastApplied.Load()),
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
	cm.mu.Lock()
	cm.applyCond.Broadcast()
	cm.persistCond.Broadcast()
	cm.syncCond.Broadcast()
	for _, cond := range cm.replicationCond {
		cond.Broadcast()
	}
	cm.mu.Unlock()
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

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.leaderId = -1
	cm.recentAcks = make(map[int]time.Time)
	cm.persistMeta()
	for _, cond := range cm.replicationCond {
		cond.Broadcast()
	}
}

func (cm *ConsensusModule) Snapshot(index int, snapshot []byte) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if index <= cm.lastIncludedIndex || index > int(cm.lastApplied.Load()) {
		return
	}

	cm.dlog("Snapshotting at index %d", index)

	offset := index - cm.lastIncludedIndex
	newLog := make([]LogEntry, 1+len(cm.log)-offset-1)
	newLog[0] = LogEntry{Term: cm.log[offset].Term}
	copy(newLog[1:], cm.log[offset+1:])

	cm.lastIncludedTerm = newLog[0].Term
	cm.lastIncludedIndex = index
	cm.log = newLog

	cm.persistedIndex = cm.lastIncludedIndex + len(cm.log) - 1
	cm.walTruncated = true

	meta := persistentMeta{
		CurrentTerm:       cm.currentTerm,
		VotedFor:          cm.votedFor,
		CommitIndex:       int(cm.commitIndex.Load()),
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
	}
	// Write snapshot before metadata to ensure recovery safety.
	cm.storage.SaveSnapshot(encodeMeta(meta), snapshot)
	cm.persistCond.Broadcast()
}

func (cm *ConsensusModule) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	reply.Term = cm.currentTerm
	if args.Term < cm.currentTerm {
		return nil
	}

	if args.Term > cm.currentTerm || cm.state != Follower {
		cm.becomeFollower(args.Term)
	}

	cm.leaderId = args.LeaderId
	cm.lastElectionReset = time.Now()

	if args.LastIncludedIndex <= cm.lastIncludedIndex {
		return nil
	}

	if args.LastIncludedIndex < cm.lastIncludedIndex+len(cm.log) &&
		cm.log[args.LastIncludedIndex-cm.lastIncludedIndex].Term == args.LastIncludedTerm {
		offset := args.LastIncludedIndex - cm.lastIncludedIndex
		newLog := make([]LogEntry, 1+len(cm.log)-offset-1)
		newLog[0] = LogEntry{Term: args.LastIncludedTerm}
		copy(newLog[1:], cm.log[offset+1:])
		cm.log = newLog
	} else {
		cm.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	cm.lastIncludedIndex = args.LastIncludedIndex
	cm.lastIncludedTerm = args.LastIncludedTerm

	if int(cm.commitIndex.Load()) < args.LastIncludedIndex {
		cm.commitIndex.Store(int64(args.LastIncludedIndex))
	}
	if int(cm.lastApplied.Load()) < args.LastIncludedIndex {
		cm.lastApplied.Store(int64(args.LastIncludedIndex))
	}
	cm.persistedIndex = cm.lastIncludedIndex + len(cm.log) - 1

	cm.walTruncated = true
	meta := persistentMeta{
		CurrentTerm:       cm.currentTerm,
		VotedFor:          cm.votedFor,
		CommitIndex:       int(cm.commitIndex.Load()),
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
	}
	cm.storage.SaveSnapshot(encodeMeta(meta), args.Data)

	cm.pendingSnapshot = &ApplyMsg{
		CommandValid: false,
		Data:         args.Data,
		CommandIndex: args.LastIncludedIndex,
		CommandTerm:  args.LastIncludedTerm,
	}
	cm.applyCond.Broadcast()

	return nil
}

func (cm *ConsensusModule) GetApplyCh() <-chan ApplyMsg {
	return cm.applyCh
}

func (cm *ConsensusModule) GetInternalState() (int, int, []LogEntry) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return int(cm.commitIndex.Load()), int(cm.lastApplied.Load()), append([]LogEntry(nil), cm.log...)
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
		id:                id,
		peerIds:           peerIds,
		server:            server,
		storage:           storage,
		applyCh:           applyCh,
		state:             Follower,
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]LogEntry, 1),
		nextIndex:         make(map[int]int),
		matchIndex:        make(map[int]int),
		leaderId:          -1,
		triggerCh:         make(chan struct{}, 1),
		replicationCond:   make(map[int]*sync.Cond),
		persistedIndex:    0,
		lastElectionReset: time.Now(),
		recentAcks:        make(map[int]time.Time),
	}
	cm.applyCond = sync.NewCond(&cm.mu)
	cm.persistCond = sync.NewCond(&cm.mu)
	cm.syncCond = sync.NewCond(&cm.mu)
	for _, peerId := range peerIds {
		cm.replicationCond[peerId] = sync.NewCond(&cm.mu)
	}
	if cm.storage.HasData() {
		cm.restoreFromStorage()
		cm.lastApplied.Store(int64(cm.lastIncludedIndex))
		cm.persistedIndex = cm.lastIncludedIndex + len(cm.log) - 1
	} else {
		cm.currentTerm = 0
		cm.votedFor = -1
		cm.log = []LogEntry{{Term: 0}}
		cm.commitIndex.Store(0)
		cm.lastIncludedIndex = 0
		cm.lastIncludedTerm = 0
	}

	go cm.runElectionTimer()
	go cm.applier()
	go cm.persister()
	return cm
}

func (cm *ConsensusModule) persister() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for !cm.isKilled() {
		lastLogIndex := cm.lastIncludedIndex + len(cm.log) - 1
		if cm.persistedIndex >= lastLogIndex && !cm.walTruncated {
			cm.persistCond.Wait()
			continue
		}

		truncated := cm.walTruncated
		cm.walTruncated = false

		var walEntries []LogEntry
		var targetIndex int

		if truncated {
			walEntries = make([]LogEntry, len(cm.log)-1)
			copy(walEntries, cm.log[1:])
			targetIndex = lastLogIndex
		} else {
			fromIdx := cm.persistedIndex + 1
			toIdx := lastLogIndex
			numNew := toIdx - fromIdx + 1
			walEntries = make([]LogEntry, numNew)
			for i := 0; i < numNew; i++ {
				localIdx := (fromIdx + i) - cm.lastIncludedIndex
				walEntries[i] = cm.log[localIdx]
			}
			targetIndex = toIdx
		}

		meta := persistentMeta{
			CurrentTerm:       cm.currentTerm,
			VotedFor:          cm.votedFor,
			CommitIndex:       int(cm.commitIndex.Load()),
			LastIncludedIndex: cm.lastIncludedIndex,
			LastIncludedTerm:  cm.lastIncludedTerm,
		}

		cm.mu.Unlock()

		cm.storage.SaveMeta(encodeMeta(meta))
		if truncated {
			cm.storage.ClearLog()
		}

		for _, entry := range walEntries {
			var cmdBuf bytes.Buffer
			enc := gob.NewEncoder(&cmdBuf)
			if err := enc.Encode(&entry); err == nil {
				cm.storage.AppendLog(cmdBuf.Bytes())
			}
		}
		cm.storage.Sync()

		cm.mu.Lock()
		if targetIndex > cm.persistedIndex {
			cm.persistedIndex = targetIndex
		}
		cm.syncCond.Broadcast()

		if cm.state == Leader {
			cm.advanceCommitIndex()
		}
	}
}

func encodeMeta(meta persistentMeta) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(meta); err != nil {
		panic("encodeMeta: " + err.Error())
	}
	return buf.Bytes()
}

func (cm *ConsensusModule) persistMeta() {
	meta := persistentMeta{
		CurrentTerm:       cm.currentTerm,
		VotedFor:          cm.votedFor,
		CommitIndex:       int(cm.commitIndex.Load()),
		LastIncludedIndex: cm.lastIncludedIndex,
		LastIncludedTerm:  cm.lastIncludedTerm,
	}
	cm.storage.SaveMeta(encodeMeta(meta))
}

func (cm *ConsensusModule) restoreFromStorage() {
	metaData, snapData, logsData, err := cm.storage.ReadState()
	if err != nil {
		panic("restoreFromStorage: " + err.Error())
	}

	if len(metaData) > 0 {
		var meta persistentMeta
		dec := gob.NewDecoder(bytes.NewBuffer(metaData))
		if err := dec.Decode(&meta); err == nil {
			cm.currentTerm = meta.CurrentTerm
			cm.votedFor = meta.VotedFor
			cm.commitIndex.Store(int64(meta.CommitIndex))
			cm.lastIncludedIndex = meta.LastIncludedIndex
			cm.lastIncludedTerm = meta.LastIncludedTerm
		}
	}

	cm.log = []LogEntry{{Term: cm.lastIncludedTerm}}
	for _, logBytes := range logsData {
		var entry LogEntry
		if err := gob.NewDecoder(bytes.NewReader(logBytes)).Decode(&entry); err == nil {
			cm.log = append(cm.log, entry)
		}
	}

	maxCommitIndex := cm.lastIncludedIndex + len(cm.log) - 1
	if int(cm.commitIndex.Load()) < cm.lastIncludedIndex {
		cm.commitIndex.Store(int64(cm.lastIncludedIndex))
	} else if int(cm.commitIndex.Load()) > maxCommitIndex {
		cm.commitIndex.Store(int64(maxCommitIndex))
	}

	if len(snapData) > 0 {
		cm.pendingSnapshot = &ApplyMsg{
			CommandValid: false,
			Data:         snapData,
			CommandIndex: cm.lastIncludedIndex,
			CommandTerm:  cm.lastIncludedTerm,
		}
	}
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
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
		cm.persistMeta()
	}

	return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	reply.ConflictTerm = -1

	if args.Term < cm.currentTerm {
		reply.Term = cm.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > cm.currentTerm || cm.state == Candidate {
		cm.becomeFollower(args.Term)
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
		conflictTerm := cm.log[args.PrevLogIndex-cm.lastIncludedIndex].Term
		reply.ConflictTerm = conflictTerm
		firstIdx := args.PrevLogIndex
		for firstIdx > cm.lastIncludedIndex && cm.log[firstIdx-cm.lastIncludedIndex].Term == conflictTerm {
			firstIdx--
		}
		reply.ConflictIndex = firstIdx + 1
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
		if insertIndex-1 < cm.persistedIndex {
			cm.persistedIndex = insertIndex - 1
			cm.walTruncated = true
		}
		cm.persistCond.Broadcast()
	}

	// Wait for durability before acknowledging success.
	if len(args.Entries) > 0 {
		targetIndex := args.PrevLogIndex + len(args.Entries)
		for cm.persistedIndex < targetIndex && !cm.isKilled() {
			cm.syncCond.Wait()
		}
		if cm.isKilled() {
			return nil
		}
	}

	if args.LeaderCommit > int(cm.commitIndex.Load()) {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		cm.commitIndex.Store(int64(min(args.LeaderCommit, lastNewEntryIndex)))
		cm.persistMeta()
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

	cm.persistCond.Broadcast()
	select {
	case cm.triggerCh <- struct{}{}:
	default:
	}

	return index, term, true
}

func (cm *ConsensusModule) applier() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for !cm.isKilled() {
		if cm.pendingSnapshot != nil {
			snap := cm.pendingSnapshot
			cm.pendingSnapshot = nil
			cm.mu.Unlock()
			cm.applyCh <- *snap
			cm.mu.Lock()
			continue
		}

		for int(cm.lastApplied.Load()) >= int(cm.commitIndex.Load()) && cm.pendingSnapshot == nil && !cm.isKilled() {
			cm.applyCond.Wait()
		}
		if cm.isKilled() {
			return
		}

		if cm.pendingSnapshot != nil {
			continue
		}

		if int(cm.commitIndex.Load()) <= cm.lastIncludedIndex {
			cm.lastApplied.Store(int64(max(int(cm.lastApplied.Load()), cm.lastIncludedIndex)))
			continue
		}

		commitIndex := int(cm.commitIndex.Load())
		lastApplied := max(int(cm.lastApplied.Load()), cm.lastIncludedIndex)
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
		if commitIndex > int(cm.lastApplied.Load()) {
			cm.lastApplied.Store(int64(commitIndex))
		}
	}
}

// LinearizableRead verifies leadership lease for consistent GET operations.
func (cm *ConsensusModule) LinearizableRead() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state != Leader {
		return fmt.Errorf("not leader")
	}

	validAcks := 1 // self
	now := time.Now()
	for _, ackTime := range cm.recentAcks {
		if now.Sub(ackTime) < 150*time.Millisecond {
			validAcks++
		}
	}

	if validAcks*2 > len(cm.peerIds)+1 {
		return nil
	}
	return fmt.Errorf("leader lease expired")
}

func (cm *ConsensusModule) ReadBarrier() (int, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state != Leader {
		return 0, fmt.Errorf("not leader")
	}

	validAcks := 1
	now := time.Now()
	for _, ackTime := range cm.recentAcks {
		if now.Sub(ackTime) < 150*time.Millisecond {
			validAcks++
		}
	}

	if validAcks*2 <= len(cm.peerIds)+1 {
		return 0, fmt.Errorf("leader lease expired")
	}

	return int(cm.commitIndex.Load()), nil
}
