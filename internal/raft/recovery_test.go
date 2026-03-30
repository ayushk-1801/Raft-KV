package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type persistentTestStorage struct {
	mu   sync.Mutex
	meta []byte
	snap []byte
	logs [][]byte
}

func (s *persistentTestStorage) AppendLog(entry []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]byte, len(entry))
	copy(copied, entry)
	s.logs = append(s.logs, copied)
	return nil
}

func (s *persistentTestStorage) Sync() error { return nil }

func (s *persistentTestStorage) ReadLogRange(start, end int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if start < 0 {
		start = 0
	}
	if end > len(s.logs) {
		end = len(s.logs)
	}
	if start > end {
		return nil, nil
	}

	out := make([][]byte, 0, end-start)
	for _, entry := range s.logs[start:end] {
		copied := make([]byte, len(entry))
		copy(copied, entry)
		out = append(out, copied)
	}
	return out, nil
}

func (s *persistentTestStorage) SaveSnapshot(meta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = append([]byte(nil), meta...)
	s.snap = append([]byte(nil), data...)
	return nil
}

func (s *persistentTestStorage) SaveMeta(meta []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = append([]byte(nil), meta...)
	return nil
}

func (s *persistentTestStorage) ClearLog() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = nil
	return nil
}

func (s *persistentTestStorage) HasData() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.meta) > 0 || len(s.snap) > 0 || len(s.logs) > 0
}

func (s *persistentTestStorage) ReadState() (meta []byte, snap []byte, logs [][]byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta = append([]byte(nil), s.meta...)
	snap = append([]byte(nil), s.snap...)
	logs = make([][]byte, 0, len(s.logs))
	for _, entry := range s.logs {
		copied := make([]byte, len(entry))
		copy(copied, entry)
		logs = append(logs, copied)
	}
	return meta, snap, logs, nil
}

type noopTransport struct{}

func (noopTransport) SendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error {
	return fmt.Errorf("unexpected transport call")
}

func (noopTransport) SendAppendEntries(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	return fmt.Errorf("unexpected transport call")
}

func (noopTransport) SendInstallSnapshot(peerId int, args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	return fmt.Errorf("unexpected transport call")
}

func encodeTestLogEntry(t *testing.T, entry LogEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&entry); err != nil {
		t.Fatalf("encode log entry: %v", err)
	}
	return buf.Bytes()
}

func TestRestoreReplaysCommittedSuffix(t *testing.T) {
	storage := &persistentTestStorage{}
	storage.meta = encodeMeta(persistentMeta{
		CurrentTerm:       3,
		VotedFor:          -1,
		CommitIndex:       1,
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	})
	storage.logs = [][]byte{
		encodeTestLogEntry(t, LogEntry{Term: 3, Command: "restored"}),
	}

	applyCh := make(chan ApplyMsg, 2)
	cm := NewConsensusModule(0, nil, noopTransport{}, storage, applyCh)
	defer cm.Kill()

	select {
	case msg := <-applyCh:
		if !msg.CommandValid {
			t.Fatalf("expected committed command, got snapshot message")
		}
		if msg.CommandIndex != 1 {
			t.Fatalf("expected apply index 1, got %d", msg.CommandIndex)
		}
		if got, ok := msg.Command.(string); !ok || got != "restored" {
			t.Fatalf("expected restored command, got %#v", msg.Command)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for restored committed entry to apply")
	}

	if got := int(cm.commitIndex.Load()); got != 1 {
		t.Fatalf("expected commit index 1 after restore, got %d", got)
	}
}

func TestLeadershipTransitionsClearRecentAcks(t *testing.T) {
	cm := &ConsensusModule{
		id:              1,
		log:             []LogEntry{{Term: 0}},
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		replicationCond: make(map[int]*sync.Cond),
		recentAcks: map[int]time.Time{
			2: time.Now(),
			3: time.Now(),
		},
		triggerCh: make(chan struct{}, 1),
		storage:   &persistentTestStorage{},
	}
	atomic.StoreInt32(&cm.dead, 1)

	cm.startLeader()
	if len(cm.recentAcks) != 0 {
		t.Fatalf("expected startLeader to clear recent acknowledgements, got %d entries", len(cm.recentAcks))
	}

	cm.recentAcks[2] = time.Now()
	cm.becomeFollower(2)
	if len(cm.recentAcks) != 0 {
		t.Fatalf("expected becomeFollower to clear recent acknowledgements, got %d entries", len(cm.recentAcks))
	}
}
