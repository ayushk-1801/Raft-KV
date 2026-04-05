package raft

import (
	"time"
)

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.leaderId = cm.id
	cm.recentAcks = make(map[int]time.Time)
	cm.dlog("Became LEADER for term %d", cm.currentTerm)

	lastLogIndex := cm.lastIncludedIndex + len(cm.log) - 1

	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = lastLogIndex + 1
		cm.matchIndex[peerId] = cm.lastIncludedIndex
		go cm.replicator(peerId)
	}

	go cm.runHeartbeatTimer()
}

func (cm *ConsensusModule) runHeartbeatTimer() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for !cm.isKilled() {
		select {
		case <-ticker.C:
		case <-cm.triggerCh:
		}

		cm.mu.Lock()
		if cm.state != Leader {
			cm.mu.Unlock()
			return
		}
		for _, peerId := range cm.peerIds {
			cm.replicationCond[peerId].Broadcast()
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) replicator(peer int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	savedTerm := cm.currentTerm

	for !cm.isKilled() && cm.state == Leader && savedTerm == cm.currentTerm {
		nextIdx := cm.nextIndex[peer]

		if nextIdx <= cm.lastIncludedIndex {
			_, snapshotData, _, _ := cm.storage.ReadState()
			args := InstallSnapshotArgs{
				Term:              savedTerm,
				LeaderId:          cm.id,
				LastIncludedIndex: cm.lastIncludedIndex,
				LastIncludedTerm:  cm.lastIncludedTerm,
				Data:              snapshotData,
			}
			cm.mu.Unlock()

			var reply InstallSnapshotReply
			err := cm.server.SendInstallSnapshot(peer, args, &reply)

			cm.mu.Lock()
			if cm.state != Leader || cm.currentTerm != savedTerm {
				return
			}
			if err != nil {
				cm.replicationCond[peer].Wait()
				continue
			}
			if reply.Term > cm.currentTerm {
				_ = cm.becomeFollower(reply.Term)
				return
			}
			cm.matchIndex[peer] = max(cm.matchIndex[peer], args.LastIncludedIndex)
			cm.nextIndex[peer] = cm.matchIndex[peer] + 1
			continue
		}

		prevLogIndex := nextIdx - 1
		prevLogTerm := cm.lastIncludedTerm
		if prevLogIndex > cm.lastIncludedIndex {
			prevLogTerm = cm.log[prevLogIndex-cm.lastIncludedIndex].Term
		}

		lastLogIndex := cm.lastIncludedIndex + len(cm.log) - 1
		var entries []LogEntry
		if nextIdx <= lastLogIndex {
			entries = make([]LogEntry, len(cm.log[nextIdx-cm.lastIncludedIndex:]))
			copy(entries, cm.log[nextIdx-cm.lastIncludedIndex:])
		}

		args := AppendEntriesArgs{
			Term:         savedTerm,
			LeaderID:     cm.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: int(cm.commitIndex.Load()),
		}
		cm.mu.Unlock()

		var reply AppendEntriesReply
		err := cm.server.SendAppendEntries(peer, args, &reply)

		cm.mu.Lock()
		if cm.state != Leader || cm.currentTerm != savedTerm {
			return
		}
		if err != nil {
			cm.replicationCond[peer].Wait()
			continue
		}

		cm.recentAcks[peer] = time.Now()
		if reply.Term > cm.currentTerm {
			_ = cm.becomeFollower(reply.Term)
			return
		}

		if reply.Success {
			newMatch := args.PrevLogIndex + len(args.Entries)
			if newMatch > cm.matchIndex[peer] {
				cm.matchIndex[peer] = newMatch
			}
			cm.nextIndex[peer] = cm.matchIndex[peer] + 1
			cm.advanceCommitIndex()
		} else {
			if reply.ConflictTerm != -1 {
				lastIndexWithTerm := -1
				for i := len(cm.log) - 1; i >= 0; i-- {
					if cm.log[i].Term == reply.ConflictTerm {
						lastIndexWithTerm = i + cm.lastIncludedIndex
						break
					}
				}
				if lastIndexWithTerm != -1 {
					cm.nextIndex[peer] = lastIndexWithTerm + 1
				} else {
					cm.nextIndex[peer] = reply.ConflictIndex
				}
			} else {
				cm.nextIndex[peer] = reply.ConflictIndex
			}
			if cm.nextIndex[peer] < 1 {
				cm.nextIndex[peer] = 1
			}
		}

		if cm.nextIndex[peer] > (cm.lastIncludedIndex + len(cm.log) - 1) {
			cm.replicationCond[peer].Wait()
		}
	}
}

func (cm *ConsensusModule) advanceCommitIndex() {
	for i := cm.lastIncludedIndex + len(cm.log) - 1; i > int(cm.commitIndex.Load()); i-- {
		if i > cm.persistedIndex {
			continue
		}
		if cm.log[i-cm.lastIncludedIndex].Term != cm.currentTerm {
			continue
		}

		matchCount := 1
		for _, peerId := range cm.peerIds {
			if cm.matchIndex[peerId] >= i {
				matchCount++
			}
		}

		if matchCount*2 > len(cm.peerIds)+1 {
			prevCommitIndex := int(cm.commitIndex.Load())
			cm.commitIndex.Store(int64(i))
			if err := cm.persistMeta(); err != nil {
				cm.commitIndex.Store(int64(prevCommitIndex))
				return
			}
			cm.applyCond.Broadcast()
			return
		}
	}
}
