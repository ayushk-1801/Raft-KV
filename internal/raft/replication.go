package raft

import (
	"time"
)

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.leaderId = cm.id
	cm.dlog("Became LEADER for term %d", cm.currentTerm)

	lastLogIndex := cm.lastIncludedIndex + len(cm.log) - 1

	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = lastLogIndex + 1
		cm.matchIndex[peerId] = 0
	}

	go cm.broadcastHeartbeats()
	go cm.runHeartbeatTimer()
}

func (cm *ConsensusModule) runHeartbeatTimer() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for !cm.isKilled() {
		<-ticker.C
		cm.mu.Lock()
		if cm.state != Leader {
			cm.mu.Unlock()
			continue
		}
		cm.mu.Unlock()
		cm.broadcastHeartbeats()
	}
}

func (cm *ConsensusModule) broadcastHeartbeats() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	savedTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peer int) {
			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}

			nextIdx := cm.nextIndex[peer]

			if nextIdx <= cm.lastIncludedIndex {
				snapshotData, _ := cm.storage.Get("raft_snapshot")
				args := InstallSnapshotArgs{
					Term:              savedTerm,
					LeaderId:          cm.id,
					LastIncludedIndex: cm.lastIncludedIndex,
					LastIncludedTerm:  cm.lastIncludedTerm,
					Data:              snapshotData,
				}
				cm.mu.Unlock()

				var reply InstallSnapshotReply
				if err := cm.server.SendInstallSnapshot(peer, args, &reply); err == nil {
					cm.mu.Lock()
					defer cm.mu.Unlock()
					if reply.Term > cm.currentTerm {
						cm.currentTerm = reply.Term
						cm.state = Follower
						cm.votedFor = -1
						cm.persistToStorage()
						return
					}
					if cm.state == Leader && savedTerm == cm.currentTerm {
						cm.matchIndex[peer] = max(cm.matchIndex[peer], args.LastIncludedIndex)
						cm.nextIndex[peer] = cm.matchIndex[peer] + 1
					}
				}
				return
			}

			prevLogIndex := nextIdx - 1
			prevLogTerm := cm.lastIncludedTerm
			if prevLogIndex > cm.lastIncludedIndex {
				prevLogTerm = cm.log[prevLogIndex-cm.lastIncludedIndex].Term
			}

			entries := make([]LogEntry, len(cm.log[nextIdx-cm.lastIncludedIndex:]))
			copy(entries, cm.log[nextIdx-cm.lastIncludedIndex:])

			args := AppendEntriesArgs{
				Term:         savedTerm,
				LeaderID:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()

			var reply AppendEntriesReply
			err := cm.server.SendAppendEntries(peer, args, &reply)
			if err != nil {
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()

			if reply.Term > cm.currentTerm {
				cm.currentTerm = reply.Term
				cm.state = Follower
				cm.votedFor = -1
				cm.persistToStorage()
				return
			}

			if cm.state != Leader || savedTerm != cm.currentTerm {
				return
			}

			if reply.Success {
				cm.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				cm.nextIndex[peer] = cm.matchIndex[peer] + 1
				cm.advanceCommitIndex()
			} else {
				if reply.ConflictTerm != -1 {
					lastIndexWithTerm := -1
					for i := len(cm.log) - 1; i > 0; i-- {
						if cm.log[i].Term == reply.ConflictTerm {
							lastIndexWithTerm = i
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
		}(peerId)
	}
}

func (cm *ConsensusModule) advanceCommitIndex() {
	for i := cm.lastIncludedIndex + len(cm.log) - 1; i > cm.commitIndex; i-- {
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
			cm.commitIndex = i
			cm.dlog("Advanced CommitIndex to %d", cm.commitIndex)
			cm.applyCond.Broadcast()
			return
		}
	}
}
