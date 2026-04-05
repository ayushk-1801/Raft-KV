package raft

import (
	"math/rand"
	"time"
)

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.randomTimeout()
	cm.mu.Lock()
	lastSeen := cm.lastElectionReset
	cm.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !cm.isKilled() {
		<-ticker.C
		cm.mu.Lock()

		if cm.state == Leader || cm.electionTimerPaused {
			cm.mu.Unlock()
			continue
		}

		if cm.lastElectionReset != lastSeen {
			lastSeen = cm.lastElectionReset
			timeoutDuration = cm.randomTimeout()
			cm.mu.Unlock()
			continue
		}

		if time.Since(lastSeen) >= timeoutDuration {
			cm.startElection()
			lastSeen = cm.lastElectionReset
			timeoutDuration = cm.randomTimeout()
		}

		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm++
	savedCurrentTerm := cm.currentTerm
	cm.votedFor = cm.id
	cm.leaderId = -1
	cm.lastElectionReset = time.Now()
	if err := cm.persistMeta(); err != nil {
		return
	}

	cm.dlog("Starting election for term %d", savedCurrentTerm)
	votesReceived := 1

	args := RequestVoteArgs{
		Term:         savedCurrentTerm,
		CandidateID:  cm.id,
		LastLogIndex: cm.lastIncludedIndex + len(cm.log) - 1,
		LastLogTerm:  cm.log[len(cm.log)-1].Term,
	}

	for _, peerId := range cm.peerIds {
		go func(peer int) {
			var reply RequestVoteReply
			if err := cm.server.SendRequestVote(peer, args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if cm.state != Candidate || cm.currentTerm != savedCurrentTerm {
					return
				}
				if reply.Term > savedCurrentTerm {
					_ = cm.becomeFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					votesReceived++
					if votesReceived*2 > len(cm.peerIds)+1 {
						cm.startLeader()
					}
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}
