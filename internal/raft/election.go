package raft

import (
	"math/rand"
	"time"
)

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.randomTimeout()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !cm.isKilled() {
		<-ticker.C
		cm.mu.Lock()

		if cm.state == Leader || cm.electionTimerPaused {
			cm.mu.Unlock()
			continue
		}
		if time.Since(cm.lastElectionReset) >= timeoutDuration {
			cm.startElection()
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
	cm.persistToStorage()

	cm.dlog("Starting election")
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
					cm.dlog("Term mismatch in RequestVote response. Stepping down to Follower")
					cm.currentTerm = reply.Term
					cm.votedFor = -1
					cm.state = Follower
					return
				}
				if reply.VoteGranted {
					votesReceived++
					cm.dlog("Received vote from %d. Total: %d", peer, votesReceived)
					if votesReceived*2 > len(cm.peerIds)+1 {
						cm.startLeader()
						return
					}
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) randomTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}