package main

import (
	"log"
	"sync"
)

type Raft struct {
	state        RaftState
	config       *Config
	lastLog      int64
	logs         LogStore
	currentTerm  int64
	rpcCh        chan RPC
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func NewRaft() (*Raft, error) {
	r := &Raft{
		state:      Follower,
		config:     DefaultConfig(),
		shutdownCh: make(chan struct{}),
	}
	go r.run()
	return r, nil
}

func (r *Raft) GetState() RaftState {
	return r.state
}

func (r *Raft) run() {
	for {
		select {
		case <-r.shutdownCh:
			return
		default:
		}
		switch r.state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func (r *Raft) runFollower() {
	for {
		log.Print("run follower")
		select {
		case rpc := <-r.rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.followerAppendEntries(rpc, cmd)
				return
			case *RequestVoteRequest:
				return
			default:
				log.Printf("follower unexpected type %#v", rpc.Command)
			}
		case <-randomTimeout(r.config.HeartbeatTimeout):
			// 時間切れでcandidateへstateの変更
			r.state = Candidate
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) followerAppendEntries(rpc RPC, a *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}
	// old term skip
	if a.Term < r.currentTerm {
		return
	}
	if a.Term > r.currentTerm {
		r.currentTerm = a.Term
		resp.Term = a.Term
	}
	var prevLog Log
	if err := r.logs.GetLog(a.PrevLogIndex, &prevLog); err != nil {
		log.Printf("failed to get prev log: %d %v", a.PrevLogIndex, err)
		return
	}
	if a.PrevLogTerm != prevLog.Term {
		log.Printf("prev log term mis match: ours: %d remote: %v", prevLog.Term, a.PrevLogTerm)
		return
	}
	for _, entry := range a.Entries {
		if entry.Index <= r.lastLog {
			log.Printf("clear log suffix from %d to %d", entry.Index, r.lastLog)
			if err := r.logs.DeleteRange(entry.Index, r.lastLog); err != nil {
				log.Printf("fail to clear log")
				return
			}
		}
		if err := r.logs.StoreLog(entry); err != nil {
			log.Printf("fail to append to log: %v", err)
			return
		}
		r.lastLog = entry.Index
	}
}
func (r *Raft) followerRequestVoteRequest(rpc RPC, a *AppendEntriesRequest) {}

func (r *Raft) runCandidate() {
	for {
		log.Print("run candidate")
		select {
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runLeader() {
	for {
		log.Print("run leader")
		select {
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) Shutdown() {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()
	if r.shutdownCh != nil {
		close(r.shutdownCh)
		r.shutdownCh = nil
	}
}
