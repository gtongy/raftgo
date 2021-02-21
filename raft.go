package main

import (
	"fmt"
	"log"
	"sync"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

type Raft struct {
	state        RaftState
	config       *Config
	lastLog      uint64
	logs         LogStore
	currentTerm  uint64
	rpcCh        chan RPC
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	stable       StableStore
}

func NewRaft(stable StableStore, logs LogStore) (*Raft, error) {
	lastLog, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("Failed to find last log: %v", err)
	}
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, err
	}
	r := &Raft{
		state:       Follower,
		lastLog:     lastLog,
		config:      DefaultConfig(),
		stable:      stable,
		currentTerm: currentTerm,
		shutdownCh:  make(chan struct{}),
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
				r.followerRequestVote(rpc, cmd)
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
	// TODO: respond
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
func (r *Raft) followerRequestVote(rpc RPC, req *RequestVoteRequest) {
	resp := &RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}
	// TODO: respond
	// old term skip
	if req.Term < r.currentTerm {
		return
	}
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		resp.Term = req.Term
	}

	// check voted
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		log.Printf("fail to get last vote %v", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		log.Printf("fail to get last vote candidate %v", err)
		return
	}
	// check voted in this election before
	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		log.Printf("duplicate RequestVote for same term %d", req.Term)
		if string(lastVoteCandBytes) == req.CandidateID {
			log.Printf("duplicate RequestVote from candidate: %s", req.CandidateID)
			resp.VoteGranted = true
		}
		return
	}
	if r.lastLog > 0 {
		var lastLog Log
		if err := r.logs.GetLog(r.lastLog, &lastLog); err != nil {
			log.Printf("fail to get last log %v", err)
			return
		}
		if lastLog.Term > req.LastLogTerm {
			log.Printf("reject vote since our last term is greater")
			return
		}
		if lastLog.Index > req.LastLogIndex {
			log.Printf("reject vote since our last index is greater")
			return
		}
	}

	// Seems we should grant a vote
	if err := r.stable.SetUint64(keyLastVoteTerm, req.Term); err != nil {
		log.Printf("[ERR] Failed to persist last vote term: %v", err)
		return
	}
	if err := r.stable.Set(keyLastVoteCand, []byte(req.CandidateID)); err != nil {
		log.Printf("[ERR] Failed to persist last vote candidate: %v", err)
		return
	}
	resp.VoteGranted = true
}

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
