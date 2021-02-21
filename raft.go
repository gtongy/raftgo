package main

import (
	"fmt"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
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
	peers        []net.Addr
	trans        Transport
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	stable       StableStore
}

func NewRaft(stable StableStore, logs LogStore, trans Transport) (*Raft, error) {
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
		config:      DefaultConfig(),
		lastLog:     lastLog,
		logs:        logs,
		currentTerm: currentTerm,
		peers:       make([]net.Addr, 0, 5),
		trans:       trans,
		shutdownCh:  make(chan struct{}),
		stable:      stable,
	}
	go r.run()
	return r, nil
}

func (r *Raft) GetState() RaftState {
	return r.state
}

func (r *Raft) run() {
	ch := r.trans.Consumer()
	for {
		select {
		case <-r.shutdownCh:
			return
		default:
		}
		switch r.state {
		case Follower:
			r.runFollower(ch)
		case Candidate:
			r.runCandidate(ch)
		case Leader:
			r.runLeader()
		}
	}
}

func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}
	var rpcErr error
	defer rpc.Respond(resp, rpcErr)
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
		logrus.Printf("failed to get prev log: %d %v", a.PrevLogIndex, err)
		return
	}
	if a.PrevLogTerm != prevLog.Term {
		logrus.Printf("prev log term mis match: ours: %d remote: %v", prevLog.Term, a.PrevLogTerm)
		return
	}
	for _, entry := range a.Entries {
		if entry.Index <= r.lastLog {
			logrus.Printf("clear log suffix from %d to %d", entry.Index, r.lastLog)
			if err := r.logs.DeleteRange(entry.Index, r.lastLog); err != nil {
				logrus.Printf("fail to clear log")
				return
			}
		}
		if err := r.logs.StoreLog(entry); err != nil {
			logrus.Printf("fail to append to log: %v", err)
			return
		}
		r.lastLog = entry.Index
	}
}

func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	resp := &RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}
	var rpcErr error
	defer rpc.Respond(resp, rpcErr)
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
		logrus.Printf("fail to get last vote %v", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		logrus.Printf("fail to get last vote candidate %v", err)
		return
	}
	// check voted in this election before
	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		logrus.Printf("duplicate RequestVote for same term %d", req.Term)
		if string(lastVoteCandBytes) == req.CandidateID {
			logrus.Printf("duplicate RequestVote from candidate: %s", req.CandidateID)
			resp.VoteGranted = true
		}
		return
	}
	if r.lastLog > 0 {
		var lastLog Log
		if err := r.logs.GetLog(r.lastLog, &lastLog); err != nil {
			logrus.Printf("fail to get last log %v", err)
			return
		}
		if lastLog.Term > req.LastLogTerm {
			logrus.Printf("reject vote since our last term is greater")
			return
		}
		if lastLog.Index > req.LastLogIndex {
			logrus.Printf("reject vote since our last index is greater")
			return
		}
	}

	// Seems we should grant a vote
	if err := r.stable.SetUint64(keyLastVoteTerm, req.Term); err != nil {
		logrus.Printf("[ERR] Failed to persist last vote term: %v", err)
		return
	}
	if err := r.stable.Set(keyLastVoteCand, []byte(req.CandidateID)); err != nil {
		logrus.Printf("[ERR] Failed to persist last vote candidate: %v", err)
		return
	}
	resp.VoteGranted = true
}

func (r *Raft) runFollower(ch <-chan RPC) {
	for {
		logrus.Print("run follower")
		select {
		case rpc := <-ch:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.appendEntries(rpc, cmd)
				return
			case *RequestVoteRequest:
				r.requestVote(rpc, cmd)
				return
			default:
				logrus.Printf("follower unexpected type %#v", rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}
		case <-randomTimeout(r.config.HeartbeatTimeout):
			// 時間切れでcandidateへstateの変更
			r.state = Candidate
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runCandidate(ch <-chan RPC) {
	logrus.Print("run candidate")
	voteCh := r.electSelf()
	electionTimer := randomTimeout(r.config.ElectionTimeout)
	grantedVotes := 0
	clusterSize := len(r.peers) + 1
	votesNeeded := (clusterSize >> 1) + 1
	logrus.Printf("cluster size: %d, votes needed: %d", clusterSize, votesNeeded)
	transition := false
	for !transition {
		select {
		case rpc := <-ch:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.appendEntries(rpc, cmd)
				return
			case *RequestVoteRequest:
				r.requestVote(rpc, cmd)
				return
			default:
				logrus.Printf("follower unexpected type %#v", rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}
		case vote := <-voteCh:
			// termの確認
			if vote.Term > r.getCurrentTerm() {
				logrus.Printf("newer term discovered")
				r.state = Follower
				if err := r.setCurrentTerm(vote.Term); err != nil {
					logrus.Printf("fail to update current term: %v", err)
				}
				return
			}
			// grantedな投票の票数確認
			if vote.VoteGranted {
				grantedVotes++
				logrus.Printf("vote granted. tally: %d", grantedVotes)
			}

			// 過半数表を得ているか。leaderになり得るか
			if grantedVotes >= votesNeeded {
				logrus.Printf("election won. tally: %d", grantedVotes)
				r.state = Leader
			}
		case <-electionTimer:
			logrus.Printf("election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runLeader() {
	for {
		logrus.Print("run leader")
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

// send RequestVote RPC to all peers, and vote for ourself
func (r *Raft) electSelf() <-chan *RequestVoteResponse {
	respCh := make(chan *RequestVoteResponse, len(r.peers)+1)
	var lastLog Log
	if r.lastLog > 0 {
		if err := r.logs.GetLog(r.lastLog, &lastLog); err != nil {
			logrus.Printf("failed to get last log: %d %v", r.lastLog, err)
			return nil
		}
	}
	// increment term
	if err := r.setCurrentTerm(r.currentTerm + 1); err != nil {
		logrus.Printf("fail to get last log: %d %v", r.lastLog, err)
		return nil
	}
	req := &RequestVoteRequest{
		Term:         r.getCurrentTerm(),
		CandidateID:  r.CandidateID(),
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	askPeer := func(peer net.Addr) {
		resp := new(RequestVoteResponse)
		err := r.trans.RequestVote(peer, req, resp)
		if err != nil {
			logrus.Printf("failed to make RequestVote RPC to %v: %v", peer, err)
			resp.Term = req.Term
			resp.VoteGranted = false
		}
		respCh <- resp
	}
	for _, peer := range r.peers {
		go askPeer(peer)
	}

	respCh <- &RequestVoteResponse{Term: req.Term, VoteGranted: true}
	return respCh
}

func (r *Raft) setCurrentTerm(t uint64) error {
	r.currentTerm = t
	return nil
}

func (r *Raft) getCurrentTerm() uint64 {
	return r.currentTerm
}

func (r *Raft) CandidateID() string {
	// TODO
	return "uuid"
}
