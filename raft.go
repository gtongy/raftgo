package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
	keyCandidateId  = []byte("CandidateId")
)

type commitTuple struct {
	index  uint64
	future *logFuture
}

type Raft struct {
	raftState
	// manage commands to be applied
	applyCh      chan *logFuture
	config       *Config
	lastLog      uint64
	logs         LogStore
	currentTerm  uint64
	peers        []net.Addr
	rpcCh        <-chan RPC
	trans        Transport
	shutdownCh   chan struct{}
	leaderCh     chan bool
	fsm          FSM
	commitCh     chan commitTuple
	commitIndex  uint64
	shutdownLock sync.Mutex
	stable       StableStore
}

func (r *Raft) State() RaftState {
	return r.getState()
}

func (r *Raft) LeaderCh() <-chan bool {
	return r.leaderCh
}

func NewRaft(config *Config, fsm FSM, logs LogStore, stable StableStore, peers []net.Addr, trans Transport) (*Raft, error) {
	lastLog, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("Failed to find last log: %v", err)
	}
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, err
	}
	localAddr := trans.LocalAddr()
	otherPeers := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		if p.String() != localAddr.String() {
			otherPeers = append(otherPeers, p)
		}
	}
	r := &Raft{
		applyCh:     make(chan *logFuture),
		config:      config,
		lastLog:     lastLog,
		logs:        logs,
		peers:       otherPeers,
		rpcCh:       trans.Consumer(),
		trans:       trans,
		shutdownCh:  make(chan struct{}),
		fsm:         fsm,
		commitCh:    make(chan commitTuple, 128),
		commitIndex: 0,
		leaderCh:    make(chan bool, 1),
		stable:      stable,
	}
	r.setState(Follower)
	r.setCurrentTerm(currentTerm)
	r.setLastLog(lastLog)
	go r.run()
	go r.runFSM()
	return r, nil
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	logFuture := &logFuture{
		log: Log{
			Type: LogCommand,
			Data: cmd,
		},
		errCh: make(chan error, 1),
	}
	select {
	case <-timer:
		return errorFuture{fmt.Errorf("timeout enqueuing operation")}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

func (r *Raft) run() {
	for {
		select {
		case <-r.shutdownCh:
			return
		default:
		}
		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
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

func (r *Raft) runFollower() {
	logrus.Print("run follower")
	for {
		select {
		case rpc := <-r.rpcCh:
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
			logrus.Info("heartbeat timeout reached, starting election")
			// 時間切れでcandidateへstateの変更
			r.setState(Candidate)
			return
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) quorumSize() int {
	clusterSize := len(r.peers) + 1
	// 過半数以上獲得が必要
	votesNeeded := (clusterSize / 2) + 1
	return votesNeeded
}

func (r *Raft) runCandidate() {
	logrus.Print("run candidate")
	voteCh := r.electSelf()
	electionTimer := randomTimeout(r.config.ElectionTimeout)
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	transition := false
	for !transition {
		select {
		case rpc := <-r.rpcCh:
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
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
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
				r.setState(Leader)
				return
			}
		case a := <-r.applyCh:
			a.respond(fmt.Errorf("not leader"))
		case <-electionTimer:
			logrus.Printf("election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runLeader() {
	stopCh := make(chan struct{})
	defer close(stopCh)
	commitCh := make(chan *logFuture)
	inflight := NewInflight(commitCh)
	triggers := make([]chan struct{}, 0, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		triggers = append(triggers, make(chan struct{}))
	}

	for i, peer := range r.peers {
		go r.replicate(inflight, triggers[i], stopCh, peer)
	}

	go r.applyNoop()

	logrus.Print("run leader")
	asyncNotifyBool(r.leaderCh, true)
	defer func() {
		asyncNotifyBool(r.leaderCh, false)
	}()
	r.leaderLoop(inflight, commitCh, r.rpcCh, triggers)
}

func (r *Raft) leaderLoop(inflight *inflight, commitCh <-chan *logFuture, rpcCh <-chan RPC, triggers []chan struct{}) {
	for {
		select {
		case applyLog := <-r.applyCh:
			applyLog.log.Index = r.lastLog + 1
			applyLog.log.Term = r.getCurrentTerm()
			if err := r.logs.StoreLog(&applyLog.log); err != nil {
				logrus.Printf("fail to commit log: %v", err)
				applyLog.respond(err)
				r.setState(Follower)
				return
			}

			inflight.Start(applyLog, r.quorumSize())
			inflight.Commit(applyLog.log.Index)
			r.setLastLog(applyLog.log.Index)
			asyncNotify(triggers)
		case commitLog := <-commitCh:
			r.commitIndex = commitLog.log.Index
			r.commitCh <- commitTuple{
				index:  commitLog.log.Index,
				future: commitLog,
			}
		case rpc := <-r.rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				r.requestVote(rpc, cmd)
			default:
				log.Printf("leader state, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) applyNoop() {
	logFuture := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	r.applyCh <- logFuture
}

type followerReplication struct {
	matchIndex uint64
	nextIndex  uint64
}

func (r *Raft) replicate(inflight *inflight, triggerCh, stopCh chan struct{}, peer net.Addr) {
	last := r.getLastLog()
	indexes := followerReplication{
		matchIndex: last,
		nextIndex:  last + 1,
	}
	shouldStop := false
	for !shouldStop {
		select {
		case <-triggerCh:
			shouldStop = r.replicateTo(inflight, &indexes, r.getLastLog(), peer)
		case <-randomTimeout(r.config.CommitTimeout):
			shouldStop = r.replicateTo(inflight, &indexes, r.getLastLog(), peer)
		case <-stopCh:
			return
		}
	}
}

func (r *Raft) replicateTo(inflight *inflight, indexes *followerReplication, lastIndex uint64, peer net.Addr) (shouldStop bool) {
	var l Log
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
START:
	req = AppendEntriesRequest{
		Term:         r.getCurrentTerm(),
		LeaderID:     r.CandidateID(),
		LeaderCommit: 0,
	}

	// get previous log entry based on the next index
	if indexes.nextIndex > 1 {
		if err := r.logs.GetLog(indexes.nextIndex-1, &l); err != nil {
			logrus.Printf("fail to get log at index %d: %v", indexes.nextIndex-1, err)
			return
		}
	}
	req.PrevLogIndex = l.Index
	req.PrevLogTerm = l.Term

	// Append up to MaxAppendEntries or up to the lastIndex
	req.Entries = make([]*Log, 0, 16)
	// NOTE: digging
	maxIndex := min(indexes.nextIndex+uint64(r.config.MaxAppendEntries)-1, lastIndex)
	for i := indexes.nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			logrus.Printf("fail to get log at index %d: %v", i, err)
			return
		}
		req.Entries = append(req.Entries, oldLog)
	}

	// make rpc call
	if err := r.trans.AppendEntries(peer, &req, &resp); err != nil {
		logrus.Printf("fail to AppendEntries to %v: %v", peer, err)
		return
	}
	// check newer term
	if resp.Term > req.Term {
		return true
	}
	if resp.Success {
		for i := indexes.matchIndex; i <= maxIndex; i++ {
			inflight.Commit(i)
		}
		indexes.matchIndex = maxIndex
		indexes.nextIndex = maxIndex + 1
	} else {
		logrus.Printf("AppendEntries to %v rejected, sending older logs", peer)
		indexes.nextIndex--
		indexes.matchIndex = indexes.nextIndex - 1
	}

	// check if there are more logs to replicate
	if indexes.nextIndex <= lastIndex {
		goto START
	}
	return
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
	if r.getLastLog() > 0 {
		if err := r.logs.GetLog(r.getLastLog(), &lastLog); err != nil {
			logrus.Printf("failed to get last log: %d %v", r.lastLog, err)
			return nil
		}
	}
	// increment term
	r.setCurrentTerm(r.getCurrentTerm() + 1)
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

	if err := r.persistVote(req.Term, req.CandidateID); err != nil {
		logrus.Printf("fail to persist vote: %v", err)
	}

	respCh <- &RequestVoteResponse{Term: req.Term, VoteGranted: true}
	return respCh
}

func (r *Raft) persistVote(term uint64, candidateID string) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, []byte(candidateID)); err != nil {
		return err
	}
	return nil
}

func (r *Raft) runFSM() {
	for {
		select {
		case commitTuple := <-r.commitCh:
			var l *Log
			if commitTuple.future != nil {
				l = &commitTuple.future.log
			} else {
				l = new(Log)
				if err := r.logs.GetLog(commitTuple.index, l); err != nil {
					logrus.Printf("fail to get log: %v", err)
					panic(err)
				}
			}
			if l.Type == LogCommand {
				r.fsm.Apply(l.Data)
			}
			// invoke the future if given
			if commitTuple.future != nil {
				commitTuple.future.respond(nil)
			}
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) CandidateID() string {
	// Get the persistent id
	raw, err := r.stable.Get(keyCandidateId)
	if err == nil {
		return string(raw)
	}

	// Generate a UUID on the first call
	if err != nil && err.Error() == "not found" {
		id := generateUUID()
		if err := r.stable.Set(keyCandidateId, []byte(id)); err != nil {
			panic(fmt.Errorf("Failed to write CandidateId: %v", err))
		}
		return id
	}
	panic(fmt.Errorf("Failed to read CandidateId: %v", err))
}
