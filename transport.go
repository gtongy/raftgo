package main

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []*Log
	LeaderCommit int64
}
type AppendEntriesResponse struct {
	Term    int64
	Success bool
}
type RequestVoteRequest struct {
	Term         int64
	CandidateID  int64
	LastLogIndex int64
	LastLogTerm  int64
}
type RequestVoteResponse struct {
	Term        int64
	VoteGranted bool
}

type RPC struct {
	Command interface{}
}
