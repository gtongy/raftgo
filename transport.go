package main

type AppendEntriesRequest struct {
	Term         int32
	LeaderID     int64
	PrevLogIndex int32
	PrevLogTerm  int32
	LeaderCommit int32
}
type AppendEntriesResponse struct {
	Term    int32
	Success bool
}
type RequestVoteRequest struct {
	Term         int32
	CandidateID  int32
	LastLogIndex int32
	LastLogTerm  int32
}
type RequestVoteResponse struct {
	Term        int32
	VoteGranted bool
}

type RPC struct {
	Command interface{}
}
