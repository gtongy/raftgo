package main

import "net"

type RPCResponse struct {
	Response interface{}
	Error    error
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*Log
	LeaderCommit uint64
}
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type RPC struct {
	Command  interface{}
	RespChan chan<- RPCResponse
}

func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

type Transport interface {
	Consumer() <-chan RPC
	RequestVote(peer net.Addr, req *RequestVoteRequest, resp *RequestVoteResponse) error
	AppendEntries(peer net.Addr, req *AppendEntriesRequest, resp *AppendEntriesResponse) error
}
