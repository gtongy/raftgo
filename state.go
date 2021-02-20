package main

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type RaftState int32
