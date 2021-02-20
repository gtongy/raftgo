package main

import "fmt"

type Raft struct {
	state  RaftState
	config *Config
}

func NewRaft() (*Raft, error) {
	r := &Raft{
		state:  Follower,
		config: DefaultConfig(),
	}
	r.run()
	return r, nil
}

func (r *Raft) run() {
	switch r.state {
	case Follower:
		r.runFollower()
	case Candidate:
		r.runCandidate()
	case Leader:
		r.runLeader()
	}
}

func (r *Raft) runFollower() {
	fmt.Println("run follower")
	// follower logic
}

func (r *Raft) runCandidate() {
	fmt.Println("run candidate")
	// candidate logic
}

func (r *Raft) runLeader() {
	fmt.Println("run leader")
	// leader logic
}
