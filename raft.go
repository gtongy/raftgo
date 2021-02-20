package main

import (
	"log"
	"sync"
)

type Raft struct {
	state        RaftState
	config       *Config
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
		case <-r.shutdownCh:
			return
		}
	}
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
