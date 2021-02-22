package main

import (
	"sync/atomic"
)

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type RaftState uint32

type StableStore interface {
	Set(key []byte, val []byte) error
	Get(key []byte) ([]byte, error)

	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
}

type raftState struct {
	state           RaftState
	currentTerm     uint64
	lastLog         uint64
	commitIndex     uint64
	lastApplied     uint64
}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getLastLog() uint64 {
	return atomic.LoadUint64(&r.lastLog)
}

func (r *raftState) setLastLog(lastLog uint64) {
	atomic.StoreUint64(&r.lastLog, lastLog)
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(commitIndex uint64) {
	atomic.StoreUint64(&r.commitIndex, commitIndex)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(lastApplied uint64) {
	atomic.StoreUint64(&r.lastApplied, lastApplied)
}
