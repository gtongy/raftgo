package main

import (
	"net"
	"sync"
)

// Implements the net.Addr interface
type InmemAddr struct {
	Id string
}

func NewInmemAddr() *InmemAddr {
	return &InmemAddr{generateUUID()}
}


func (ia *InmemAddr) Network() string {
	return "inmem"
}

func (ia *InmemAddr) String() string {
	return ia.Id
}

type InmemTransport struct {
	sync.RWMutex
	consumerCh chan RPC
	localAddr *InmemAddr
	peers map[string]*InmemTransport
}

func NewInmemTransport() (*InmemAddr, *InmemTransport) {
	addr := NewInmemAddr()
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr: addr,
	}
	return addr, trans
}

// Consumer implements the Transport interface.
func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

// RequestVote implements the Transport interface.
func (i *InmemTransport) RequestVote(peer net.Addr, req *RequestVoteRequest, resp *RequestVoteResponse) error {
	// TODO
	return nil
}

func (i *InmemTransport) AppendEntries(peer net.Addr, req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	// TODO
	return nil
}