package main

import (
	"fmt"
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

func (i *InmemTransport) LocalAddr() net.Addr {
	return i.localAddr
}

// RequestVote implements the Transport interface.
func (i *InmemTransport) RequestVote(target net.Addr, req *RequestVoteRequest, resp *RequestVoteResponse) error {
	i.RLock()
	peer, ok := i.peers[target.String()]
	i.RUnlock()

	if !ok {
		return fmt.Errorf("Failed to connect to peer: %v", target)
	}

	// Send the RPC over
	respCh := make(chan RPCResponse)
	peer.consumerCh <- RPC{
		Command:  req,
		RespChan: respCh,
	}

	// Wait for a response
	rpcResp := <-respCh
	if rpcResp.Error != nil {
		return rpcResp.Error
	}

	// Copy the result back
	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) AppendEntries(target net.Addr, req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	i.RLock()
	peer, ok := i.peers[target.String()]
	i.RUnlock()

	if !ok {
		return fmt.Errorf("Failed to connect to peer: %v", target)
	}

	// Send the RPC over
	respCh := make(chan RPCResponse)
	peer.consumerCh <- RPC{
		Command:  req,
		RespChan: respCh,
	}

	// Wait for a response
	rpcResp := <-respCh
	if rpcResp.Error != nil {
		return rpcResp.Error
	}

	// Copy the result back
	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}