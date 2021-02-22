package main

// client use of replicated log
type FSM interface {
	Apply([]byte)
}