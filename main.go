package main

import "time"

func main() {
	store, err := NewLevelDBStore(".")
	if err != nil {
		panic(err)
	}
	trans := NewInmemTransport()
	r, err := NewRaft(store, store, trans)
	if err != nil {
		panic(err)
	}
	defer r.Shutdown()
	time.Sleep(1000 * time.Millisecond)
}
