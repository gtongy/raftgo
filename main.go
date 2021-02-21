package main

import "time"

func main() {
	store, err := NewLevelDBStore(".")
	if err != nil {
		panic(err)
	}
	r, err := NewRaft(store, store)
	if err != nil {
		panic(err)
	}
	defer r.Shutdown()
	time.Sleep(2000 * time.Millisecond)
}
