package main

import "time"

func main() {
	r, err := NewRaft()
	if err != nil {
		panic(err)
	}
	defer r.Shutdown()
	time.Sleep(1000 * time.Millisecond)
}
