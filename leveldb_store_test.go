package main

import (
	"io/ioutil"
	"testing"
)

func LevelDBTestStore(t *testing.T) (string, *LevelDBStore) {
	// Create a test dir
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	// New level
	l, err := NewLevelDBStore(dir)
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	return dir, l
}