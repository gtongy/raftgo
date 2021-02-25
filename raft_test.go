package main

import (
	"os"
	"testing"
	"time"
)

type MockFSM struct {
	logs [][]byte
}

func (m *MockFSM) Apply(log []byte) {
	m.logs = append(m.logs, log)
}

func inmemConfig() *Config {
	return &Config{
		HeartbeatTimeout: 500 * time.Millisecond,
		ElectionTimeout:  500 * time.Millisecond,
		CommitTimeout:    50 * time.Millisecond,
		MaxAppendEntries: 16,
	}
}

func TestRaft_SingleNode(t *testing.T) {
	dir, store := LevelDBTestStore(t)
	defer os.RemoveAll(dir)
	defer store.Close()

	_, trans := NewInmemTransport()
	fsm := &MockFSM{}
	conf := inmemConfig()

	raft, err := NewRaft(conf, fsm, store, store, nil, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Watch leaderCh for change
	select {
	case v := <-raft.LeaderCh():
		if !v {
			t.Fatalf("should become leader")
		}
	case <-time.After(conf.HeartbeatTimeout * 3):
		t.Fatalf("timeout becoming leader")
	}

	t.Log(raft.State())
	// Should be leader
	if s := raft.State(); s != Leader {
		t.Fatalf("expected leader: %v", s)
	}
}
