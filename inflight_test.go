package main

import (
	"fmt"
	"testing"
)

func TestInflight_StartCommit(t *testing.T) {
	commitCh := make(chan *logFuture, 1)
	in := NewInflight(commitCh)

	l := &logFuture{
		log: Log{Index: 1},
	}

	in.Start(l, 3)
	in.Commit(1)
	select {
	case <-commitCh:
		t.Fatalf("should not be commited")
	default:
	}
	in.Commit(1)
	select {
	case <-commitCh:
		t.Fatalf("should not be commited")
	default:
	}
	in.Commit(1)
	select {
	case <-commitCh:
	default:
		t.Fatalf("should be commited")
	}
}

func TestInflight_Cancel(t *testing.T) {
	commitCh := make(chan *logFuture, 1)
	in := NewInflight(commitCh)

	l := &logFuture{
		log:   Log{},
		errCh: make(chan error, 1),
	}

	in.Start(l, 3)

	err := fmt.Errorf("error 1")
	in.Cancel(err)

	if l.Error() != err {
		t.Fatalf("expected error")
	}
}