package main

import "sync"

type inflight struct {
	sync.Mutex
	commitCh   chan *logFuture
	operations map[uint64]*inflightLog
}

type inflightLog struct {
	future      *logFuture
	commitCount int
	quorum      int
	committed   bool
}

func NewInflight(commitCh chan *logFuture) *inflight {
	return &inflight{
		commitCh:   commitCh,
		operations: make(map[uint64]*inflightLog),
	}
}

func (i *inflight) Start(l *logFuture, quorum int) {
	i.Lock()
	defer i.Unlock()
	op := &inflightLog{
		future:      l,
		commitCount: 0,
		quorum:      quorum,
		committed:   false,
	}
	i.operations[l.log.Index] = op
}

func (i *inflight) Commit(index uint64) {
	i.Lock()
	defer i.Unlock()

	op, ok := i.operations[index]
	if !ok {
		// ignore if not exists
		return
	}

	op.commitCount++

	// check if we have commit this
	if op.commitCount < op.quorum {
		return
	}

	if !op.committed {
		i.commitCh <- op.future
		op.committed = true
	}
}

func (i *inflight) Cancel(err error) {
	i.Lock()
	defer i.Unlock()

	for _, op := range i.operations {
		op.future.respond(err)
	}

	i.operations = make(map[uint64]*inflightLog)
}

func (i *inflight) Apply(index uint64) {
	i.Lock()
	defer i.Unlock()

	op, ok := i.operations[index]
	if !ok {
		return
	}
	if !op.committed {
		panic("operation is not yet committed")
	}
	op.future.respond(nil)
	delete(i.operations, index)
}
