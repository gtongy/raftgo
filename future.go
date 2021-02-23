package main

type ApplyFuture interface {
	Error() error
}

type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

type logFuture struct {
	log   Log
	err   error
	errCh chan error
}

func (l *logFuture) Error() error {
	if l.err != nil {
		return l.err
	}
	l.err = <-l.errCh
	return l.err
}

func (l *logFuture) respond(err error) {
	l.errCh <- err
	close(l.errCh)
}