package main

type LogType uint8

const (
	LogCommand LogType = iota
	// Noop is used to assert leadership
	LogNoop
)

type Log struct {
	Index uint64
	Term  uint64
	Type  LogType
	Data  []byte
}

type LogStore interface {
	GetLog(index uint64, log *Log) error
	DeleteRange(index uint64, lastLog uint64) error
	StoreLog(*Log) error
	LastIndex() (uint64, error)
}
