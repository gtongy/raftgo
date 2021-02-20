package main

type LogType uint8

const (
	LogCommand LogType = iota
)

type Log struct {
	Index int64
	Term  int64
	Type  LogType
	Data  []byte
}

type LogStore interface {
	GetLog(index int64, log *Log) error
	DeleteRange(index int64, lastLog int64) error
	StoreLog(*Log) error
}
