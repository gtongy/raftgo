package main

import "time"

type Config struct {
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration
	MaxAppendEntries int
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 1000 * time.Millisecond,
		ElectionTimeout:  1000 * time.Millisecond,
		CommitTimeout:    5 * time.Millisecond,
		MaxAppendEntries: 16,
	}
}
