package main

import "time"

type Config struct {
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 1000 * time.Millisecond,
		ElectionTimeout:  1000 * time.Millisecond,
	}
}
