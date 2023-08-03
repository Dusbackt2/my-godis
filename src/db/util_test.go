package db

import (
	"my-godis/src/datastruct/dict"
	"my-godis/src/datastruct/lock"
	"time"
)

func makeTestDB() *DB {
	return &DB{
		Data:     dict.MakeConcurrent(1),
		TTLMap:   dict.MakeConcurrent(ttlDictSize),
		Locker:   lock.Make(lockerSize),
		interval: 5 * time.Second,
	}
}

func toArgs(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}
