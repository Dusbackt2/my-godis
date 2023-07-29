package db

import (
	"fmt"
	"my-godis/src/datastruct/dict"
	"my-godis/src/datastruct/lock"
	"my-godis/src/interface/redis"
	"my-godis/src/lib/logger"
	"my-godis/src/redis/reply"
	"runtime/debug"
	"strings"
	"sync"
)

const (
	StringCode = iota // Data is []byte
	ListCode          // *list.LinkedList
	SetCode
	DictCode // *dict.Dict
	SortedSetCode
)

type DataEntity struct {
	Code uint8
	TTL  int64 // ttl in seconds, 0 for unlimited ttl
	Data interface{}
	sync.RWMutex
}

type DataEntityWithKey struct {
	DataEntity
	Key string
}

// args don't include cmd line
type CmdFunc func(db *DB, args [][]byte) redis.Reply

type DB struct {
	// key -> DataEntity
	Data *dict.Dict

	// dict will ensure thread safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	Locks *lock.LockMap
}

var cmdMap = MakeCmdMap()

func MakeCmdMap() map[string]CmdFunc {
	cmdMap := make(map[string]CmdFunc)
	cmdMap["ping"] = Ping

	cmdMap["set"] = Set
	cmdMap["setnx"] = SetNX
	cmdMap["setex"] = SetEX
	cmdMap["psetex"] = PSetEX
	cmdMap["mset"] = MSet
	cmdMap["mget"] = MGet
	cmdMap["get"] = Get
	cmdMap["del"] = Del

	return cmdMap
}

func MakeDB() *DB {
	return &DB{
		Data:  dict.Make(1024),
		Locks: &lock.LockMap{},
	}
}

func (db *DB) Exec(args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	cmdFunc, ok := cmdMap[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "'")
	}
	if len(args) > 1 {
		result = cmdFunc(db, args[1:])
	} else {
		result = cmdFunc(db, [][]byte{})
	}
	return
}

func (db *DB) Remove(key string) {
	db.Data.Remove(key)
	db.Locks.Clean(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.Locks.Locks(keys...)
	defer func() {
		db.Locks.UnLocks(keys...)
		db.Locks.Cleans(keys...)
	}()
	deleted = 0
	for _, key := range keys {
		_, exists := db.Data.Get(key)
		if exists {
			db.Data.Remove(key)
			deleted++
		}
	}
	return deleted
}
