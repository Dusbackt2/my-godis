package db

import (
	"fmt"
	"my-godis/src/datastruct/dict"
	List "my-godis/src/datastruct/list"
	"my-godis/src/datastruct/lock"
	"my-godis/src/interface/redis"
	"my-godis/src/lib/logger"
	"my-godis/src/redis/reply"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type DataEntity struct {
	Data interface{}
}

const (
	dataDictSize = 2 << 20
	ttlDictSize  = 2 << 10
	lockerSize   = 128
)

// args don't include cmd line
type CmdFunc func(db *DB, args [][]byte) redis.Reply

type DB struct {
	// key -> DataEntity
	Data *dict.Dict
	// key -> expireTime (time.Time)
	TTLMap *dict.Dict
	// channel -> list<*client>
	SubMap *dict.Dict

	// dict will ensure thread safety of its method
	// use this mutex for complicated command only, eg. rpush, incr ...
	Locker *lock.Locks

	// TimerTask interval
	interval time.Duration

	// channel -> list(*Client)
	subs *dict.Dict
	// lock channel
	subsLocker *lock.Locks

	stopWorld sync.RWMutex
}

var router = MakeRouter()

func MakeDB() *DB {
	db := &DB{
		Data:     dict.Make(dataDictSize),
		TTLMap:   dict.Make(ttlDictSize),
		Locker:   lock.Make(lockerSize),
		interval: 5 * time.Second,

		subs:       dict.Make(4),
		subsLocker: lock.Make(16),
	}
	db.TimerTask()
	return db
}

func (db *DB) Exec(c redis.Client, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))

	if cmd == "subscribe" {
		if len(args) < 2 {
			return &reply.ArgNumErrReply{Cmd: "subscribe"}
		}
		return Subscribe(db, c, args[1:])
	} else if cmd == "unsubscribe" {
		return UnSubscribe(db, c, args[1:])
	}

	cmdFunc, ok := router[cmd]
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

/* ---- Data Access ----- */

func (db *DB) Get(key string) (*DataEntity, bool) {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()

	raw, ok := db.Data.Get(key)
	if !ok {
		return nil, false
	}
	if db.IsExpired(key) {
		return nil, false
	}
	entity, _ := raw.(*DataEntity)
	return entity, true
}

func (db *DB) Put(key string, entity *DataEntity) int {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	return db.Data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *DataEntity) int {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	return db.Data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *DataEntity) int {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	return db.Data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	db.Data.Remove(key)
	db.TTLMap.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	deleted = 0
	for _, key := range keys {
		_, exists := db.Data.Get(key)
		if exists {
			db.Data.Remove(key)
			db.TTLMap.Remove(key)
			deleted++
		}
	}
	return deleted
}

func (db *DB) Flush() {
	db.stopWorld.Lock()
	defer db.stopWorld.Unlock()

	db.Data = dict.Make(dataDictSize)
	db.TTLMap = dict.Make(ttlDictSize)
	db.Locker = lock.Make(lockerSize)
}

/* ---- Lock Function ----- */

func (db *DB) Lock(key string) {
	db.Locker.Lock(key)
}

func (db *DB) RLock(key string) {
	db.Locker.RLock(key)
}

func (db *DB) UnLock(key string) {
	db.Locker.UnLock(key)
}

func (db *DB) RUnLock(key string) {
	db.Locker.RUnLock(key)
}

func (db *DB) Locks(keys ...string) {
	db.Locker.Locks(keys...)
}

func (db *DB) RLocks(keys ...string) {
	db.Locker.RLocks(keys...)
}

func (db *DB) UnLocks(keys ...string) {
	db.Locker.UnLocks(keys...)
}

func (db *DB) RUnLocks(keys ...string) {
	db.Locker.RUnLocks(keys...)
}

/* ---- TTL Functions ---- */

func (db *DB) Expire(key string, expireTime time.Time) {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	db.TTLMap.Put(key, expireTime)
}

func (db *DB) Persist(key string) {
	db.stopWorld.RLock()
	defer db.stopWorld.RUnlock()
	db.TTLMap.Remove(key)
}

func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.TTLMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

func (db *DB) CleanExpired() {
	now := time.Now()
	toRemove := &List.LinkedList{}
	db.TTLMap.ForEach(func(key string, val interface{}) bool {
		expireTime, _ := val.(time.Time)
		if now.After(expireTime) {
			// expired
			db.Data.Remove(key)
			toRemove.Add(key)
		}
		return true
	})
	toRemove.ForEach(func(i int, val interface{}) bool {
		key, _ := val.(string)
		db.TTLMap.Remove(key)
		return true
	})
}

func (db *DB) TimerTask() {
	ticker := time.NewTicker(db.interval)
	go func() {
		for range ticker.C {
			db.CleanExpired()
		}
	}()
}

/* ---- Subscribe Functions ---- */

func (db *DB) AfterClientClose(c redis.Client) {
	unsubscribeAll(db, c)
}
