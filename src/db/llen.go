package db

import (
	List "my-godis/src/datastruct/list"
	"my-godis/src/interface/redis"
	"my-godis/src/redis/reply"
)

func LLen(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'llen' command")
	}
	key := string(args[0])

	rawEntity, exists := db.Data.Get(key)
	var entity *DataEntity
	if !exists {
		return reply.MakeIntReply(0)
	} else {
		entity, _ = rawEntity.(*DataEntity)
	}
	entity.RLock()
	defer entity.RUnlock()

	// check type
	if entity.Code != ListCode {
		return &reply.WrongTypeErrReply{}
	}

	list, _ := entity.Data.(*List.LinkedList)
	size := int64(list.Len())
	return reply.MakeIntReply(size)
}
