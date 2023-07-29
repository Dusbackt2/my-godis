package db

import (
	"my-godis/src/interface/redis"
	"my-godis/src/redis/reply"
)

func Del(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}
