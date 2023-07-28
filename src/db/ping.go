package db

import (
	"my-godis/src/interface/redis"
	"my-godis/src/redis/reply"
)

func Ping(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply("\"" + string(args[0]) + "\"")
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}
