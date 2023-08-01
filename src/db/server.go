package db

import (
	"my-godis/src/interface/redis"
	"my-godis/src/redis/reply"
)

func Ping(db *DB, args [][]byte) (redis.Reply, *extra) {
	if len(args) == 0 {
		return &reply.PongReply{}, nil
	} else if len(args) == 1 {
		return reply.MakeStatusReply("\"" + string(args[0]) + "\""), nil
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command"), nil
	}
}
