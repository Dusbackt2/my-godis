package db

import "my-godis/src/interface/redis"

type DB interface {
	Exec(client redis.Connection, args [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}
