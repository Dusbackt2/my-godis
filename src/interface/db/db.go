package db

import "my-godis/src/interface/redis"

type DB interface {
	Exec([][]byte) redis.Reply
}

type DataEntity interface {
}
