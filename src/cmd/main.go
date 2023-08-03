package main

import (
	"fmt"
	"my-godis/src/config"
	"my-godis/src/lib/logger"
	RedisServer "my-godis/src/redis/server"
	"my-godis/src/tcp"
)

func main() {
	config.SetupConfig("redis.conf")
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})

	tcp.ListenAndServe(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
}
