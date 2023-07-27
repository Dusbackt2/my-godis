package main

import (
	"my-godis/src/lib/logger"
	"my-godis/src/server"
	"time"
)

func main() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2023-07-27",
	})

	server.ListenAndServe(&server.Config{
		Address:    "127.0.0.1:6379",
		MaxConnect: 16,
		Timeout:    2 * time.Second,
	}, server.MakeEchoHandler())
}
