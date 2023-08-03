package tcp

import (
	"context"
	"fmt"
	"my-godis/src/interface/tcp"
	"my-godis/src/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"my-godis/src/lib/sync/atomic"
)

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServe(cfg *Config, handler tcp.Handler) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		logger.Fatal(fmt.Sprintf("listen err: %v", err))
	}

	// listen signal
	var closing atomic.AtomicBool
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			logger.Info("shuting down...")
			closing.Set(true)
			_ = listener.Close() // listener.Accept() will return err immediately
			_ = handler.Close()  // close connections
		}
	}()

	// listen port
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	defer func() {
		// close during unexpected error
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx, _ := context.WithCancel(context.Background())
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				logger.Info("waiting disconnect...")
				waitDone.Wait()
				return // handler will be closed by defer
			}
			logger.Error(fmt.Sprintf("accept err: %v", err))
			continue
		}
		// handle
		logger.Info("accept link")
		go func() {
			defer func() {
				waitDone.Done()
			}()
			waitDone.Add(1)
			handler.Handle(ctx, conn)
		}()
	}
}
