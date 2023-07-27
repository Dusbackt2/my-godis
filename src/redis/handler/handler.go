package handler

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"bufio"
	"context"
	"io"
	DBImpl "my-godis/src/db"
	"my-godis/src/interface/db"
	"my-godis/src/lib/logger"
	"my-godis/src/lib/sync/atomic"
	"my-godis/src/redis/parser"
	"net"
	"strconv"
	"sync"
)

var (
	UnknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         db.DB
	closing    atomic.AtomicBool // refusing new client and new request
}

func MakeHandler() *Handler {
	return &Handler{
		db: DBImpl.MakeDB(),
	}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		conn.Close()
	}

	client := &Client{
		conn: conn,
	}
	h.activeConn.Store(client, 1)

	reader := bufio.NewReader(conn)
	for {
		// may occurs: client EOF, client timeout, server early close
		msg, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
			} else {
				logger.Warn(err)
			}
			client.Close()
			h.activeConn.Delete(client)
			return // io error, disconnect with client
		}

		if len(msg) == 0 {
			continue // ignore empty request
		}

		if !client.sending.Get() {
			// new request
			if msg[0] == '*' {
				// bulk multi msg
				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					client.conn.Write(UnknownErrReplyBytes)
					continue
				}
				expectedLine *= 2
				client.waitingReply.Add(1)
				client.sending.Set(true)
				client.expectedLineCount = uint32(expectedLine)
				client.sentLineCount = 0
				client.sentLines = make([][]byte, expectedLine)
			} else {
				// TODO: text protocol
			}
		} else {
			// receive following part of a request
			client.sentLines[client.sentLineCount] = msg[0 : len(msg)-2]
			client.sentLineCount++
			// if sending finished
			if client.sentLineCount == client.expectedLineCount {
				client.sending.Set(false) // finish sending progress
				// exec cmd
				if len(client.sentLines)%2 != 0 {
					client.conn.Write(UnknownErrReplyBytes)
					client.expectedLineCount = 0
					client.sentLineCount = 0
					client.sentLines = nil
					client.waitingReply.Done()
					continue
				}

				// send reply
				args := parser.Parse(client.sentLines)
				result := h.db.Exec(args)
				if result != nil {
					conn.Write(result.ToBytes())
				} else {
					conn.Write(UnknownErrReplyBytes)
				}

				// finish reply
				client.expectedLineCount = 0
				client.sentLineCount = 0
				client.sentLines = nil
				client.waitingReply.Done()
			}
		}

	}
}

func (h *Handler) Close() error {
	logger.Info("handler shuting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	return nil
}
