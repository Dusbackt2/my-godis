package handler

import (
	"my-godis/src/lib/sync/wait"
	"net"
	"time"

	"my-godis/src/lib/sync/atomic"
)

type Client struct {
	conn net.Conn

	// waiting util reply finished
	waitingReply wait.Wait

	// is sending request in progress
	sending atomic.AtomicBool
	// multi bulk msg lineCount - 1(first line)
	expectedLineCount uint32
	// sent line count, exclude first line
	sentLineCount uint32
	// sent lines, exclude first line
	sentLines [][]byte
}

func (c *Client) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	c.conn.Close()
	return nil
}

func MakeClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}
