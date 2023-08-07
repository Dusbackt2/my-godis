package cluster

import (
	"my-godis/src/interface/redis"
	"my-godis/src/redis/reply"
)

// TODO: support multiplex slots
func Rename(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer := cluster.peerPicker.Get(src)
	destPeer := cluster.peerPicker.Get(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.Relay(srcPeer, c, args)
}

func RenameNx(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'renamenx' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer := cluster.peerPicker.Get(src)
	destPeer := cluster.peerPicker.Get(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.Relay(srcPeer, c, args)
}
