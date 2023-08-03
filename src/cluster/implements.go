package cluster

import (
	"fmt"
	"my-godis/src/interface/redis"
	"my-godis/src/redis/client"
	"my-godis/src/redis/reply"

	"strings"
)

func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmd)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
}

func (cluster *Cluster) getPeerClient(peer string) (*client.Client, error) {
	peerClient, ok := cluster.peers[peer]
	// lazy init
	if !ok {
		var err error
		peerClient, err = client.MakeClient(peer)
		if err != nil {
			return nil, err
		}
		peerClient.Start()
		cluster.peers[peer] = peerClient
	}
	return peerClient, nil
}

// return peer -> keys
func (cluster *Cluster) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		peer := cluster.peerPicker.Get(key)
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}

// relay command to peer
func (cluster *Cluster) Relay(peer string, c redis.Connection, args [][]byte) redis.Reply {
	if peer == cluster.self {
		// to self db
		return cluster.db.Exec(c, args)
	} else {
		peerClient, err := cluster.getPeerClient(peer)
		if err != nil {
			return reply.MakeErrReply(err.Error())
		}
		return peerClient.Send(args)
	}
}

func defaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	key := string(args[1])
	peer := cluster.peerPicker.Get(key)
	return cluster.Relay(peer, c, args)
}

func Ping(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 1 {
		return &reply.PongReply{}
	} else if len(args) == 2 {
		return reply.MakeStatusReply("\"" + string(args[1]) + "\"")
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

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

func MSetNX(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}
	var peer string
	size := argCount / 2
	for i := 0; i < size; i++ {
		key := string(args[2*i])
		currentPeer := cluster.peerPicker.Get(key)
		if peer == "" {
			peer = currentPeer
		} else {
			if peer != currentPeer {
				return reply.MakeErrReply("ERR msetnx must within one slot in cluster mode")
			}
		}
	}
	return cluster.Relay(peer, c, args)
}

// TODO: avoid part failure
func Del(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	failedKeys := make([]string, 0)
	groupMap := cluster.groupBy(keys)
	for peer, group := range groupMap {
		resp := cluster.Relay(peer, c, makeArgs("DEL", group...))
		if reply.IsErrorReply(resp) {
			failedKeys = append(failedKeys, group...)
		}
	}
	if len(failedKeys) > 0 {
		return reply.MakeErrReply("ERR part failure: " + strings.Join(failedKeys, ","))
	}
	return reply.MakeIntReply(int64(len(keys)))
}

func MGet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}

	resultMap := make(map[string][]byte)
	groupMap := cluster.groupBy(keys)
	for peer, group := range groupMap {
		resp := cluster.Relay(peer, c, makeArgs("MGET", group...))
		if reply.IsErrorReply(resp) {
			errReply := resp.(reply.ErrorReply)
			return reply.MakeErrReply(fmt.Sprintf("ERR during get %s occurs: %v", group[0], errReply.Error()))
		}
		arrReply, _ := resp.(*reply.MultiBulkReply)
		for i, v := range arrReply.Args {
			key := group[i]
			resultMap[key] = v
		}
	}
	result := make([][]byte, len(keys))
	for i, k := range keys {
		result[i] = resultMap[k]
	}
	return reply.MakeMultiBulkReply(result)
}

func MSet(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'mset' command")
	}

	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
		valueMap[keys[i]] = string(args[2*i+1])
	}

	failedKeys := make([]string, 0)
	groupMap := cluster.groupBy(keys)
	for peer, groupKeys := range groupMap {
		peerArgs := make([][]byte, 2*len(groupKeys)+1)
		peerArgs[0] = []byte("MSET")
		for i, k := range groupKeys {
			peerArgs[2*i+1] = []byte(k)
			value := valueMap[k]
			peerArgs[2*i+2] = []byte(value)
		}
		resp := cluster.Relay(peer, c, peerArgs)
		if reply.IsErrorReply(resp) {
			failedKeys = append(failedKeys, groupKeys...)
		}
	}
	if len(failedKeys) > 0 {
		return reply.MakeErrReply("ERR part failure: " + strings.Join(failedKeys, ","))
	}
	return &reply.OkReply{}

}
