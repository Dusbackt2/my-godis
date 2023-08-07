package cluster

import (
	"fmt"
	"my-godis/src/cluster/idgenerator"
	"my-godis/src/config"
	"my-godis/src/datastruct/dict"
	"my-godis/src/db"
	"my-godis/src/interface/redis"
	"my-godis/src/lib/consistenthash"
	"my-godis/src/lib/logger"
	"my-godis/src/redis/client"
	"my-godis/src/redis/reply"

	"runtime/debug"
	"strings"
)

type Cluster struct {
	self string

	peerPicker *consistenthash.Map
	peers      map[string]*client.Client

	db           *db.DB
	transactions *dict.SimpleDict // id -> Transaction

	idGenerator *idgenerator.IdGenerator
}

const (
	replicas = 4
	lockSize = 64
)

func MakeCluster() *Cluster {
	cluster := &Cluster{
		self: config.Properties.Self,

		db:           db.MakeDB(),
		transactions: dict.MakeSimple(),
		peerPicker:   consistenthash.New(replicas, nil),
		peers:        make(map[string]*client.Client),

		idGenerator: idgenerator.MakeGenerator("godis", config.Properties.Self),
	}
	if config.Properties.Peers != nil && len(config.Properties.Peers) > 0 && config.Properties.Self != "" {
		contains := make(map[string]bool)
		peers := make([]string, 0, len(config.Properties.Peers)+1)
		for _, peer := range config.Properties.Peers {
			if _, ok := contains[peer]; ok {
				continue
			}
			contains[peer] = true
			peers = append(peers, peer)
		}
		peers = append(peers, config.Properties.Self)
		cluster.peerPicker.Add(peers...)
	}
	return cluster
}

// args contains all
type CmdFunc func(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply

func (cluster *Cluster) Close() {
	cluster.db.Close()
}

var router = MakeRouter()

func (cluster *Cluster) Exec(c redis.Connection, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmd + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, args)
	return
}

func (cluster *Cluster) AfterClientClose(c redis.Connection) {

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

func Ping(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 1 {
		return &reply.PongReply{}
	} else if len(args) == 2 {
		return reply.MakeStatusReply("\"" + string(args[1]) + "\"")
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// relay command to peer
// cannot call Prepare, Commit, Rollback of self node
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

// rollback local transaction
func Rollback(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rollback' command")
	}
	txId := string(args[1])
	raw, ok := cluster.transactions.Get(txId)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)
	err := tx.rollback()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return reply.MakeIntReply(1)
}

func Commit(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'commit' command")
	}
	txId := string(args[1])
	raw, ok := cluster.transactions.Get(txId)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	// finish transaction
	defer func() {
		cluster.db.UnLocks(tx.keys...)
		cluster.transactions.Remove(tx.id)
	}()

	cmd := strings.ToLower(string(tx.args[0]))
	var result redis.Reply
	if cmd == "del" {
		result = CommitDel(cluster, c, tx)
	}

	if reply.IsErrorReply(result) {
		// failed
		err2 := tx.rollback()
		return reply.MakeErrReply(fmt.Sprintf("err occurs when rollback:  %v, origin err: %s", err2, result))
	}

	return &reply.OkReply{}
}

/*----- utils -------*/

func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmd)
	for i, arg := range args {
		result[i+1] = []byte(arg)
	}
	return result
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
