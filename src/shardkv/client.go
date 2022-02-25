package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

func key2shard(
	key string,
) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	make_end  func(string) *labrpc.ClientEnd
	clientId  int64
	messageId int
}

func MakeClerk(
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd,
) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

func (ck *Clerk) Command(
	request OperationRequest,
) string {
	request.ClientId = ck.clientId
	request.MessageId = ck.messageId
	key := request.Key
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for index := 0; index < len(servers); index++ {
				server := ck.make_end(servers[index])
				response := OperationResponse{}
				ok := server.Call("ShardKV.Operation", &request, &response)

				if ok && (response.Err == OK || response.Err == ErrNoKey) {
					ck.messageId += 1
					return response.Value
				}
				if ok && response.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Get(
	key string,
) string {
	args := OperationRequest{
		Key:    key,
		Method: "Get",
	}
	return ck.Command(args)
}

func (ck *Clerk) Put(
	key string,
	value string,
) {
	args := OperationRequest{
		Key:    key,
		Value:  value,
		Method: "Put",
	}
	ck.Command(args)
}

func (ck *Clerk) Append(
	key string,
	value string,
) {
	args := OperationRequest{
		Key:    key,
		Value:  value,
		Method: "Append",
	}
	ck.Command(args)
}
