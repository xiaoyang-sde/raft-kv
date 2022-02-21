package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leader    int
	clientId  int64
	messageId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

func (ck *Clerk) Command(
	key string,
	value string,
	method string,
) string {
	index := ck.leader
	for {
		args := CommandArgs{
			ClientId:  ck.clientId,
			MessageId: ck.messageId,
			Method:    method,
			Key:       key,
			Value:     value,
		}
		reply := CommandReply{}

		ok := ck.servers[index%len(ck.servers)].Call("KVServer.Command", &args, &reply)
		err := reply.Err
		value := reply.Value

		if !ok || err == ErrWrongLeader || err == ErrTimeout {
			index += 1
		} else {
			ck.leader = index % len(ck.servers)
			ck.messageId += 1
			return value
		}
	}
}

func (ck *Clerk) Get(
	key string,
) string {
	return ck.Command(key, "", "Get")
}

func (ck *Clerk) Put(
	key string,
	value string,
) {
	ck.Command(key, value, "Put")
}
func (ck *Clerk) Append(
	key string,
	value string,
) {
	ck.Command(key, value, "Append")
}
