package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
)

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

func MakeClerk(
	servers []*labrpc.ClientEnd,
) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

func (ck *Clerk) Operation(
	key string,
	value string,
	method string,
) string {
	index := ck.leader
	for {
		request := OperationRequest{
			ClientId:  ck.clientId,
			MessageId: ck.messageId,
			Method:    method,
			Key:       key,
			Value:     value,
		}
		response := OperationResponse{}

		ok := ck.servers[index%len(ck.servers)].Call("KVServer.Operation", &request, &response)
		err := response.Err
		value := response.Value

		if ok && err == OK {
			ck.leader = index % len(ck.servers)
			ck.messageId += 1
			return value
		} else {
			index += 1
		}
	}
}

func (ck *Clerk) Get(
	key string,
) string {
	return ck.Operation(key, "", "Get")
}

func (ck *Clerk) Put(
	key string,
	value string,
) {
	ck.Operation(key, value, "Put")
}

func (ck *Clerk) Append(
	key string,
	value string,
) {
	ck.Operation(key, value, "Append")
}
