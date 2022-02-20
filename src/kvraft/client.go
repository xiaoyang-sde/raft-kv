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

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(
	key string,
) string {
	index := ck.leader
	for {
		args := GetArgs{
			ClientId:  ck.clientId,
			MessageId: ck.messageId,
			Key:       key,
		}
		reply := GetReply{}

		ck.servers[index%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		err := reply.Err
		value := reply.Value

		if err == ErrNoKey || err == OK {
			ck.leader = index % len(ck.servers)
			ck.messageId += 1
			return value
		} else if err == ErrWrongLeader {
			index += 1
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(
	key string,
	value string,
	op string,
) {
	index := ck.leader
	for {
		args := PutAppendArgs{
			ClientId:  ck.clientId,
			MessageId: ck.messageId,
			Key:       key,
			Value:     value,
			Op:        op,
		}
		reply := PutAppendReply{}

		ck.servers[index%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		err := reply.Err

		if err == OK {
			ck.leader = index % len(ck.servers)
			ck.messageId += 1
			return
		} else if err == ErrNoKey {
			index += 1
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
