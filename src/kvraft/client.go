package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int
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
	id := nrand()
	for {
		args := GetArgs{
			Id:  id,
			Key: key,
		}
		reply := GetReply{}

		ck.servers[index%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		err := reply.Err
		value := reply.Value

		switch err {
		case ErrNoKey:
			ck.leader = index % len(ck.servers)
			return ""
		case OK:
			ck.leader = index % len(ck.servers)
			return value
		default:
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
	id := nrand()
	for {
		args := PutAppendArgs{
			Id:    id,
			Key:   key,
			Value: value,
			Op:    op,
		}
		reply := PutAppendReply{}

		ck.servers[index%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		err := reply.Err
		switch err {
		case OK:
			ck.leader = index % len(ck.servers)
			return
		default:
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
