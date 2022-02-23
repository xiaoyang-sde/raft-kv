package shardctrler

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
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
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

func (ck *Clerk) Command(
	args *CommandArgs,
) Config {
	for {
		for _, server := range ck.servers {
			reply := &CommandReply{}
			ok := server.Call("ShardCtrler.Command", args, &reply)
			if ok && !reply.WrongLeader {
				ck.messageId += 1
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Query(
	num int,
) Config {
	args := &CommandArgs{}
	args.Method = "Query"
	args.ClientId = ck.clientId
	args.MessageId = ck.messageId
	args.QueryNum = num
	return ck.Command(args)
}

func (ck *Clerk) Join(
	servers map[int][]string,
) {
	args := &CommandArgs{}
	args.Method = "Join"
	args.ClientId = ck.clientId
	args.MessageId = ck.messageId
	args.JoinServers = servers
	ck.Command(args)
}

func (ck *Clerk) Leave(
	gids []int,
) {
	args := &CommandArgs{}
	args.Method = "Leave"
	args.ClientId = ck.clientId
	args.MessageId = ck.messageId
	args.LeaveGIDs = gids
	ck.Command(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	args.Method = "Move"
	args.ClientId = ck.clientId
	args.MessageId = ck.messageId
	args.MoveShard = shard
	args.MoveGID = gid
	ck.Command(args)
}
