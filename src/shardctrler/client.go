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

func MakeClerk(
	servers []*labrpc.ClientEnd,
) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

func (ck *Clerk) Operation(
	request OperationRequest,
) Config {
	for {
		for _, server := range ck.servers {
			response := OperationResponse{}
			ok := server.Call("ShardCtrler.Operation", &request, &response)
			if ok && response.Err == OK {
				ck.messageId += 1
				return response.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Query(
	num int,
) Config {
	request := OperationRequest{
		ClientId:  ck.clientId,
		MessageId: ck.messageId,
		Method:    "Query",
		QueryNum:  num,
	}
	return ck.Operation(request)
}

func (ck *Clerk) Join(
	servers map[int][]string,
) {
	request := OperationRequest{
		ClientId:    ck.clientId,
		MessageId:   ck.messageId,
		Method:      "Join",
		JoinServers: servers,
	}
	ck.Operation(request)
}

func (ck *Clerk) Leave(
	gids []int,
) {
	request := OperationRequest{
		ClientId:  ck.clientId,
		MessageId: ck.messageId,
		Method:    "Leave",
		LeaveGIDs: gids,
	}
	ck.Operation(request)
}

func (ck *Clerk) Move(
	shard int,
	gid int,
) {
	request := OperationRequest{
		ClientId:  ck.clientId,
		MessageId: ck.messageId,
		Method:    "Move",
		MoveShard: shard,
		MoveGID:   gid,
	}
	ck.Operation(request)
}
