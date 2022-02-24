package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrTimeout     = "ErrTimeout"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type CommandArgs struct {
	ClientId  int64
	MessageId int
	Key       string
	Value     string
	Method    string
}

type CommandReply struct {
	Err   Err
	Value string
}

type RequestShardArgs struct {
	Shard int
}

type RequestShardReply struct {
	Err   Err
	State map[string]string
}
