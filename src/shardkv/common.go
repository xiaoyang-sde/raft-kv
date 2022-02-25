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
	ErrFuture      = "ErrFuture"
)

type Err string

type OperationRequest struct {
	ClientId  int64
	MessageId int
	Key       string
	Value     string
	Method    string
}

type OperationResponse struct {
	Err   Err
	Value string
}

type PullShardRequest struct {
	Num       int
	ShardList []int
}

type PullShardResponse struct {
	Err    Err
	Num    int
	State  map[int]Shard
	Client map[int64]int
}

type DeleteShardRequest struct {
	Num       int
	ShardList []int
}

type DeleteShardResponse struct {
	Err Err
}
