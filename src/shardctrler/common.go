package shardctrler

const NShards = 10

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK         = "OK"
	ErrTimeout = "ErrTimeout"
)

type Err string

type OperationRequest struct {
	ClientId    int64
	MessageId   int
	Method      string
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
	QueryNum    int
}

type OperationResponse struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
