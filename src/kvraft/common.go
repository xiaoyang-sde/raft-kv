package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
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
