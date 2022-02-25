package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
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
