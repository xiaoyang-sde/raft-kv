package mr

import (
	"os"
	"strconv"
)

type InitWorkerArgs struct {
}

type InitWorkerReply struct {
	NMap    int
	NReduce int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Scheduled bool
	Phase     string
	TaskId    int
	Filename  string
	Content   string
}

type CommitTaskArgs struct {
	Phase  string
	TaskId int
}

type CommitTaskReply struct {
	Done bool
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
