package mr

import "os"
import "strconv"

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Scheduled bool
	Phase     string
	TaskId    int
	Filename  string
	Content   string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
