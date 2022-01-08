package mr

import "fmt"
import "log"
import "net"
import "os"
import "sync"
import "net/rpc"
import "net/http"
import "io/ioutil"

type MapTask struct {
	filename string
	content  string
	status   string
}

type ReduceTask struct {
	status string
}

type Coordinator struct {
	mu         sync.Mutex
	phase      string
	nMap       int
	nReduce    int
	mapTask    map[int]*MapTask
	reduceTask map[int]*ReduceTask
}

func (c *Coordinator) InitWorker(
	args *InitWorkerArgs,
	reply *InitWorkerReply,
) error {
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetTask(
	args *GetTaskArgs,
	reply *GetTaskReply,
) error {
	c.mu.Lock()
	if len(c.mapTask) == 0 {
		c.phase = "reduce"
	}

	if c.phase == "map" {
		for id, task := range c.mapTask {
			if task.status == "scheduled" {
				continue
			}
			reply.Scheduled = true
			reply.Phase = c.phase
			reply.TaskId = id
			reply.Filename = task.filename
			reply.Content = task.content

			task.status = "scheduled"
			break
		}
	}

	if c.phase == "reduce" {
		for id, task := range c.reduceTask {
			if task.status == "scheduled" {
				continue
			}
			reply.Scheduled = true
			reply.Phase = c.phase
			reply.TaskId = id
			reply.Filename = fmt.Sprintf("mr-*-%d", id)
			reply.Content = ""

			task.status = "scheduled"
			break
		}
	}

	if reply.Scheduled {
		fmt.Printf(
			"[%s task scheduled] id: %d - file: %s \n",
			reply.Phase,
			reply.TaskId,
			reply.Filename,
		)
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) UpdateTask(
	args *UpdateTaskArgs,
	reply *UpdateTaskReply,
) error {
	c.mu.Lock()
	if args.Phase == "map" {
		delete(c.mapTask, args.TaskId)
	}
	if args.Phase == "reduce" {
		delete(c.reduceTask, args.TaskId)
	}
	c.mu.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.phase == "reduce" && len(c.reduceTask) == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:      "map",
		nMap:       len(files),
		nReduce:    nReduce,
		mapTask:    make(map[int]*MapTask),
		reduceTask: make(map[int]*ReduceTask),
	}

	for index, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		c.mapTask[index] = &MapTask{
			filename: filename,
			content:  string(content),
			status:   "idle",
		}
	}

	for index := 0; index < nReduce; index++ {
		c.reduceTask[index] = &ReduceTask{
			status: "idle",
		}
	}

	fmt.Printf("The total amount of map tasks: %d\n", len(c.mapTask))
	c.server()
	return &c
}
