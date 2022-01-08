package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	filename  string
	content   string
	status    string
	timestamp time.Time
}

type Coordinator struct {
	mu         sync.Mutex
	phase      string
	nMap       int
	nReduce    int
	mapTask    map[int]*Task
	reduceTask map[int]*Task
}

func (c *Coordinator) InitWorker(
	args *InitWorkerArgs,
	reply *InitWorkerReply,
) error {
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) heartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.mapTask {
		if task.status == "idle" {
			continue
		}
		diff := time.Since(task.timestamp)
		if diff.Seconds() > 10 {
			task.status = "idle"
		}
	}

	for _, task := range c.reduceTask {
		if task.status == "idle" {
			continue
		}
		diff := time.Since(task.timestamp)
		if diff.Seconds() > 10 {
			task.status = "idle"
		}
	}
}

func (c *Coordinator) GetTask(
	args *GetTaskArgs,
	reply *GetTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
			task.timestamp = time.Now()
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
			task.timestamp = time.Now()
			break
		}
	}
	return nil
}

func (c *Coordinator) CommitTask(
	args *CommitTaskArgs,
	reply *CommitTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	phase := args.Phase
	taskId := args.TaskId
	if phase == "map" && c.mapTask[taskId].status == "scheduled" {
		delete(c.mapTask, taskId)
	}

	if phase == "reduce" && c.reduceTask[taskId].status == "scheduled" {
		delete(c.reduceTask, taskId)
	}

	reply.Done = c.phase == "reduce" && len(c.reduceTask) == 0
	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == "reduce" && len(c.reduceTask) == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:      "map",
		nMap:       len(files),
		nReduce:    nReduce,
		mapTask:    make(map[int]*Task),
		reduceTask: make(map[int]*Task),
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

		c.mapTask[index] = &Task{
			filename: filename,
			content:  string(content),
			status:   "idle",
		}
	}

	for index := 0; index < nReduce; index++ {
		c.reduceTask[index] = &Task{
			status: "idle",
		}
	}

	go func() {
		for {
			c.heartbeat()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
