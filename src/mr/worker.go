package mr

import "os"
import "fmt"
import "time"
import "log"
import "encoding/json"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	initWorkerArgs := InitWorkerArgs{}
	initWorkerReply := InitWorkerReply{}
	call(
		"Coordinator.InitWorker",
		&initWorkerArgs,
		&initWorkerReply,
	)
	nReduce := initWorkerReply.NReduce

	for {
		getTaskArgs := GetTaskArgs{}
		getTaskReply := GetTaskReply{}
		call(
			"Coordinator.GetTask",
			&getTaskArgs,
			&getTaskReply,
		)

		if getTaskReply.Scheduled {
			taskId := getTaskReply.TaskId
			phase := getTaskReply.Phase
			filename := getTaskReply.Filename
			content := getTaskReply.Content

			fmt.Printf(
				"[%s task received] id: %d, file: %s \n",
				phase,
				taskId,
				filename,
			)


			if phase == "map" {
				mapResult := mapf(filename, content)
				fmt.Printf(
					"[%s task finished] id: %d, kv length: %d\n",
					phase,
					taskId,
					len(mapResult),
				)

				for _, kv := range mapResult {
					reduceId := ihash(kv.Key) % nReduce
					resultFilename := fmt.Sprintf(
						"mr-%d-%d",
						taskId,
						reduceId,
					)
					resultFile, openErr := os.OpenFile(
						resultFilename,
						os.O_APPEND|os.O_CREATE|os.O_WRONLY,
						0644,
					);
					if openErr != nil {
						fmt.Printf(
							"[encode error] id: %d, result file: %s\n",
							taskId,
							resultFilename,
						)
						break
					}

					enc := json.NewEncoder(resultFile)
					encodeErr := enc.Encode(&kv)
					if encodeErr != nil {
						fmt.Printf(
							"[encode error] id: %d, key: %s, value: %s\n",
							taskId,
							kv.Key,
							kv.Value,
						)
						break
					}

					resultFile.Close()
				}

				fmt.Printf(
					"[%s task encoded] id: %d\n",
					phase,
					taskId,
				)
			}
		} else {
			time.Sleep(1 * time.Millisecond)
			break
		}
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
