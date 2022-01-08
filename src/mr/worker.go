package mr

import "os"
import "fmt"
import "time"
import "log"
import "sort"
import "encoding/json"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	nMap := initWorkerReply.NMap
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

				for reduceId := 0; reduceId < 10; reduceId++ {
					resultFilename := fmt.Sprintf(
						"mr-%d-%d",
						taskId,
						reduceId,
					)
					os.Create(resultFilename)
				}

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
					)
					if openErr != nil {
						fmt.Printf(
							"[map encode error] id: %d, result file: %s\n",
							taskId,
							resultFilename,
						)
						return
					}

					enc := json.NewEncoder(resultFile)
					encodeErr := enc.Encode(&kv)
					if encodeErr != nil {
						fmt.Printf(
							"[map encode error] id: %d, key: %s, value: %s\n",
							taskId,
							kv.Key,
							kv.Value,
						)
						return
					}

					resultFile.Close()
				}

				fmt.Printf(
					"[%s task encoded] id: %d\n",
					phase,
					taskId,
				)
			}

			if phase == "reduce" {
				reduceInput := []KeyValue{}
				for mapId := 0; mapId < nMap; mapId++ {
					inputFilename := fmt.Sprintf(
						"mr-%d-%d",
						mapId,
						taskId,
					)

					inputFile, openErr := os.Open(inputFilename)
					if openErr != nil {
						fmt.Printf(
							"[reduce decode error] id: %d, input file: %s\n",
							taskId,
							inputFilename,
						)
						break
					}

					dec := json.NewDecoder(inputFile)
					for {
						var kv KeyValue
						if decodeErr := dec.Decode(&kv); decodeErr != nil {
							break
						}
						reduceInput = append(reduceInput, kv)
					}
				}

				sort.Sort(ByKey(reduceInput))

				resultFilename := fmt.Sprintf(
					"mr-out-%d",
					taskId,
				)
				resultFile, _ := os.Create(resultFilename)

				i := 0
				for i < len(reduceInput) {
					j := i + 1
					for j < len(reduceInput) && reduceInput[j].Key == reduceInput[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, reduceInput[k].Value)
					}
					output := reducef(reduceInput[i].Key, values)

					fmt.Fprintf(resultFile, "%v %v\n", reduceInput[i].Key, output)

					i = j
				}

				resultFile.Close()
			}

			updateTaskArgs := UpdateTaskArgs{
				Phase:  phase,
				TaskId: taskId,
			}
			updateTaskReply := UpdateTaskReply{}
			call(
				"Coordinator.UpdateTask",
				&updateTaskArgs,
				&updateTaskReply,
			)
		} else {
			time.Sleep(1 * time.Millisecond)
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
