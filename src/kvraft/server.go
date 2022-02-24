package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(
	format string,
	a ...interface{},
) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	ClientId  int64
	MessageId int
	Key       string
	Value     string
	Method    string
}

type Snapshot struct {
	State  map[string]string
	Client map[int64]int
}

type KVServer struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	persister    *raft.Persister

	state    map[string]string
	client   map[int64]int
	clientCh map[int]chan Command
}

func (kv *KVServer) snapshot(
	index int,
) {
	snapshot := Snapshot{
		State:  kv.state,
		Client: kv.client,
	}

	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(snapshot)

	DPrintf(
		"[%d] %d - snapshot\n",
		kv.me, index,
	)

	kv.rf.Snapshot(index, writer.Bytes())
}

func (kv *KVServer) readPersist(
	snapshot []byte,
) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)

	var decodeSnapshot Snapshot
	if err := decoder.Decode(&decodeSnapshot); err == nil {
		kv.state = decodeSnapshot.State
		kv.client = decodeSnapshot.Client
	} else {
		panic(err)
	}
}

func (kv *KVServer) applyRoutine() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		commandValid := applyMsg.CommandValid
		if commandValid {
			command := applyMsg.Command.(Command)
			commandIndex := applyMsg.CommandIndex

			messageId := command.MessageId
			clientId := command.ClientId
			method := command.Method
			key := command.Key
			value := command.Value

			if kv.client[clientId] < messageId {
				switch method {
				case "Put":
					kv.state[key] = value
				case "Append":
					kv.state[key] += value
				}
				kv.client[clientId] = messageId
			}

			if clientCh, ok := kv.clientCh[commandIndex]; ok {
				DPrintf(
					"[%d][%d][%d] %v (%v, %v) - broadcast\n",
					kv.me, clientId, messageId, method, key, value,
				)
				clientCh <- command
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot(commandIndex)
			}
		}

		snapshotValid := applyMsg.SnapshotValid
		if snapshotValid {
			snapshot := applyMsg.Snapshot
			snapshotTerm := applyMsg.SnapshotTerm
			snapshotIndex := applyMsg.SnapshotIndex
			if kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, snapshot) {
				kv.readPersist(snapshot)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Command(
	args *CommandArgs,
	reply *CommandReply,
) {
	clientId := args.ClientId
	messageId := args.MessageId
	key := args.Key
	value := args.Value
	method := args.Method
	command := Command{
		ClientId:  clientId,
		MessageId: messageId,
		Key:       key,
		Value:     value,
		Method:    method,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DPrintf(
		"[%d][%d][%d] %v (%v, %v) - start\n",
		kv.me, clientId, messageId, method, key, value,
	)
	kv.clientCh[index] = make(chan Command, 1)
	clientCh := kv.clientCh[index]
	kv.mu.Unlock()

	select {
	case appliedCommand := <-clientCh:
		DPrintf(
			"[%d][%d][%d] %v (%v, %v) - applied\n",
			kv.me, clientId, messageId, method, key, value,
		)
		kv.mu.RLock()
		if method == "Get" {
			reply.Value = kv.state[key]
		}
		kv.mu.RUnlock()

		appliedClientId := appliedCommand.ClientId
		appliedMessageId := appliedCommand.MessageId
		if clientId != appliedClientId || messageId != appliedMessageId {
			DPrintf(
				"[%d][%d][%d] %v (%v, %v) - stale\n",
				kv.me, clientId, messageId, method, key, value,
			)
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = OK
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf(
			"[%v][%d][%d] %v (%v, %v) - timeout\n",
			kv.me, clientId, messageId, method, key, value,
		)
	}

	go func() {
		kv.mu.Lock()
		close(clientCh)
		delete(kv.clientCh, index)
		kv.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
) *KVServer {
	labgob.Register(Command{})
	labgob.Register(Snapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.state = make(map[string]string)
	kv.client = make(map[int64]int)
	kv.clientCh = make(map[int]chan Command)

	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyRoutine()
	return kv
}
