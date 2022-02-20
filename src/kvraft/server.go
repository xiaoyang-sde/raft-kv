package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Id    int64
	Key   string
	Value string
	Op    string
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	state   map[string]string
	index   map[int]int64
	applied map[int64]bool
	cond    *sync.Cond
	term    int
}

func (kv *KVServer) applyRoutine() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		command := applyMsg.Command.(Op)
		commandIndex := applyMsg.CommandIndex
		commandValid := applyMsg.CommandValid
		if commandValid {
			id := command.Id
			op := command.Op
			key := command.Key
			value := command.Value

			if !kv.applied[id] {
				switch op {
				case "Put":
					kv.state[key] = value
				case "Append":
					kv.state[key] += value
				}
				kv.applied[id] = true
			}

			kv.index[commandIndex] = id
			DPrintf("[%v] %v (%v, %v) - broadcast\n", kv.me, op, key, value)
			kv.cond.Broadcast()
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(
	args *GetArgs,
	reply *GetReply,
) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	id := args.Id
	key := args.Key
	command := Op{
		Id:  id,
		Key: key,
		Op:  "Get",
	}

	index, term, isLeader := kv.rf.Start(command)
	if !isLeader || term > kv.term {
		kv.term = term
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] Get %v - started\n", kv.me, key)

	for kv.index[index] == 0 {
		kv.cond.Wait()
	}

	if kv.index[index] != id {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%d] Get %v - index\n", kv.me, key)

	value, ok := kv.state[key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(
	args *PutAppendArgs,
	reply *PutAppendReply,
) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	id := args.Id
	key := args.Key
	value := args.Value
	op := args.Op
	command := Op{
		Id:    id,
		Key:   key,
		Value: value,
		Op:    op,
	}

	index, term, isLeader := kv.rf.Start(command)
	if !isLeader || term > kv.term {
		kv.term = term
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%d] %v (%v, %v) - started\n", kv.me, op, key, value)

	for kv.index[index] == 0 {
		kv.cond.Wait()
	}

	if kv.index[index] != id {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%d] %v (%v, %v) - index\n", kv.me, op, key, value)
	reply.Err = OK
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
	// Your code here, if desired.
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.state = make(map[string]string)
	kv.index = make(map[int]int64)
	kv.applied = make(map[int64]bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.cond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.term = 1

	go kv.applyRoutine()
	return kv
}
