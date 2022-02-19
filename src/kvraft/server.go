package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
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
	applied map[int]bool
	cond    *sync.Cond
}

func (kv *KVServer) applyRoutine() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		command := applyMsg.Command.(Op)
		commandValid := applyMsg.CommandValid
		commandIndex := applyMsg.CommandIndex
		if commandValid {
			op := command.Op
			key := command.Key
			value := command.Value

			switch op {
			case "Put":
				kv.state[key] = value
			case "Append":
				kv.state[key] += value
			}

			kv.applied[commandIndex] = true
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

	key := args.Key
	command := Op{
		Key: key,
		Op:  "Get",
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] Get %v - started\n", kv.me, key)

	for !kv.applied[index] {
		kv.cond.Wait()
	}
	DPrintf("[%d] Get %v - applied\n", kv.me, key)

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

	key := args.Key
	value := args.Value
	op := args.Op
	command := Op{
		Key:   key,
		Value: value,
		Op:    op,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[%d] %v (%v, %v) - started\n", kv.me, op, key, value)

	for !kv.applied[index] {
		kv.cond.Wait()
	}
	DPrintf("[%d] %v (%v, %v) - applied\n", kv.me, op, key, value)
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
	kv.applied = make(map[int]bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.cond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyRoutine()
	return kv
}
