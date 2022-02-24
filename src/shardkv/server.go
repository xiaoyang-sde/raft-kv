package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	maxraftstate int
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd

	clerk    *shardctrler.Clerk
	config   shardctrler.Config
	state    map[string]string
	client   map[int64]int
	clientCh map[int]chan CommandArgs
}

type Snapshot struct {
	State  map[string]string
	Client map[int64]int
}

func (kv *ShardKV) GetConfig() {
	kv.config = kv.clerk.Query(-1)
}

func (kv *ShardKV) snapshot(
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

func (kv *ShardKV) readPersist(
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

func (kv *ShardKV) applyRoutine() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		commandValid := applyMsg.CommandValid
		if commandValid {
			command := applyMsg.Command.(CommandArgs)
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
					"[%v][%d][%d] %v (%v, %v) - broadcast\n",
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

func (kv *ShardKV) Command(
	args *CommandArgs,
	reply *CommandReply,
) {
	clientId := args.ClientId
	messageId := args.MessageId
	key := args.Key
	value := args.Value
	method := args.Method

	index, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DPrintf(
		"[%d][%d][%d][%d] %v (%v, %v) - start\n",
		kv.gid, kv.me, clientId, messageId, method, key, value,
	)
	kv.clientCh[index] = make(chan CommandArgs, 1)
	clientCh := kv.clientCh[index]
	kv.mu.Unlock()

	select {
	case appliedCommand := <-clientCh:
		DPrintf(
			"[%d][%d][%d][%d] %v (%v, %v) - applied\n",
			kv.gid, kv.me, clientId, messageId, method, key, value,
		)
		kv.mu.Lock()
		if method == "Get" {
			reply.Value = kv.state[key]
		}
		kv.mu.Unlock()

		appliedClientId := appliedCommand.ClientId
		appliedMessageId := appliedCommand.MessageId
		if clientId != appliedClientId || messageId != appliedMessageId {
			DPrintf(
				"[%v][%d][%d] %v (%v, %v) - stale\n",
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	labgob.Register(CommandArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid

	kv.ctrlers = ctrlers
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.state = make(map[string]string)
	kv.client = make(map[int64]int)
	kv.clientCh = make(map[int]chan CommandArgs)

	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.GetConfig()
	DPrintf("%+v\n", kv.config)
	go kv.applyRoutine()
	return kv
}
