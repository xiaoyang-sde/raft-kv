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

func DPrintf(
	format string,
	a ...interface{},
) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	maxraftstate int
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd

	clerk      *shardctrler.Clerk
	state      [shardctrler.NShards]Shard
	config     shardctrler.Config
	lastConfig shardctrler.Config
	client     map[int64]int
	clientCh   map[int]chan Command
}

type Command struct {
	ClientId  int64
	MessageId int
	Key       string
	Value     string
	Method    string
}

type Configuration struct {
	Config shardctrler.Config
}

type MergeState struct {
	State  map[int]Shard
	Client map[int64]int
}

const (
	Default = 0
	Pull    = 1
	Push    = 2
	Stale   = 3
)

type Shard struct {
	Status int
	State  map[string]string
}

func (shard *Shard) Get(
	key string,
) string {
	return shard.State[key]
}

func (shard *Shard) Put(
	key string,
	value string,
) {
	shard.State[key] = value
}

func (shard *Shard) Append(
	key string,
	value string,
) {
	shard.State[key] += value
}

func (shard *Shard) Copy() map[string]string {
	result := make(map[string]string)
	for k, v := range shard.State {
		result[k] = v
	}
	return result
}

type Snapshot struct {
	State      [shardctrler.NShards]Shard
	Config     shardctrler.Config
	LastConfig shardctrler.Config
	Client     map[int64]int
}

func (kv *ShardKV) staleShard(
	shard int,
) bool {
	if kv.config.Shards[shard] != kv.gid {
		return true
	}
	if kv.state[shard].Status == Pull || kv.state[shard].Status == Push {
		return true
	}
	return false
}

func (kv *ShardKV) snapshot(
	index int,
) {
	snapshot := Snapshot{
		State:      kv.state,
		Config:     kv.config,
		LastConfig: kv.lastConfig,
		Client:     kv.client,
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
		kv.config = decodeSnapshot.Config
		kv.lastConfig = decodeSnapshot.LastConfig
		kv.client = decodeSnapshot.Client
		return
	} else {
		panic(err)
	}
}

func (kv *ShardKV) configureRoutine() {
	for !kv.killed() {
		kv.mu.RLock()
		defaultShard := true
		for _, shard := range kv.state {
			if shard.Status != Default {
				defaultShard = false
				break
			}
		}
		currentNum := kv.config.Num
		kv.mu.RUnlock()

		_, isLeader := kv.rf.GetState()
		if isLeader && defaultShard {
			nextConfig := kv.clerk.Query(currentNum + 1)
			nextNum := nextConfig.Num

			if currentNum+1 == nextNum {
				command := Configuration{
					Config: nextConfig,
				}
				_, _, isLeader := kv.rf.Start(command)
				if isLeader {
					DPrintf(
						"[Group %d][Instance %d] Configuration %+v - start\n",
						kv.gid, kv.me, nextConfig,
					)
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applyRoutine() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		commandValid := applyMsg.CommandValid
		if commandValid {
			commandIndex := applyMsg.CommandIndex

			if command, ok := applyMsg.Command.(Command); ok {
				messageId := command.MessageId
				clientId := command.ClientId
				method := command.Method
				key := command.Key
				value := command.Value
				shard := key2shard(key)

				if !kv.staleShard(shard) && kv.client[clientId] < messageId {
					switch method {
					case "Put":
						kv.state[shard].Put(key, value)
					case "Append":
						kv.state[shard].Append(key, value)
					}
					kv.client[clientId] = messageId
				}

				if clientCh, ok := kv.clientCh[commandIndex]; ok {
					DPrintf(
						"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - broadcast\n",
						kv.gid, kv.me, clientId, messageId, method, key, value,
					)
					clientCh <- command
				}
			}

			if command, ok := applyMsg.Command.(Configuration); ok {
				config := command.Config
				kv.applyConfiguration(config)
			}

			if command, ok := applyMsg.Command.(MergeState); ok {
				state := command.State
				client := command.Client
				kv.applyMergeState(state, client)
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

func (kv *ShardKV) migrationRoutine() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.RLock()
			gidShardList := make(map[int][]int)
			for shard := range kv.state {
				if kv.state[shard].Status == Pull {
					gid := kv.lastConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.RUnlock()

			var waitGroup sync.WaitGroup
			for gid, shardList := range gidShardList {
				waitGroup.Add(1)
				go kv.sendPullShard(&waitGroup, gid, shardList)
			}
			waitGroup.Wait()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) applyConfiguration(
	nextConfig shardctrler.Config,
) {
	nextNum := nextConfig.Num
	if nextNum != kv.config.Num+1 {
		return
	}

	for shard, gid := range nextConfig.Shards {
		targetGid := kv.config.Shards[shard]
		if gid == kv.gid && targetGid != kv.gid && targetGid != 0 {
			kv.state[shard].Status = Pull
		}
		if gid != kv.gid && targetGid == kv.gid && gid != 0 {
			kv.state[shard].Status = Push
		}
	}

	kv.lastConfig = kv.config
	kv.config = nextConfig

	DPrintf(
		"[Group %d][Instance %d] Configuration %+v - applied\n",
		kv.gid, kv.me, nextConfig,
	)
	DPrintf("[Group %d][Instance %d] Shard %+v - applied\n",
		kv.gid, kv.me, kv.state,
	)
}

func (kv *ShardKV) applyMergeState(
	state map[int]Shard,
	client map[int64]int,
) {
	for shard := range state {
		if kv.state[shard].Status == Pull {
			for k, v := range state[shard].State {
				kv.state[shard].Put(k, v)
			}
			kv.state[shard].Status = Default
		}
	}

	for clientId, messageId := range client {
		if kv.client[clientId] < messageId {
			kv.client[clientId] = messageId
		}
	}

	DPrintf(
		"[Group %d][Instance %d] MergeState %+v - applied\n",
		kv.gid, kv.me, state,
	)
}

func (kv *ShardKV) sendPullShard(
	waitGroup *sync.WaitGroup,
	gid int,
	shardList []int,
) {
	for _, server := range kv.lastConfig.Groups[gid] {
		args := PullShardArgs{
			ShardList: shardList,
			Num:       kv.config.Num,
		}
		reply := PullShardReply{}

		ok := kv.make_end(server).Call("ShardKV.PullShard", &args, &reply)
		if ok && reply.Err == OK {
			state := reply.State
			client := reply.Client
			command := MergeState{
				State:  state,
				Client: client,
			}
			_, _, isLeader := kv.rf.Start(command)
			if isLeader {
				DPrintf(
					"[Group %d][Instance %d] MergeState %v from %d - start\n",
					kv.gid, kv.me, gid, shardList,
				)
			}
		}
	}
	waitGroup.Done()
}

func (kv *ShardKV) PullShard(
	args *PullShardArgs,
	reply *PullShardReply,
) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Num < args.Num {
		reply.Err = ErrFuture
		kv.mu.Unlock()
		return
	}

	shardList := args.ShardList
	state := make(map[int]Shard)
	for _, shard := range shardList {
		state[shard] = Shard{
			State: kv.state[shard].Copy(),
		}
		kv.state[shard].Status = Default
	}

	client := make(map[int64]int)
	for clientId, messageId := range kv.client {
		client[clientId] = messageId
	}

	reply.State = state
	reply.Client = client
	reply.Err = OK
	kv.mu.Unlock()
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
	shard := key2shard(key)

	kv.mu.RLock()
	if kv.staleShard(shard) {
		DPrintf(
			"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - stale shard %d\n",
			kv.gid, kv.me, clientId, messageId, method, key, value, shard,
		)
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

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
		"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - start\n",
		kv.gid, kv.me, clientId, messageId, method, key, value,
	)
	kv.clientCh[index] = make(chan Command, 1)
	clientCh := kv.clientCh[index]
	kv.mu.Unlock()

	select {
	case appliedCommand := <-clientCh:
		DPrintf(
			"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - applied\n",
			kv.gid, kv.me, clientId, messageId, method, key, value,
		)

		appliedClientId := appliedCommand.ClientId
		appliedMessageId := appliedCommand.MessageId
		if clientId != appliedClientId || messageId != appliedMessageId {
			DPrintf(
				"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - stale\n",
				kv.gid, kv.me, clientId, messageId, method, key, value,
			)
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.RLock()
		if kv.staleShard(shard) {
			DPrintf(
				"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - stale shard %d\n",
				kv.gid, kv.me, clientId, messageId, method, key, value, shard,
			)
			reply.Err = ErrWrongGroup
			kv.mu.RUnlock()
			return
		}

		if method == "Get" {
			reply.Value = kv.state[shard].Get(key)
		}
		kv.mu.RUnlock()
		reply.Err = OK
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf(
			"[Group %d][Instance %d][Client %d][Message %d] %v (%v, %v) - timeout\n",
			kv.gid, kv.me, clientId, messageId, method, key, value,
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
	labgob.Register(Command{})
	labgob.Register(Configuration{})
	labgob.Register(MergeState{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid

	kv.ctrlers = ctrlers
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.client = make(map[int64]int)
	kv.clientCh = make(map[int]chan Command)
	for index := range kv.state {
		kv.state[index] = Shard{
			Status: Default,
			State:  make(map[string]string),
		}
	}

	kv.persister = persister
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyRoutine()
	go kv.configureRoutine()
	go kv.migrationRoutine()
	return kv
}
