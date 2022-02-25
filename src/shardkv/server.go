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

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	gid          int
	maxRaftState int
	dead         int32
	rf           *raft.Raft
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	ctrlers      []*labrpc.ClientEnd
	makeEnd      func(string) *labrpc.ClientEnd

	clerk       *shardctrler.Clerk
	state       [shardctrler.NShards]Shard
	client      map[int64]int
	broadcastCh map[int]chan interface{}
	config      shardctrler.Config
	lastConfig  shardctrler.Config
}

type Operation struct {
	ClientId  int64
	MessageId int
	Key       string
	Value     string
	Method    string
}

type Configuration struct {
	Config shardctrler.Config
}

type PullShard struct {
	State  map[int]Shard
	Client map[int64]int
	Num    int
}

type DeleteShard struct {
	ShardList []int
	Num       int
}

const (
	Default    = 0
	Pull       = 1
	Push       = 2
	Collection = 3
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

	decodeSnapshot := Snapshot{}
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

func (kv *ShardKV) applyRoutine() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		commandValid := applyMsg.CommandValid
		if commandValid {
			commandIndex := applyMsg.CommandIndex
			if command, ok := applyMsg.Command.(Operation); ok {
				kv.applyOperation(command)
			}

			if command, ok := applyMsg.Command.(Configuration); ok {
				kv.applyConfiguration(command)
			}

			if command, ok := applyMsg.Command.(PullShard); ok {
				kv.applyPullShard(command)
			}

			if command, ok := applyMsg.Command.(DeleteShard); ok {
				kv.applyDeleteShard(command)
			}

			if broadcastCh, ok := kv.broadcastCh[commandIndex]; ok {
				broadcastCh <- applyMsg.Command
			}

			if kv.maxRaftState != -1 && kv.persister.RaftStateSize() > kv.maxRaftState {
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

func (kv *ShardKV) configurationRoutine() {
	for !kv.killed() {
		kv.mu.RLock()
		defaultShard := true
		for _, shard := range kv.state {
			if shard.Status != Default {
				defaultShard = false
				break
			}
		}
		num := kv.config.Num
		kv.mu.RUnlock()

		_, isLeader := kv.rf.GetState()
		if isLeader && defaultShard {
			nextConfig := kv.clerk.Query(num + 1)
			nextNum := nextConfig.Num

			if num+1 == nextNum {
				command := Configuration{
					Config: nextConfig,
				}
				index, _, isLeader := kv.rf.Start(command)
				if isLeader {
					kv.consensus(index)
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pullShardRoutine() {
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

func (kv *ShardKV) deleteShardRoutine() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.RLock()
			gidShardList := make(map[int][]int)
			for shard := range kv.state {
				if kv.state[shard].Status == Collection {
					gid := kv.lastConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.RUnlock()

			var waitGroup sync.WaitGroup
			for gid, shardList := range gidShardList {
				waitGroup.Add(1)
				go kv.sendDeleteShard(&waitGroup, gid, shardList)
			}
			waitGroup.Wait()
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) heartbeatRoutine() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			index, _, isLeader := kv.rf.Start(Operation{})
			if isLeader {
				kv.consensus(index)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *ShardKV) applyOperation(
	command Operation,
) {
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
}

func (kv *ShardKV) applyConfiguration(
	command Configuration,
) {
	nextConfig := command.Config
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
}

func (kv *ShardKV) applyPullShard(
	command PullShard,
) {
	state := command.State
	client := command.Client
	num := command.Num
	if num != kv.config.Num {
		return
	}

	for shard := range state {
		if kv.state[shard].Status == Pull {
			for k, v := range state[shard].State {
				kv.state[shard].Put(k, v)
			}
			kv.state[shard].Status = Collection
		}
	}

	for clientId, messageId := range client {
		if kv.client[clientId] < messageId {
			kv.client[clientId] = messageId
		}
	}
}

func (kv *ShardKV) applyDeleteShard(
	command DeleteShard,
) {
	shardList := command.ShardList
	num := command.Num
	if num != kv.config.Num {
		return
	}

	for _, shard := range shardList {
		if kv.state[shard].Status == Collection {
			kv.state[shard].Status = Default
		}

		if kv.state[shard].Status == Push {
			kv.state[shard] = Shard{
				Status: Default,
				State:  make(map[string]string),
			}
		}
	}
}

func (kv *ShardKV) sendPullShard(
	waitGroup *sync.WaitGroup,
	gid int,
	shardList []int,
) {
	for _, server := range kv.lastConfig.Groups[gid] {
		request := PullShardRequest{
			ShardList: shardList,
			Num:       kv.config.Num,
		}
		response := PullShardResponse{}

		ok := kv.makeEnd(server).Call("ShardKV.PullShard", &request, &response)
		if ok && response.Err == OK {
			state := response.State
			client := response.Client
			num := response.Num
			command := PullShard{
				State:  state,
				Client: client,
				Num:    num,
			}
			index, _, isLeader := kv.rf.Start(command)
			if isLeader {
				_, response.Err = kv.consensus(index)
			} else {
				response.Err = ErrWrongLeader
			}
			break
		}
	}
	waitGroup.Done()
}

func (kv *ShardKV) sendDeleteShard(
	waitGroup *sync.WaitGroup,
	gid int,
	shardList []int,
) {
	for _, server := range kv.lastConfig.Groups[gid] {
		request := DeleteShardRequest{
			ShardList: shardList,
			Num:       kv.config.Num,
		}
		response := DeleteShardResponse{}

		ok := kv.makeEnd(server).Call("ShardKV.DeleteShard", &request, &response)
		if ok && response.Err == OK {
			num := kv.config.Num
			command := DeleteShard{
				Num:       num,
				ShardList: shardList,
			}

			index, _, isLeader := kv.rf.Start(command)
			if isLeader {
				_, response.Err = kv.consensus(index)
			} else {
				response.Err = ErrWrongLeader
			}
			break
		}
	}
	waitGroup.Done()
}

func (kv *ShardKV) PullShard(
	request *PullShardRequest,
	response *PullShardResponse,
) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.config.Num < request.Num {
		response.Err = ErrFuture
		kv.mu.RUnlock()
		return
	}

	shardList := request.ShardList
	state := make(map[int]Shard)
	for _, shard := range shardList {
		state[shard] = Shard{
			State: kv.state[shard].Copy(),
		}
	}

	client := make(map[int64]int)
	for clientId, messageId := range kv.client {
		client[clientId] = messageId
	}

	response.State = state
	response.Client = client
	response.Num = kv.config.Num
	response.Err = OK
	kv.mu.RUnlock()
}

func (kv *ShardKV) DeleteShard(
	request *DeleteShardRequest,
	response *DeleteShardResponse,
) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.config.Num > request.Num {
		response.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	num := request.Num
	shardList := request.ShardList
	command := DeleteShard{
		Num:       num,
		ShardList: shardList,
	}
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		_, response.Err = kv.consensus(index)
	} else {
		response.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Operation(
	request *OperationRequest,
	response *OperationResponse,
) {
	clientId := request.ClientId
	messageId := request.MessageId
	key := request.Key
	value := request.Value
	method := request.Method
	shard := key2shard(key)

	kv.mu.RLock()
	if kv.staleShard(shard) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	command := Operation{
		ClientId:  clientId,
		MessageId: messageId,
		Key:       key,
		Value:     value,
		Method:    method,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}

	appliedCommand, err := kv.consensus(index)
	if err == ErrTimeout {
		response.Err = ErrTimeout
		return
	}

	operation, ok := appliedCommand.(Operation)
	if !ok {
		response.Err = ErrWrongLeader
		return
	}

	appliedClientId := operation.ClientId
	appliedMessageId := operation.MessageId
	if clientId != appliedClientId || messageId != appliedMessageId {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.staleShard(shard) {
		response.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	if method == "Get" {
		response.Value = kv.state[shard].Get(key)
	}
	kv.mu.RUnlock()
	response.Err = OK
}

func (kv *ShardKV) consensus(
	index int,
) (interface{}, Err) {
	kv.mu.Lock()
	kv.broadcastCh[index] = make(chan interface{}, 1)
	broadcastCh := kv.broadcastCh[index]
	defer kv.closeChannel(index)
	kv.mu.Unlock()

	select {
	case command := <-broadcastCh:
		return command, OK
	case <-time.After(500 * time.Millisecond):
		return nil, ErrTimeout
	}
}

func (kv *ShardKV) closeChannel(
	index int,
) {
	kv.mu.Lock()
	if broadcastCh, ok := kv.broadcastCh[index]; ok {
		close(broadcastCh)
		delete(kv.broadcastCh, index)
	}
	kv.mu.Unlock()
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
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxRaftState int,
	gid int,
	ctrlers []*labrpc.ClientEnd,
	makeEnd func(string) *labrpc.ClientEnd,
) *ShardKV {
	labgob.Register(Operation{})
	labgob.Register(Configuration{})
	labgob.Register(PullShard{})
	labgob.Register(DeleteShard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.makeEnd = makeEnd
	kv.gid = gid

	kv.ctrlers = ctrlers
	kv.clerk = shardctrler.MakeClerk(kv.ctrlers)

	kv.client = make(map[int64]int)
	kv.broadcastCh = make(map[int]chan interface{})
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
	go kv.configurationRoutine()
	go kv.pullShardRoutine()
	go kv.deleteShardRoutine()
	go kv.heartbeatRoutine()
	return kv
}
