package shardctrler

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead      int32
	persister *raft.Persister
	client    map[int64]int
	clientCh  map[int]chan CommandArgs
	configs   []Config
}

func (sc *ShardCtrler) applyRoutine() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		sc.mu.Lock()

		commandValid := applyMsg.CommandValid
		if commandValid {
			command := applyMsg.Command.(CommandArgs)
			commandIndex := applyMsg.CommandIndex

			messageId := command.MessageId
			clientId := command.ClientId
			method := command.Method

			if sc.client[clientId] < messageId {
				lastConfig := sc.configs[len(sc.configs)-1]
				config := Config{
					Num:    lastConfig.Num + 1,
					Groups: make(map[int][]string),
				}

				for gid, servers := range lastConfig.Groups {
					config.Groups[gid] = make([]string, len(servers))
					copy(config.Groups[gid], servers)
				}

				for shard, gid := range lastConfig.Shards {
					config.Shards[shard] = gid
				}

				switch method {
				case "Join":
					joinServers := command.JoinServers
					for gid, servers := range joinServers {
						config.Groups[gid] = servers
					}

				case "Leave":
					leaveGIDs := command.LeaveGIDs
					for _, gid := range leaveGIDs {
						delete(config.Groups, gid)
					}

				case "Move":
					moveGID := command.MoveGID
					moveShard := command.MoveShard
					config.Shards[moveShard] = moveGID
				}

				if method == "Join" || method == "Leave" {
					gids := make([]int, 0)
					for gid := range config.Groups {
						gids = append(gids, gid)
					}

					if len(gids) > 0 {
						sort.Ints(gids)
						for shard := range config.Shards {
							config.Shards[shard] = gids[shard%len(gids)]
						}
					} else {
						for shard := range config.Shards {
							config.Shards[shard] = 0
						}
					}
				}

				if method != "Query" {
					sc.configs = append(sc.configs, config)
				}
				sc.client[clientId] = messageId
			}

			if clientCh, ok := sc.clientCh[commandIndex]; ok {
				if method != "Query" {
					DPrintf("[Controller][%d] %+v - broadcast\n", sc.me, command)
				}
				clientCh <- command
			}
		}

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) Command(
	args *CommandArgs,
	reply *CommandReply,
) {
	clientId := args.ClientId
	messageId := args.MessageId
	queryNum := args.QueryNum

	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	if args.Method != "Query" {
		DPrintf("[Controller][%d] %+v - start\n", sc.me, args)
	}

	sc.mu.Lock()
	sc.clientCh[index] = make(chan CommandArgs, 1)
	clientCh := sc.clientCh[index]
	sc.mu.Unlock()

	select {
	case appliedCommand := <-clientCh:
		appliedClientId := appliedCommand.ClientId
		appliedMessageId := appliedCommand.MessageId
		if clientId != appliedClientId || messageId != appliedMessageId {
			reply.WrongLeader = true
			return
		}

		if args.Method != "Query" {
			DPrintf("[Controller][%d] %+v - applied\n", sc.me, args)
		}
		sc.mu.Lock()
		if queryNum == -1 || queryNum >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[queryNum]
		}
		sc.mu.Unlock()
	case <-time.After(500 * time.Millisecond):
		if args.Method != "Query" {
			DPrintf("[Controller][%d] %+v - timeout\n", sc.me, args)
		}
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		close(clientCh)
		delete(sc.clientCh, index)
		sc.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.persister = persister

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	labgob.Register(CommandArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.client = make(map[int64]int)
	sc.clientCh = make(map[int]chan CommandArgs)

	go sc.applyRoutine()
	return sc
}
