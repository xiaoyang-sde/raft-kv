package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower           = 0
	Candidate          = 1
	Leader             = 2
	HEART_BEAT_TIMEOUT = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state         int       // the state of the current instance
	currentTerm   int       // latest term server has seen
	votedFor      int       // candidateId that received vote in current term
	voteCount     int       // the number of votes received in the current term
	lastHeartbeat time.Time // the timestamp of the last heartbeat message

	log         []LogEntry    // log entries
	commitIndex int           // index of highest log entry known to be committed
	lastApplied int           // index of highest log entry applied to state machine
	nextIndex   []int         // inidex of the next log entry to send to each server
	matchIndex  []int         // index of highest log entry known to be replicated on each server
	applyCh     chan ApplyMsg // the channel to send ApplyMsg
	applyCond   *sync.Cond    // the condition variable for the applyRoutine

	snapshot []byte
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Success       bool
	Term          int
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetIndex(
	index int,
) int {
	return index - rf.log[0].Index
}

func (rf *Raft) GetLogEntry(
	index int,
) LogEntry {
	if index < 0 {
		index += rf.log[0].Index + len(rf.log)
	}
	return rf.log[rf.GetIndex(index)]
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(
	state []byte,
	snapshot []byte,
) {
	if state == nil || len(state) < 1 {
		return
	}

	reader := bytes.NewBuffer(state)
	decoder := labgob.NewDecoder(reader)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if decoder.Decode(&currentTerm) == nil {
		rf.currentTerm = currentTerm
	} else {
		return
	}

	if decoder.Decode(&votedFor) == nil {
		rf.votedFor = votedFor
	} else {
		return
	}

	if decoder.Decode(&log) == nil {
		rf.log = log
		rf.lastApplied = rf.log[0].Index
		rf.commitIndex = rf.log[0].Index
	} else {
		return
	}

	rf.snapshot = snapshot
}

//
// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(
	lastIncludedTerm int,
	lastIncludedIndex int,
	snapshot []byte,
) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	log := make([]LogEntry, 0)
	log = append(log, LogEntry{
		Command: -1,
		Term:    lastIncludedTerm,
		Index:   lastIncludedIndex,
	})

	if lastIncludedIndex <= rf.GetLogEntry(-1).Index && rf.GetLogEntry(lastIncludedIndex).Term == lastIncludedTerm {
		rf.log = append(log, rf.log[rf.GetIndex(lastIncludedIndex)+1:]...)
	} else {
		rf.log = log
	}

	rf.snapshot = snapshot
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(
	index int,
	snapshot []byte,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if index < rf.log[0].Index {
		return
	}

	log := make([]LogEntry, 0)
	log = append(log, LogEntry{
		Command: -1,
		Term:    rf.GetLogEntry(index).Term,
		Index:   rf.GetLogEntry(index).Index,
	})
	rf.log = append(log, rf.log[rf.GetIndex(index+1):]...)
	rf.snapshot = snapshot
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	candidateId := args.CandidateId
	candidateTerm := args.Term

	if candidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if candidateTerm > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = candidateTerm
	}
	reply.Term = rf.currentTerm

	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm
	localLastLogIndex := rf.GetLogEntry(-1).Index
	localLastLogTerm := rf.GetLogEntry(-1).Term

	if candidateLastLogTerm < localLastLogTerm {
		return
	}

	if candidateLastLogTerm == localLastLogTerm && candidateLastLogIndex < localLastLogIndex {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == candidateId {
		rf.lastHeartbeat = time.Now()
		rf.votedFor = candidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	leaderTerm := args.Term

	if leaderTerm < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if leaderTerm > rf.currentTerm {
		rf.currentTerm = leaderTerm
		rf.votedFor = -1
	}

	rf.state = Follower
	rf.lastHeartbeat = time.Now()

	leaderLogEntries := args.Entries
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm

	if prevLogIndex < rf.log[0].Index {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if prevLogIndex > rf.GetLogEntry(-1).Index {
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.GetLogEntry(-1).Index + 1
		return
	}

	if rf.GetLogEntry(prevLogIndex).Term != prevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictTerm = rf.GetLogEntry(prevLogIndex).Term

		for i := prevLogIndex; i >= rf.log[0].Index; i -= 1 {
			if rf.GetLogEntry(i).Term != reply.ConflictTerm {
				break
			}
			reply.ConflictIndex = i
		}
		return
	}

	for i := 0; i < len(leaderLogEntries); i++ {
		logIndex := prevLogIndex + i + 1
		if logIndex <= rf.GetLogEntry(-1).Index && rf.GetLogEntry(-1).Term != leaderLogEntries[i].Term {
			rf.log = rf.log[:rf.GetIndex(logIndex)]
		}
		if logIndex > rf.GetLogEntry(-1).Index {
			rf.log = append(rf.log, leaderLogEntries[i])
		}
	}

	leaderCommit := args.LeaderCommit
	if rf.commitIndex < leaderCommit {
		rf.commitIndex = Min(rf.GetLogEntry(-1).Index, leaderCommit)
		rf.applyCond.Signal()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) InstallSnapshot(
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	leaderTerm := args.Term
	snapshot := args.Data
	done := args.Done
	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm

	if leaderTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if leaderTerm > rf.currentTerm {
		rf.currentTerm = leaderTerm
		rf.votedFor = -1
	}

	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	if lastIncludedIndex <= rf.commitIndex {
		return
	}

	if done {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  lastIncludedTerm,
			SnapshotIndex: lastIncludedIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}
}

func (rf *Raft) sendRequestVote(
	server int,
) {
	rf.mu.RLock()
	if rf.state != Candidate {
		rf.mu.RUnlock()
		return
	}

	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.GetLogEntry(-1).Index,
		LastLogTerm:  rf.GetLogEntry(-1).Term,
	}
	requestVoteReply := RequestVoteReply{}
	rf.mu.RUnlock()

	rf.sendRequestVoteRPC(
		server,
		&requestVoteArgs,
		&requestVoteReply,
	)

	rf.mu.Lock()

	term := requestVoteReply.Term
	voteGranted := requestVoteReply.VoteGranted

	if term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = term
		rf.persist()
	}

	if rf.state != Candidate || term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if voteGranted {
		rf.voteCount += 1
	}

	if rf.voteCount == len(rf.peers)/2+1 {
		rf.state = Leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.GetLogEntry(-1).Index + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.sendAppendEntries(server)
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(
	server int,
) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	startIndex := rf.nextIndex[server]
	if startIndex <= rf.log[0].Index {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := startIndex - 1
	prevLogTerm := rf.GetLogEntry(prevLogIndex).Term

	entries := make(
		[]LogEntry,
		len(rf.log[rf.GetIndex(startIndex):]),
	)
	copy(entries, rf.log[rf.GetIndex(startIndex):])

	appendEntriesArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	appendEntriesReply := AppendEntriesReply{}

	rf.mu.RUnlock()
	ok := rf.sendAppendEntriesRPC(
		server,
		&appendEntriesArgs,
		&appendEntriesReply,
	)
	if !ok {
		return
	}
	rf.mu.Lock()

	success := appendEntriesReply.Success
	term := appendEntriesReply.Term
	conflictTerm := appendEntriesReply.ConflictTerm
	conflictIndex := appendEntriesReply.ConflictIndex

	if term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = term
		rf.persist()
	}

	if rf.state != Leader || term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if success {
		rf.matchIndex[server] = prevLogIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		for i := rf.GetLogEntry(-1).Index; i > rf.commitIndex; i-- {
			if rf.GetLogEntry(i).Term != rf.currentTerm {
				continue
			}
			matchCount := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= i {
					matchCount += 1
				}
			}

			if matchCount > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.applyCond.Signal()
				break
			}
		}
	} else if conflictIndex != 0 {
		rf.nextIndex[server] = conflictIndex
		if conflictTerm > 0 {
			for i := rf.GetLogEntry(-1).Index; i > rf.log[0].Index; i -= 1 {
				if rf.GetLogEntry(i).Term == conflictTerm {
					rf.nextIndex[server] = i + 1
					break
				}
			}
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(
	server int,
) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	installSnapshotArgs := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Offset:            0,
		Data:              rf.snapshot,
		Done:              true,
	}
	installSnapshotReply := InstallSnapshotReply{}

	rf.mu.RUnlock()
	ok := rf.sendInstallSnapshotRPC(
		server,
		&installSnapshotArgs,
		&installSnapshotReply,
	)
	if !ok {
		return
	}
	rf.mu.Lock()

	term := installSnapshotReply.Term
	if term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = term
		rf.persist()
	}

	if rf.state != Leader || term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.matchIndex[server] = rf.log[0].Index
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.mu.Unlock()
}

func (rf *Raft) electionRoutine() {
	for !rf.killed() {
		electionTimeout := time.Duration(
			HEART_BEAT_TIMEOUT*2+rand.Intn(HEART_BEAT_TIMEOUT),
		) * time.Millisecond

		rf.mu.Lock()

		if rf.state != Leader && time.Since(rf.lastHeartbeat) >= electionTimeout {
			rf.state = Candidate
			rf.currentTerm += 1
			rf.voteCount = 1
			rf.votedFor = rf.me
			rf.lastHeartbeat = time.Now()
			rf.persist()

			rf.mu.Unlock()

			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				go rf.sendRequestVote(server)
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applyRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex := rf.commitIndex
		queue := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			queue = append(queue, ApplyMsg{
				CommandValid: true,
				Command:      rf.GetLogEntry(i).Command,
				CommandIndex: rf.GetLogEntry(i).Index,
			})
		}
		rf.mu.Unlock()

		for _, applyMsg := range queue {
			rf.applyCh <- applyMsg
		}

		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicationRoutine() {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.state == Leader {
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				if rf.nextIndex[server] <= rf.log[0].Index {
					rf.mu.RUnlock()
					go rf.sendInstallSnapshot(server)
				} else {
					rf.mu.RUnlock()
					go rf.sendAppendEntries(server)
				}
				rf.mu.RLock()
			}
		}
		rf.mu.RUnlock()
		time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVoteRPC(
	server int,
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotRPC(
	server int,
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.GetLogEntry(-1).Index + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()

		rf.mu.Unlock()
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.sendAppendEntries(server)
		}
		rf.mu.Lock()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Command: 0,
		Term:    0,
		Index:   0,
	})
	rf.snapshot = make([]byte, 0)

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(
		persister.ReadRaftState(),
		persister.ReadSnapshot(),
	)

	go rf.electionRoutine()
	go rf.replicationRoutine()
	go rf.applyRoutine()
	return rf
}
