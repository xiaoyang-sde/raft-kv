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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower           = 1
	Candidate          = 2
	Leader             = 3
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
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
}

type LogEntry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	Entries      []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	candidateId := args.CandidateId
	candidateTerm := args.Term

	if candidateTerm < rf.currentTerm {
		rf.debug(
			fmt.Sprintf("-- Stale Vote -> %d", rf.votedFor),
			"warning",
		)
		reply.Term = rf.currentTerm
		return
	}

	if candidateTerm > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = candidateTerm
	}

	if rf.votedFor == -1 || rf.votedFor == candidateId {
		rf.votedFor = candidateId
		reply.VoteGranted = true
		rf.debug(
			fmt.Sprintf("-- Vote -> %d", rf.votedFor),
			"success",
		)
	} else {
		rf.debug(
			fmt.Sprintf("-- Duplicate Vote -> %d", rf.votedFor),
			"warning",
		)
	}
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	leaderTerm := args.Term
	leaderId := args.LeaderId

	if leaderTerm < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if leaderTerm > rf.currentTerm {
		rf.currentTerm = leaderTerm
		rf.state = Follower
		rf.votedFor = -1
	}

	leaderCommit := args.LeaderCommit
	if rf.commitIndex < leaderCommit {
		rf.debug(fmt.Sprintf("-- Commit %d --", leaderCommit), "success")
		rf.commitIndex = leaderCommit
	}

	rf.lastHeartbeat = time.Now()

	leaderLogEntries := args.Entries
	if len(leaderLogEntries) == 0 {
		rf.debug(
			fmt.Sprintf("<- Heartbeat -- %d", leaderId),
			"success",
		)
		return
	}

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	if prevLogIndex >= len(rf.log) || rf.log[prevLogIndex].Term != prevLogTerm {
		rf.debug(
			fmt.Sprintf(
				"<- AppendEntries (%d >= %d or %d != %d) -- %d",
				prevLogIndex,
				len(rf.log),
				prevLogTerm,
				rf.log[prevLogIndex].Term,
				leaderId,
			),
			"error",
		)
		reply.Success = false
		return
	}

	diffIndex := 0
	for i := prevLogIndex + 1; i < len(rf.log); i++ {
		localLogEntry := rf.log[i]
		leaderLogIndex := i - prevLogIndex
		if leaderLogIndex >= len(leaderLogEntries) {
			break
		}
		if localLogEntry.Term == leaderLogEntries[leaderLogIndex].Term {
			diffIndex += 1
			continue
		}
		rf.log = rf.log[:i]
	}

	rf.log = append(rf.log, leaderLogEntries[diffIndex:]...)
	reply.Success = true

	rf.debug(
		fmt.Sprintf(
			"<- AppendEntries (%d - %v) -- %d",
			prevLogIndex+1,
			rf.log[prevLogIndex+1:],
			leaderId,
		),
		"success",
	)
}

func (rf *Raft) sendRequestVote(
	server int,
) {
	rf.mu.Lock()
	requestVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	requestVoteReply := RequestVoteReply{}
	rf.mu.Unlock()

	rf.sendRequestVoteRPC(
		server,
		&requestVoteArgs,
		&requestVoteReply,
	)

	rf.mu.Lock()
	rf.debug(
		fmt.Sprintf("-- RequestVote -> %d", server),
		"success",
	)

	voteGranted := requestVoteReply.VoteGranted
	if rf.state == Candidate && voteGranted {
		rf.voteCount += 1
		rf.debug(
			fmt.Sprintf("<- Vote -- %d", server),
			"success",
		)
	}

	if rf.state == Candidate && rf.voteCount == len(rf.peers)/2+1 {
		rf.debug("-- Leader --", "success")
		rf.state = Leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastApplied + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.sendHeartbeat(server)
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionRoutine() {
	for !rf.killed() {
		electionTimeout := time.Duration(
			HEART_BEAT_TIMEOUT*2+rand.Intn(HEART_BEAT_TIMEOUT),
		) * time.Millisecond

		rf.mu.Lock()
		if rf.state == Follower && time.Since(rf.lastHeartbeat) >= electionTimeout {
			rf.state = Candidate
		}

		if rf.state == Candidate {
			rf.currentTerm += 1
			rf.voteCount = 1
			rf.votedFor = rf.me
			rf.debug(
				fmt.Sprintf(
					"-- Election (Timeout: %d ms, Since Last: %d ms) --",
					electionTimeout.Milliseconds(),
					time.Since(rf.lastHeartbeat).Milliseconds(),
				),
				"success",
			)

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

		time.Sleep(electionTimeout)
	}
}

func (rf *Raft) sendHeartbeat(
	server int,
) {
	rf.mu.Lock()
	appendEntriesArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}

	appendEntriesReply := AppendEntriesReply{}
	rf.debug(
		fmt.Sprintf("-- Heartbeat -> %d", server),
		"success",
	)
	rf.mu.Unlock()

	rf.sendAppendEntriesRPC(
		server,
		&appendEntriesArgs,
		&appendEntriesReply,
	)
}

func (rf *Raft) heartbeatRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				go rf.sendHeartbeat(server)
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(
	server int,
	startIndex int,
) {
	rf.mu.Lock()
	prevLogIndex := startIndex - 1
	prevLogTerm := rf.log[startIndex-1].Term

	appendEntriesArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      rf.log[startIndex:],
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	appendEntriesReply := AppendEntriesReply{}
	rf.debug(
		fmt.Sprintf(
			"-- AppendEntries (%d - %v) -> %d",
			startIndex,
			rf.log[startIndex:],
			server,
		),
		"success",
	)
	rf.mu.Unlock()

	rf.sendAppendEntriesRPC(
		server,
		&appendEntriesArgs,
		&appendEntriesReply,
	)

	rf.mu.Lock()
	success := appendEntriesReply.Success
	if success {
		rf.nextIndex[server] = len(rf.log)
		rf.matchIndex[server] = len(rf.log) - 1
	} else {
		rf.nextIndex[server] -= 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) replicationRoutine() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state == Leader {
			lastLogIndex := len(rf.log) - 1
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				if lastLogIndex >= rf.nextIndex[server] {
					go rf.sendAppendEntries(server, lastLogIndex)
				}
			}

			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				if rf.log[i].Term != rf.currentTerm {
					continue
				}
				matchCount := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= i {
						matchCount += 1
					}
				}

				if matchCount >= len(rf.peers)/2 {
					rf.commitIndex = i
					break
				}
			}
			rf.debug(
				fmt.Sprintf(
					"-- %v %v --",
					rf.matchIndex,
					rf.nextIndex,
				),
				"success",
			)
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- applyMsg
			rf.debug(
				fmt.Sprintf("-- Apply %d --", i),
				"success",
			)
		}
		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
	}
	return ok
}

func (rf *Raft) sendAppendEntriesRPC(
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
	}
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
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendAppendEntries(server, index)
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) debug(
	message string,
	level string,
) {
	status := fmt.Sprintf(
		"[Instance %d][State %d][Term %d][Commit %d]",
		rf.me,
		rf.state,
		rf.currentTerm,
		rf.commitIndex,
	)

	if level == "success" {
		fmt.Println("\033[37m", status, message, "\033[0m")
	}
	if level == "error" {
		fmt.Println("\033[31m", status, message, "\033[0m")
	}
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
		Term: -1,
	})

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionRoutine()
	go rf.heartbeatRoutine()
	go rf.replicationRoutine()
	return rf
}
