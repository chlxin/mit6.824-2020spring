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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

const (
	roleFollower  = 0
	roleCandidate = 1
	roleLeader    = 2
)

const (
	voteForNobody = -1
)

const (
	leaderUnknown = -1
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogHeader struct {
	Term  int
	Index int
}

type LogEntry struct {
	header  *LogHeader
	Command interface{}
}

var emptyLogEntry = &LogEntry{header: &LogHeader{Term: -1, Index: -1}, Command: nil}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	electionCond  *sync.Cond
	heartbeatCond *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// config
	ElectionTimeoutMin int64 // milliseconds
	ElectionTimeoutMax int64
	HeartbeatInterval  int64

	role                 int
	leaderID             int
	lastReceiveAppendRPC time.Time // for follower or candidate
	lastSendAppendRPC    time.Time // for leader

	// persistent state
	currentTerm int
	voteFor     int
	logs        []*LogEntry // 如何设计来支持更高效的查询
	// volatile state
	commitIndex int
	lastApplied int
	// volatile state on leader
	nextIndexes  []int // TODO: maybe struct is better? depend on if there are other information to store
	matchIndexes []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == roleLeader
	rf.mu.Unlock()

	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int         // index of log entry immediately preeding new ones
	PrevLogTerm  int         // term of prevIndex entry
	Entries      []*LogEntry // maybe []byte is better?
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.currentTerm = args.Term
	// may be we don't need check role

	if rf.voteFor == voteForNobody {
		incoming := &LogHeader{Term: args.LastLogTerm, Index: args.LastLogIndex}
		local := rf.getLastLog().header // 有问题，如果rf.logs是空就gg
		if checkIncomingUpToDate(local, incoming) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	// 2B: checkLog
	if args.LeaderCommit > rf.commitIndex {
		commit := args.LeaderCommit
		lastLog := rf.getLastLog()
		if commit > lastLog.header.Index {
			commit = lastLog.header.Index
		}
		rf.commitIndex = commit
	}

	rf.leaderID = args.LeaderID
	reply.Success = true // 2B: should add some logical

	rf.lastReceiveAppendRPC = time.Now() // important

}

// getLastLog 必须在持有rf.mu锁的情况下才能调用
func (rf *Raft) getLastLog() *LogEntry {
	lenOfLogs := len(rf.logs)
	if lenOfLogs == 0 {
		return emptyLogEntry
	}
	return rf.logs[lenOfLogs-1]
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 必须要持有rf.mu的锁才可以调用
func (rf *Raft) becomeFollower() {
	lastRole := rf.role

	rf.role = roleFollower
	rf.leaderID = leaderUnknown
	rf.voteFor = voteForNobody
	rf.nextIndexes = nil
	rf.matchIndexes = nil
	rf.lastReceiveAppendRPC = time.Now()
	// 如果从leader变成follower，必须要唤醒选举协程
	if lastRole == roleLeader {
		rf.electionCond.Signal()
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = roleLeader
	rf.leaderID = rf.me
	lens := len(rf.peers)
	rf.nextIndexes = make([]int, lens)

	rf.matchIndexes = make([]int, lens)
	rf.lastSendAppendRPC = time.Now()
	// 必须要唤醒心跳协程
	rf.heartbeatCond.Signal()
}

func (rf *Raft) becomeCandidate() {
	rf.role = roleCandidate
	rf.voteFor = voteForNobody
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.nextIndexes = nil
	rf.matchIndexes = nil
	rf.lastReceiveAppendRPC = time.Now()
}

func (rf *Raft) applier() {

}

func (rf *Raft) heartbeat() {
Outer:
	for {
		rf.mu.Lock()
		for rf.role != roleLeader {
			rf.heartbeatCond.Wait()
		}
		interval := time.Duration(rf.HeartbeatInterval) * time.Millisecond
		for elapsed := time.Now().Sub(rf.lastSendAppendRPC); elapsed < interval; {
			if rf.role != roleLeader {
				rf.mu.Unlock()
				continue Outer
			}
			rf.mu.Unlock()
			time.Sleep(interval - elapsed)
			rf.mu.Lock()
		}
		if rf.role != roleLeader {
			rf.mu.Unlock()
			continue
		}
		go rf.execHeartbeat()
		rf.lastSendAppendRPC = time.Now()
		rf.mu.Unlock()
	}
}

func (rf *Raft) election() {
Outer:
	for {
		// TODO: 需要检查raft应用是不是已经kill了
		rf.mu.Lock()

		for rf.role == roleLeader {
			rf.electionCond.Wait()
		}

		timeout := rf.nextElectionTimeout()
		for elapsed := time.Now().Sub(rf.lastReceiveAppendRPC); elapsed < timeout; {
			if rf.role == roleLeader {
				rf.mu.Unlock()
				continue Outer
			}
			rf.mu.Unlock()
			time.Sleep(timeout - elapsed)
			rf.mu.Lock()
		}
		// 理论上不会，不会从follower突然自己变成了leader，但是因为刚拿到锁，保险起见仍然check一下
		if rf.role == roleLeader {
			rf.mu.Unlock()
			continue
		}
		// still hold lock
		// 到这里就已经选举超时了
		rf.becomeCandidate()
		// TODO: 发送消息
		go rf.execElection()
		rf.mu.Unlock()
	}
}

func (rf *Raft) execElection() {
	rf.mu.Lock()
	lastLog := rf.getLastLog()
	// args在后面全程不变
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLog.header.Index,
		LastLogTerm:  lastLog.header.Term,
	}
	peersLen := len(rf.peers)
	count := 1
	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			continue
		}
		go func(rf *Raft, server int, count *int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, args, &reply)
			if !ok {
				log.Printf("[debug] sendRequestVote failed")
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.becomeFollower()
				return
			}
			if rf.role != roleCandidate {
				// 可能已经变成follower，也可能是之前已经成为了leader
				log.Printf("[debug] rf role is not roleCandidate")
				return
			}
			half := len(rf.peers)/2 + 1
			if reply.VoteGranted {
				*count++
				if *count == half {
					rf.becomeLeader()
				}
			}
		}(rf, i, &count)
	}
	rf.mu.Unlock()

}

func (rf *Raft) execHeartbeat() {
	rf.mu.Lock()
	peersLen := len(rf.peers)

	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			continue
		}
		go func(rf *Raft, server int, term int, me int, commitIndex int) {
			// TODO: 2A实验只需要选举，这里暂时就不先拿日志做文章， 2B时候来填坑
			// lastLog := rf.getLastLog()
			// match := rf.matchIndexes[server]
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     me,
				PrevLogIndex: -1,
				PrevLogTerm:  -1,
				// Entries
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				log.Printf("[debug] sendAppendEntries failed")
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.becomeFollower()
				return
			}
			if rf.role != roleLeader {
				// 可能已经变成follower，也可能是之前已经成为了leader
				log.Printf("[debug] rf role is not roleLeader")
				return
			}
			// TODO: 需要补充来更新matchIndex和nextIndex

		}(rf, i, rf.currentTerm, rf.me, rf.commitIndex)
	}
	rf.mu.Unlock()
}

func (rf *Raft) nextElectionTimeout() time.Duration {
	timeout := rf.ElectionTimeoutMax - rf.ElectionTimeoutMin
	if timeout <= 0 {
		log.Fatalf("illegal timeout, %d, %d", rf.ElectionTimeoutMin, rf.ElectionTimeoutMax)
	}
	rand.Seed(time.Now().Unix())
	rtimeout := rand.Int63n(timeout)

	return time.Duration(rtimeout) * time.Millisecond
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.electionCond = sync.NewCond(&rf.mu)
	rf.heartbeatCond = sync.NewCond(&rf.mu)
	rf.leaderID = leaderUnknown
	rf.ElectionTimeoutMin = 200
	rf.ElectionTimeoutMax = 400
	rf.HeartbeatInterval = 50

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.election()
	go rf.heartbeat()
	rf.becomeFollower()
	rf.mu.Unlock()
	return rf
}

// check incoming log is at least up-to-date
func checkIncomingUpToDate(local, incoming *LogHeader) bool {

	if incoming.Term > local.Term {
		return true
	}

	if incoming.Term == local.Term && incoming.Index >= local.Index {
		return true
	}

	return false
}
