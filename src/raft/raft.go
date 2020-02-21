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
	"encoding/json"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"sort"
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

var (
	errLogsNotExist = fmt.Errorf("log not exist")
	errIllegalIndex = fmt.Errorf("illegal index")
	errIllegalState = fmt.Errorf("illegal stete")
	errShouldRetry  = fmt.Errorf("should retry append logs")
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

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (lh *LogEntry) compareTo(another *LogEntry) int {
	if lh.Term > another.Term {
		return 1
	} else if lh.Term < another.Term {
		return -1
	}
	if lh.Index > another.Index {
		return 1
	} else if lh.Index == another.Index {
		return 0
	} else {
		return -1
	}
}

var emptyLogEntry = &LogEntry{Term: 0, Index: 0, Command: nil}

type RaftLog struct {
	// raft日志有几个特征，index必须是从1开始的连续的且没有空桶，每个index日志同时记录term，term也应该是底层的
	// index为0的entry为一个空entry
	onwer     int
	mu        sync.RWMutex
	LastIndex int
	LastTerm  int
	entries   []*LogEntry
}

func NewRaftLog(owner int) *RaftLog {
	initEntries := make([]*LogEntry, 0)
	initEntries = append(initEntries, emptyLogEntry)
	return &RaftLog{
		onwer:     owner,
		LastIndex: 0,
		LastTerm:  0,
		entries:   initEntries, // 初始长度为1，因为0元素为空
	}
}

// GetEntries for debug
func (rl *RaftLog) GetEntries() []*LogEntry {
	return rl.entries
}

// GetLastLog 获取最新的日志
func (rl *RaftLog) GetLastLog() *LogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	lenOfLogs := len(rl.entries)
	return rl.entries[lenOfLogs-1]
}

func (rl *RaftLog) GetLogByIndex(index int) (*LogEntry, error) {
	if index < 1 { // index是从1开始的数字
		return nil, errIllegalIndex
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	idx := rl.calculateIndex(index)

	if idx >= len(rl.entries) {
		return nil, errLogsNotExist
	}
	e := rl.entries[idx]
	if e.Index != idx {
		log.Fatalf("[ERROR] index != log.Index")
		return nil, errIllegalState
	}
	return e, nil
}

func (rl *RaftLog) GetLogBetweenIndexes(start, end int) ([]*LogEntry, error) {
	if start > end {
		return nil, errIllegalIndex
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if start < 0 {
		return nil, errLogsNotExist
	}
	start = rl.calculateIndex(start)
	end = rl.calculateIndex(end)

	l := len(rl.entries)
	if end > l {
		return nil, errLogsNotExist
	}
	logs := rl.entries[start:end]
	return logs, nil
}

// AppendLog 追加一个日志，必须保证term比最新的log的term至少一样大
func (rl *RaftLog) AppendLog(term int, command interface{}) (int, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if term < rl.LastTerm {
		return -1, errIllegalIndex
	}
	rl.LastIndex++
	rl.LastTerm = term
	e := &LogEntry{
		Term:    term,
		Index:   rl.LastIndex,
		Command: command,
	}
	rl.entries = append(rl.entries, e)
	// DEBUG("[debug] AppendLog entries:%v", rl.entries)
	return e.Index, nil
}

// AppendLogs 从远程接受的log，无条件的接收
// 先做一致性检查，通过后，可以追加日志，以prevLogIndex为起点，之后的日志都以entries为准
func (rl *RaftLog) AppendLogs(prevLogIndex int, prevLogTerm int, entries []*LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	CHECK_LOGS(entries)

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if !rl.consistentCheck(prevLogIndex, prevLogTerm) {
		log.Fatalf("consistentCheck not pass")
	}

	DEBUG("me:%d AppendLogs prevLogIndex:%d, prevLogTerm:%d, local:%s, incomings:%s",
		rl.onwer, prevLogIndex, prevLogTerm, ENTRIES_STRING(rl.entries), ENTRIES_STRING(entries))

	// 到这里说明prevLogIndex在entries能找到，并且term也一致，因此直接删除之后的日志
	rl.entries = rl.entries[:prevLogIndex+1]
	rl.entries = append(rl.entries, entries...)
	CHECK_LOGS(rl.entries)
	lastEntry := rl.entries[len(rl.entries)-1]
	rl.LastIndex = lastEntry.Index
	rl.LastTerm = lastEntry.Term

	return nil
}

func (rl *RaftLog) CanAppend(prevLogIndex int, prevLogTerm int) bool {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.consistentCheck(prevLogIndex, prevLogTerm)
}

// consistentCheck raft日志的一致性检查，必须持有锁的情况下调用
func (rl *RaftLog) consistentCheck(prevLogIndex int, prevLogTerm int) bool {
	index := rl.calculateIndex(prevLogIndex)
	if index >= len(rl.entries) {
		return false
	}
	e := rl.entries[index]
	if e.Term == prevLogTerm {
		return true
	}
	return false
}

func (rl *RaftLog) calculateIndex(index int) int {
	return index
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh       chan ApplyMsg
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
	logs        *RaftLog // 如何设计来支持更高效的查询
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

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("Term:%d, CandidateID:%d, LastLogIndex:%d, LastLogTerm:%d",
		args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int         // index of log entry immediately preeding new ones
	PrevLogTerm  int         // term of prevIndex entry
	Entries      []*LogEntry // maybe []byte is better?
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	bs, _ := json.Marshal(args.Entries)
	return fmt.Sprintf("Term:%d, LeaderID:%d, prevLogIndex:%d, prevLogTerm:%d, leaderCommit:%d, entries(%s)",
		args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, string(bs))
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// var (
// 	r1 int64
// 	r2 int64
// )

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// atomic.AddInt64(&r1, 1)
	// fmt.Println("r1:", r1)

	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DEBUG("me:%d, term:%d receive RequestVote, args:%s", rf.me, rf.currentTerm, args.String())

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.becomeFollower(leaderUnknown)
	}

	// may be we don't need check role

	if rf.voteFor == voteForNobody {
		incoming := &LogEntry{Term: args.LastLogTerm, Index: args.LastLogIndex}
		local := rf.logs.GetLastLog()
		if checkIncomingUpToDate(local, incoming) {
			DEBUG("me:%d, term:%d give the vote to [%d], local:%s, incoming:%s",
				rf.me, rf.currentTerm, args.CandidateID, ENTRY_STRING(local), ENTRY_STRING(incoming))
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.voteFor = args.CandidateID
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

}

// AppendEntries 心跳或者追加日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// atomic.AddInt64(&r2, 1)
	// fmt.Println("r2:", r2)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("me:%d, term:%d role:%d receive AppendEntries, args:%s", rf.me, rf.currentTerm, rf.role, args.String())
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.becomeFollower(args.LeaderID)
	}
	rf.lastReceiveAppendRPC = time.Now() // important

	if !rf.logs.CanAppend(args.PrevLogIndex, args.PrevLogTerm) {
		DEBUG("me:%d term:%d prevLogIndex:%d prevLogTerm:%d consistent check not pass",
			rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		return
	}
	err := rf.logs.AppendLogs(args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	// DEBUG("[debug] me:%d term:%d appendLogs err:%v", rf.me, rf.currentTerm, err)
	if err != nil {
		log.Fatalf("AppendEntries failed, cause of AppendLogs failed")
	}

	// DEBUG("[debug] me:%d term:%d reply yes", rf.me, rf.currentTerm)
	reply.Success = true
	rf.leaderID = args.LeaderID
	if args.LeaderCommit > rf.commitIndex {
		commit := args.LeaderCommit
		lastLog := rf.logs.GetLastLog()
		if commit > lastLog.Index {
			commit = lastLog.Index
		}

		rf.commitIndex = commit
		DEBUG("me:%d, term:%d passivity commit message commitID:%d. now commitIndex:%d", rf.me, rf.currentTerm, commit, rf.commitIndex)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DEBUG("[debug] me:%d, term:%d, role:%d, Start command:%v", rf.me, rf.currentTerm, rf.role, command)
	isLeader = rf.role == roleLeader
	term = rf.currentTerm
	if isLeader {
		var err error
		index, err = rf.logs.AppendLog(term, command)
		if err != nil {
			log.Fatalf("[error] Start.AppendLog failed, err:%v", err)
		}
		rf.matchIndexes[rf.me] = index // 自己的match值永远最新
		DEBUG("me:%d term:%d start success index:%d", rf.me, rf.currentTerm, index)
	} else {
		ll := rf.logs.GetLastLog()
		index = ll.Index + 1
	}

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
func (rf *Raft) becomeFollower(leaderID int) {
	lastRole := rf.role

	rf.role = roleFollower
	rf.leaderID = leaderID
	rf.voteFor = voteForNobody
	rf.nextIndexes = nil
	rf.matchIndexes = nil
	rf.lastReceiveAppendRPC = time.Now()
	// 如果从leader变成follower，必须要唤醒选举协程
	if lastRole == roleLeader {
		rf.electionCond.Signal()
	}

	DEBUG("me:%d become follower, status: %s", rf.me, rf.String())
}

func (rf *Raft) becomeLeader() {
	rf.role = roleLeader
	rf.leaderID = rf.me
	lens := len(rf.peers)
	rf.nextIndexes = make([]int, lens)
	lastLog := rf.logs.GetLastLog()
	for i := 0; i < lens; i++ {
		rf.nextIndexes[i] = lastLog.Index + 1
	}
	rf.matchIndexes = make([]int, lens)
	// log.Println(rf.nextIndexes, rf.matchIndexes)
	rf.lastSendAppendRPC = time.Now().Add(-time.Duration(2*rf.HeartbeatInterval) * time.Millisecond)
	// 必须要唤醒心跳协程
	rf.heartbeatCond.Signal()
	entries := rf.logs.GetEntries()
	bs, _ := json.Marshal(entries)
	DEBUG("me:%d become Leader, status: %s, logs:%s", rf.me, rf.String(), string(bs))
}

func (rf *Raft) becomeCandidate() {
	rf.role = roleCandidate
	rf.leaderID = leaderUnknown
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.nextIndexes = nil
	rf.matchIndexes = nil
	rf.lastReceiveAppendRPC = time.Now()
	DEBUG("me:%d become Candidate, status: %s", rf.me, rf.String())
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	DEBUG("me:%d term:%d start applier", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for {
		time.Sleep(300 * time.Millisecond)
		rf.mu.Lock()
		var (
			applyLogs []*LogEntry
			err       error
		)
		commitIndex := rf.commitIndex
		if rf.lastApplied < commitIndex {
			applyLogs, err = rf.logs.GetLogBetweenIndexes(rf.lastApplied+1, commitIndex+1)
			if err != nil {
				DEBUG("[error] applier.GetLogBetweenIndexes[%d-%d] failed, err:%v",
					rf.lastApplied+1, commitIndex+1, err)
				rf.mu.Unlock()
				continue
			}
			DEBUG("me:%d, term:%d apply message index[%d-%d]",
				rf.me, rf.currentTerm, applyLogs[0].Index, applyLogs[len(applyLogs)-1].Index)
		}
		rf.mu.Unlock()
		if len(applyLogs) > 0 {
			for _, applyLog := range applyLogs {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      applyLog.Command,
					CommandIndex: applyLog.Index,
				}
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied = commitIndex
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	times := 0
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
			elapsed = time.Now().Sub(rf.lastSendAppendRPC)
		}
		if rf.role != roleLeader {
			rf.mu.Unlock()
			continue
		}
		rf.lastSendAppendRPC = time.Now()
		times++
		DEBUG("me:%d, appendEntries times:%d", rf.me, times)
		go rf.execHeartbeat()
		rf.mu.Unlock()
	}
}

func (rf *Raft) election() {
	times := 0
Outer:
	for {
		// TODO: 需要检查raft应用是不是已经kill了
		rf.mu.Lock()

		for rf.role == roleLeader {
			rf.electionCond.Wait()
		}

		timeout := rf.nextElectionTimeout()
		// DEBUG("me:%d begin election, timeout:%d", rf.me, timeout.Milliseconds())
		for elapsed := time.Now().Sub(rf.lastReceiveAppendRPC); elapsed < timeout; {
			// DEBUG("elapsed: %d", elapsed.Milliseconds())
			if rf.role == roleLeader {
				rf.mu.Unlock()
				continue Outer
			}
			rf.mu.Unlock()
			interval := timeout - elapsed
			time.Sleep(interval)
			rf.mu.Lock()
			elapsed = time.Now().Sub(rf.lastReceiveAppendRPC)
			// DEBUG("%d wakeup: %d, %d, %v", rf.me, elapsed.Milliseconds(), timeout.Milliseconds(), elapsed < timeout)
		}
		// candidate到这里可能就发现自己是leader了
		if rf.role == roleLeader {
			rf.mu.Unlock()
			continue
		}
		DEBUG("me:%d, role:%d election timeout now, ready to become candidiate", rf.me, rf.role)
		// still hold lock
		// 到这里就已经选举超时了
		rf.becomeCandidate()
		times++
		DEBUG("me:%d term:%d, election times:%d", rf.me, rf.currentTerm, times)
		go rf.execElection()
		rf.mu.Unlock()
	}
}

func (rf *Raft) execElection() {
	rf.mu.Lock()
	// DEBUG("[debug] me:%d  execElection", rf.me)
	lastLog := rf.logs.GetLastLog()
	// args在后面全程不变
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
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
				// DEBUG("[debug] sendRequestVote failed")
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.becomeFollower(leaderUnknown)
				return
			}
			if rf.role != roleCandidate {
				// 可能已经变成follower，也可能是之前已经成为了leader
				DEBUG("me:%d term:%d rf role is no longer roleCandidate", rf.me, rf.currentTerm)
				return
			}
			half := len(rf.peers)/2 + 1
			if reply.VoteGranted {
				DEBUG("me:%d ,term :%d got vote from server:%d", rf.me, rf.currentTerm, server)
				*count++
				if *count == half {
					DEBUG("me:%d, term :%d  got half:%d vote, become leader", rf.me, rf.currentTerm, half)
					rf.becomeLeader()
				}
			}
		}(rf, i, &count)
	}
	rf.mu.Unlock()

}

func (rf *Raft) execHeartbeat() {
	rf.mu.Lock()
	// log.Println("[debug] execHeartbeat")
	peersLen := len(rf.peers)

	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			continue
		}
		go func(rf *Raft, server int, me int) {
			retry := true
			for retry {
				rf.mu.Lock()
				if rf.role != roleLeader {
					rf.mu.Unlock()
					return
				}
				nextIndex := rf.nextIndexes[server]
				lastLog := rf.logs.GetLastLog()
				var (
					entries []*LogEntry
					err     error
				)
				immediatelyNewIndex := lastLog.Index + 1
				entries, err = rf.logs.GetLogBetweenIndexes(nextIndex-1, immediatelyNewIndex)
				if err != nil {
					DEBUG("[ERROR] GetLogBetweenIndexes err:%v, nextIndex:%d, immediatelyNewIndex:%d",
						err, nextIndex, immediatelyNewIndex)
					rf.mu.Unlock()
					return
				}
				fe := entries[0]
				entries = entries[1:]

				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     me,
					PrevLogIndex: fe.Index,
					PrevLogTerm:  fe.Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				var reply AppendEntriesReply
				// DEBUG("me:%d sendAppendEntries to remote:%d", rf.me, server)
				ok := rf.sendAppendEntries(server, args, &reply)
				if !ok {
					// DEBUG("[debug] sendAppendEntries failed")
					return
				}
				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.becomeFollower(leaderUnknown)
					rf.mu.Unlock()
					return
				}
				if rf.role != roleLeader {
					// 可能已经变成follower，也可能是之前已经成为了leader
					DEBUG("me:%d term:%d rf role is no longer roleLeader", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					retry = false
					rf.matchIndexes[server] = lastLog.Index
					rf.nextIndexes[server] = lastLog.Index + 1
					// set commit index
					matches := make([]int, len(rf.matchIndexes))
					copy(matches, rf.matchIndexes)
					sort.Ints(matches)
					c := matches[len(matches)/2]

					if c > rf.commitIndex {
						ce, err := rf.logs.GetLogByIndex(c)
						if err != nil {
							log.Fatalf("GetLogByIndex failed:%v", err)
						}
						// leader不能commit不属于自己term的日志
						if ce.Term != rf.currentTerm {
							DEBUG("me:%d, term:%d cannot commit index:%d, ce.Term:%v", rf.me, rf.currentTerm, c, ce.Term)
						} else {
							DEBUG("me:%d, term:%d commit message commitID:%d", rf.me, rf.currentTerm, c)
							rf.commitIndex = c
						}
					}

					// DEBUG("matches:%v, c:%d", matches, c)
				} else {
					// log.Printf("me%d, term:%d retry send to server:%d", rf.me, rf.currentTerm, server)
					retry = true
					if rf.nextIndexes[server] > 1 {
						rf.nextIndexes[server]--
					}
				}
				rf.mu.Unlock()

			}

		}(rf, i, rf.me)
	}
	rf.mu.Unlock()
}

func (rf *Raft) nextElectionTimeout() time.Duration {
	timeout := rf.ElectionTimeoutMax - rf.ElectionTimeoutMin
	if timeout <= 0 {
		log.Fatalf("illegal timeout, %d, %d", rf.ElectionTimeoutMin, rf.ElectionTimeoutMax)
	}
	// rand.Seed(time.Now().Unix())
	rtimeout := rand.Int63n(timeout)

	return time.Duration(rf.ElectionTimeoutMin+rtimeout) * time.Millisecond
}

func (rf *Raft) String() string {
	return fmt.Sprintf("term: %d, me: %d, voteFor:%d, leaderID:%d, role:%d, commitIndex:%d",
		rf.currentTerm, rf.me, rf.voteFor, rf.leaderID, rf.role, rf.commitIndex)
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.electionCond = sync.NewCond(&rf.mu)
	rf.heartbeatCond = sync.NewCond(&rf.mu)
	rf.leaderID = leaderUnknown
	rf.ElectionTimeoutMin = 700
	rf.ElectionTimeoutMax = 1100
	rf.HeartbeatInterval = 70
	rf.logs = NewRaftLog(me)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.election()
	go rf.heartbeat()
	go rf.applier()
	rf.becomeFollower(leaderUnknown)
	rf.mu.Unlock()
	return rf
}

// check incoming log is at least up-to-date
func checkIncomingUpToDate(local, incoming *LogEntry) bool {
	return incoming.compareTo(local) >= 0

}
