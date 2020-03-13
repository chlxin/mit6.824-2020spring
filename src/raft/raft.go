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
	"encoding/json"
	"fmt"
	"labgob"
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
	voteForNobody = -1
	unknownLeader = -1
)

type state string

const (
	follower  state = "follower"
	candidate state = "candidate"
	leader    state = "leader"
)

// LogEntry 日志
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (le LogEntry) compareTo(another LogEntry) int {
	if le.Term > another.Term {
		return 1
	} else if le.Term < another.Term {
		return -1
	}
	if le.Index > another.Index {
		return 1
	} else if le.Index == another.Index {
		return 0
	} else {
		return -1
	}
}

func (le LogEntry) String() string {
	return fmt.Sprintf("[Index:%d, Term:%d]", le.Index, le.Term)
}

// consistentCheckResult 返回对象的语义为，
// 假如Success为true，后面属性没有任何意义
// 假如Success为false，那么假如Snapshot为true，意味着传进来的prevIndex已经被快照了; 假如Snapshot为false，那么看Conflict
// 假如conflict为true，那么返回冲突的index和term，假如为false，那么返回最新的日志的index和term
type consistentCheckResult struct {
	Success   bool
	Conflict  bool
	Snapshot  bool
	HintIndex int
	HintTerm  int
}

func (c consistentCheckResult) String() string {
	return fmt.Sprintf("Success:%v, Conflict:%v, ConflictIndex:%d, ConflictTerm:%d",
		c.Success, c.Conflict, c.HintIndex, c.HintTerm)
}

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
	CommandTerm  int
}

type RoleChange struct {
	IsLeader bool
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
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//config
	minElectionTimeout int64 // milliseconds
	maxElectionTimeout int64
	heartbeatInterval  int64
	// condition variable
	companignCond *sync.Cond
	heartbeatCond *sync.Cond
	//rate state
	role         state
	term         int
	voteFor      int
	leaderID     int
	commitIndex  int
	lastApplied  int
	nextIndexes  []int
	matchIndexes []int

	roleChangeCh         chan RoleChange
	lastHeartbeat        time.Time
	lastReceiveHeartbeat time.Time
	// logs related
	lastIncludedIndex int // for snapshot
	lastIncludedTerm  int
	logs              []LogEntry // at least one item, the index of entries should be monotonically increasing
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.term
	isleader = rf.role == leader

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
	data := rf.encodeHardState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.term = 0
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		rf.logs = []LogEntry{LogEntry{Index: 0, Term: 0, Command: nil}}
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var (
		term              int
		lastIncludedIndex int
		lastIncludedTerm  int
		voteFor           int
		logs              []LogEntry
	)

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logs) != nil {
		rf.fatalf("readPersist failed, decode error")
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.logs = logs
	}
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
	PrevLogIndex int        // index of log entry immediately preeding new ones
	PrevLogTerm  int        // term of prevIndex entry
	Entries      []LogEntry // maybe []byte is better?
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	bs, _ := json.Marshal(args.Entries)
	return fmt.Sprintf("Term:%d, LeaderID:%d, prevLogIndex:%d, prevLogTerm:%d, leaderCommit:%d, entries(%s)",
		args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, string(bs))
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	Snapshot  bool
	Conflict  bool // 假如Success返回false，那么会返回，来告诉是否log存在冲突
	HintIndex int
	HintTerm  int
}

type InstallSnapshotArgs struct {
	LeaderID          int
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.term > args.Term {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	if rf.term < args.Term {
		rf.term = args.Term
		rf.becomeFollower(unknownLeader)
	}

	if rf.voteFor == voteForNobody {
		incoming := LogEntry{Term: args.LastLogTerm, Index: args.LastLogIndex}
		local := rf.lastLog()
		if incoming.compareTo(local) >= 0 {
			DPrintf("me:%d, term:%d give the vote to [%d], local:%s, incoming:%s",
				rf.me, rf.term, args.CandidateID, local.String(), incoming.String())
			reply.Term = rf.term
			reply.VoteGranted = true
			rf.voteFor = args.CandidateID
			rf.lastReceiveHeartbeat = time.Now() // only give the vote can reset the timer
		}
	} else {
		reply.Term = rf.term
		reply.VoteGranted = false
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("me:%d, term:%d role:%s receive AppendEntries, args:%s", rf.me, rf.term, rf.role, args.String())
	if rf.term > args.Term {
		reply.Term = rf.term
		reply.Success = false
		return
	}
	rf.term = args.Term
	reply.Term = rf.term
	rf.becomeFollower(args.LeaderID)
	rf.lastReceiveHeartbeat = time.Now()

	checkRes := rf.consistentCheck(args.PrevLogIndex, args.PrevLogTerm)
	if !checkRes.Success {
		DPrintf("me:%d term:%d prevLogIndex:%d prevLogTerm:%d consistent check not pass, checkRes:%v",
			rf.me, rf.term, args.PrevLogIndex, args.PrevLogTerm, checkRes)
		if !checkRes.Snapshot && args.PrevLogIndex == 0 {
			// 假如送过来的是0，那么清除所有快照和log
			rf.clearSnapshot()
			goto Append
		}

		reply.Success = false
		reply.Snapshot = checkRes.Snapshot
		reply.Conflict = checkRes.Conflict
		reply.HintIndex = checkRes.HintIndex
		reply.HintTerm = checkRes.HintTerm
		return
	}

Append:
	rf.AppendLogs(args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	// DPrintf("[debug] me:%d term:%d appendLogs", rf.me, rf.term)
	// rf.persist()

	// DEBUG("[debug] me:%d term:%d reply yes", rf.me, rf.currentTerm)
	reply.Success = true
	rf.leaderID = args.LeaderID
	if args.LeaderCommit > rf.commitIndex {
		commit := args.LeaderCommit
		lastLog := rf.lastLog()
		if commit > lastLog.Index {
			commit = lastLog.Index
		}

		if rf.commitIndex > commit {
			rf.fatalf("commitIndex back")
		}
		rf.commitIndex = commit
		DPrintf("me:%d, term:%d passivity commit message commitID:%d. now commitIndex:%d", rf.me, rf.term, commit, rf.commitIndex)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.term > args.Term {
		reply.Term = rf.term
		return
	}

	rf.term = args.Term
	reply.Term = rf.term
	rf.becomeFollower(args.LeaderID)
	rf.lastReceiveHeartbeat = time.Now()

	rf.snapshotFromLeader(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)

	DPrintf("me:%d, term:%d install snapshot success, args:[lastIncludedIndex:%d, lastIncludedTerm:%d]",
		rf.me, rf.term, args.LastIncludedIndex, args.LastIncludedTerm)
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.role == leader
	term = rf.term
	if isLeader {

		index = rf.AppendLog(term, command)
		// rf.persist()
		rf.matchIndexes[rf.me] = index // 自己的match值永远最新
		DPrintf("me:%d term:%d start success index:%d", rf.me, rf.term, index)
	} else {
		ll := rf.lastLog()
		index = ll.Index + 1
	}

	return index, term, isLeader
}

func (rf *Raft) becomeFollower(leaderID int) {
	lastRole := rf.role

	rf.role = follower
	rf.leaderID = leaderID
	rf.voteFor = voteForNobody

	rf.nextIndexes = nil
	rf.matchIndexes = nil
	if lastRole == leader {
		rf.companignCond.Signal()
		if rf.roleChangeCh != nil {
			go func() {
				rf.roleChangeCh <- RoleChange{IsLeader: false}
			}()
		}
	}
}

func (rf *Raft) becomeCandidate() {
	rf.role = candidate
	rf.term++
	rf.leaderID = unknownLeader
	rf.voteFor = rf.me
	rf.nextIndexes = nil
	rf.matchIndexes = nil
	rf.lastReceiveHeartbeat = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.role = leader
	rf.leaderID = rf.me
	rf.nextIndexes = make([]int, len(rf.peers))
	rf.matchIndexes = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndexes[i] = rf.commitIndex + 1
		rf.matchIndexes[i] = 0
	}
	rf.lastHeartbeat = time.Now().Add(-time.Duration(2*rf.heartbeatInterval) * time.Millisecond)
	rf.heartbeatCond.Signal()
	if rf.roleChangeCh != nil {
		go func() {
			rf.roleChangeCh <- RoleChange{IsLeader: true}
		}()
	}
}

func (rf *Raft) timeout() (res time.Duration) {
	t := rf.maxElectionTimeout - rf.minElectionTimeout
	if t <= 0 {
		log.Fatalf("illegal timeout, %d, %d", rf.minElectionTimeout, rf.maxElectionTimeout)
	}
	rtimeout := rand.Int63n(t)

	res = time.Duration(rf.minElectionTimeout+rtimeout) * time.Millisecond
	return
}

// compaign background goroutine
func (rf *Raft) compaignPoll() {
	times := 0
Outer:
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()

		for rf.role == leader {
			rf.companignCond.Wait()
		}

		timeout := rf.timeout()
		elapsed := time.Now().Sub(rf.lastReceiveHeartbeat)
		for elapsed < timeout {
			// DEBUG("elapsed: %d", elapsed.Milliseconds())
			if rf.role == leader {
				rf.mu.Unlock()
				continue Outer
			}
			rf.mu.Unlock()
			interval := timeout - elapsed
			time.Sleep(interval)
			rf.mu.Lock()
			timeout = rf.timeout()
			elapsed = time.Now().Sub(rf.lastReceiveHeartbeat)
		}
		// candidate到这里可能就发现自己是leader了
		if rf.role == leader {
			rf.mu.Unlock()
			continue
		}
		// still hold lock
		// 到这里就已经选举超时了
		rf.becomeCandidate()
		times++
		DPrintf("me:%d term:%d, election times:%d", rf.me, rf.term, times)
		go rf.compaign()
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatPoll() {
	times := 0
Outer:
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		for rf.role != leader {
			rf.heartbeatCond.Wait()
		}
		interval := time.Duration(rf.heartbeatInterval) * time.Millisecond
		for elapsed := time.Now().Sub(rf.lastHeartbeat); elapsed < interval; {
			if rf.role != leader {
				rf.mu.Unlock()
				continue Outer
			}
			rf.mu.Unlock()
			time.Sleep(interval - elapsed)
			rf.mu.Lock()
			elapsed = time.Now().Sub(rf.lastHeartbeat)
		}
		if rf.role != leader {
			rf.mu.Unlock()
			continue
		}
		rf.lastHeartbeat = time.Now()
		times++
		DPrintf("me:%d, appendEntries times:%d", rf.me, times)
		go func() {
			rf.mu.Lock()
			peersLen := len(rf.peers)

			for i := 0; i < peersLen; i++ {
				if i == rf.me {
					continue
				}
				go rf.syncLogs(i)
			}
			rf.mu.Unlock()
		}()
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	DPrintf("me:%d term:%d start applier", rf.me, rf.term)
	rf.mu.Unlock()
	for {
		if rf.killed() {
			return
		}
		time.Sleep(90 * time.Millisecond)
		rf.mu.Lock()
		var (
			applyLogs    []LogEntry
			commandValid bool
			commitIndex  int
		)

		if rf.lastApplied < rf.lastIncludedIndex {
			// should install snapshot
			commandValid = false
			commitIndex = rf.lastIncludedIndex
			applyLogs = []LogEntry{{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm, Command: "installSnapshot"}}
		} else if rf.lastApplied < rf.commitIndex {
			commandValid = true
			commitIndex = rf.commitIndex
			applyLogs = rf.getLogsRange(rf.lastApplied+1, commitIndex+1)

			DPrintf("me:%d, term:%d apply message index[%d-%d]",
				rf.me, rf.term, applyLogs[0].Index, applyLogs[len(applyLogs)-1].Index)
		}
		rf.mu.Unlock()
		if len(applyLogs) > 0 {
			for _, applyLog := range applyLogs {
				msg := ApplyMsg{
					CommandValid: commandValid,
					Command:      applyLog.Command,
					CommandIndex: applyLog.Index,
					CommandTerm:  applyLog.Term,
				}
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied = commitIndex
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) RegisterRoleChangeNotify() chan RoleChange {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.roleChangeCh = make(chan RoleChange)
	return rf.roleChangeCh
}

func (rf *Raft) syncLogs(server int) {
	retry := true
	for retry {
		rf.mu.Lock()
		if rf.role != leader {
			rf.mu.Unlock()
			return
		}

		nextIndex := rf.nextIndexes[server]
		lastIncludedIndex := rf.lastIncludedIndex
		if nextIndex <= rf.lastIncludedIndex {
			// TODO: should send snapshot
			args := &InstallSnapshotArgs{
				Term:              rf.term,
				LeaderID:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}

			rf.mu.Unlock()
			var reply InstallSnapshotReply
			ok := rf.sendInstallSnapshot(server, args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.becomeFollower(unknownLeader)
				rf.mu.Unlock()
				return
			}
			if rf.role != leader {
				// 可能已经变成follower，也可能是之前已经成为了leader
				DPrintf("me:%d term:%d rf role is no longer roleLeader", rf.me, rf.term)
				rf.mu.Unlock()
				return
			}

			rf.nextIndexes[server] = lastIncludedIndex + 1
			rf.mu.Unlock()
		} else {
			// append entries
			lastLog := rf.lastLog()
			immediatelyNewIndex := lastLog.Index + 1

			entries := rf.getLogsRange(nextIndex, immediatelyNewIndex)
			pe := rf.prevLog(nextIndex)
			args := &AppendEntriesArgs{
				Term:         rf.term,
				LeaderID:     rf.me,
				PrevLogIndex: pe.Index,
				PrevLogTerm:  pe.Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()
			var reply AppendEntriesReply
			// DPrintf("me:%d sendAppendEntries to remote:%d", rf.me, server)
			ok := rf.sendAppendEntries(server, args, &reply)
			if !ok {
				// 网络问题或者超时，下次心跳再发，其实可以尝试3次，以后来填坑
				return
			}

			rf.mu.Lock()
			// 拿锁后先检查状态和term
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.becomeFollower(unknownLeader)
				rf.mu.Unlock()
				return
			}
			if rf.role != leader {
				// 可能已经变成follower，也可能是之前已经成为了leader
				DPrintf("me:%d term:%d rf role is no longer roleLeader", rf.me, rf.term)
				rf.mu.Unlock()
				return
			}
			retry = rf.handleAppendEntriesReply(server, lastLog, &reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, lastLog LogEntry, reply *AppendEntriesReply) (retry bool) {
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
			if c <= rf.lastIncludedIndex {
				DPrintf("me:%d, term:%d commit index: %d<=lastIncludedIndex, do not commit now",
					rf.me, rf.term, c)
			} else {
				ce := rf.logs[rf.indexOf(c)]

				// leader不能commit不属于自己term的日志
				if ce.Term != rf.term {
					DPrintf("me:%d, term:%d cannot commit index:%d, ce.Term:%v", rf.me, rf.term, c, ce.Term)
				} else {
					DPrintf("me:%d, term:%d commit message commitID:%d", rf.me, rf.term, c)
					if rf.commitIndex > c {
						rf.fatalf("commitIndex back")
					}
					rf.commitIndex = c
				}
			}

		}
	} else {
		retry = true
		if reply.Snapshot {
			rf.nextIndexes[server] = 1
		} else {
			if reply.Conflict {
				if reply.HintIndex == rf.nextIndexes[server] {
					// 如果找到的冲突的term最小的index就是，当前的next，那么就减少nextIndexes
					if rf.nextIndexes[server] > 1 { // 代码到这，这是肯定能保证的，一定为true
						rf.nextIndexes[server]--
					}
				} else {
					// 说明conflictIndex比rf.nextIndexes小
					rf.nextIndexes[server] = reply.HintIndex
				}

			} else {
				rf.nextIndexes[server] = reply.HintIndex + 1
			}
		}

	}

	return
}

func (rf *Raft) compaign() {
	rf.mu.Lock()
	lastLog := rf.lastLog()
	// args在后面全程不变
	args := &RequestVoteArgs{
		Term:         rf.term,
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

			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.becomeFollower(unknownLeader)
				return
			}
			if rf.role != candidate {
				// 可能已经变成follower，也可能是之前已经成为了leader
				DPrintf("me:%d term:%d rf role is no longer candidate", rf.me, rf.term)
				return
			}
			half := len(rf.peers)/2 + 1
			if reply.VoteGranted {
				DPrintf("me:%d ,term :%d got vote from server:%d", rf.me, rf.term, server)
				*count++
				if *count == half {
					DPrintf("me:%d, term :%d  got half:%d vote, become leader", rf.me, rf.term, half)
					rf.becomeLeader()
				}
			}
		}(rf, i, &count)
	}
	rf.mu.Unlock()
}

// logs ralate funcation

func (rf *Raft) AppendLog(term int, command interface{}) int {
	ll := rf.lastLog()
	e := LogEntry{
		Term:    term,
		Index:   ll.Index + 1,
		Command: command,
	}
	rf.logs = append(rf.logs, e)

	rf.persist()
	return e.Index
}

func (rf *Raft) AppendLogs(prevLogIndex int, prevLogTerm int, entries []LogEntry) {
	if len(entries) == 0 {
		return
	}

	checkRes := rf.consistentCheck(prevLogIndex, prevLogTerm)
	if !checkRes.Success {
		rf.fatalf("consistentCheck not pass")
	}

	// DEBUG("me:%d AppendLogs prevLogIndex:%d, prevLogTerm:%d, local:%s, incomings:%s",
	// rl.onwer, prevLogIndex, prevLogTerm, ENTRIES_STRING(rl.entries), ENTRIES_STRING(entries))

	// 到这里说明prevLogIndex在entries能找到，并且term也一致，因此直接删除之后的日志
	rf.logs = rf.logs[:rf.indexOf(prevLogIndex)+1]
	rf.logs = append(rf.logs, entries...)

	rf.persist()
}

// getLogsRange 获取一定区间的日志，左闭右开, index必须至少大于0，并且大于lastIncludedIndex，因此之前必须提前判断了
func (rf *Raft) getLogsRange(start, end int) []LogEntry {
	s := rf.indexOf(start)
	e := rf.indexOf(end)
	if s > e || s <= 0 || e > len(rf.logs) {
		rf.fatalf("getLogsRange illegal argument: [%d, %d]", start, end)
	}

	return rf.logs[s:e]
}

func (rf *Raft) prevLog(index int) LogEntry {
	if index <= rf.lastIncludedIndex {
		rf.fatalf("prevLog illegal index")
	}

	idx := rf.indexOf(index - 1)
	return rf.logs[idx]
}

func (rf *Raft) lastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

// indexOf 入参必须大于等于lastIncludedIndex
func (rf *Raft) indexOf(index int) int {
	if index < rf.lastIncludedIndex {
		rf.fatalf("illegal index:%d", index)
	}
	return index - rf.lastIncludedIndex
}

// snapshotFromLeader 必须在持有锁的情况下调用,从leader的installSnapshot rpc中过来调用，生成快照，一切以leader为准
func (rf *Raft) snapshotFromLeader(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) {
	// 直接删除整个logs
	DPrintf("me:%d, term:%d snapshotFromLeader, lastIncludedIndex:%d, lastIncludedTerm:%d", rf.me, rf.term, lastIncludedIndex, rf.lastIncludedTerm)
	DPrintf("snapshotFromLeader before, rf:%s; lastCommandIndex:%d", rf.String(), lastIncludedTerm)
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	ll := rf.lastLog()
	var reserverd []LogEntry
	if ll.Index > lastIncludedIndex {
		reserverd = rf.logs[rf.indexOf(lastIncludedIndex)+1:]
	}

	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})
	if len(reserverd) > 0 {
		rf.logs = append(rf.logs, reserverd...)
	}
	DPrintf("snapshotFromLeader after, rf:%s; lastCommandIndex:%d", rf.String(), lastIncludedTerm)
	data := rf.encodeHardState()

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) clearSnapshot() {
	rf.logs = []LogEntry{LogEntry{Index: 0, Term: 0, Command: nil}}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	data := rf.encodeHardState()

	rf.persister.SaveStateAndSnapshot(data, nil)
}

// Snapshot 给外部调用，不需要持有锁
func (rf *Raft) Snapshot(lastCommandIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.indexOf(lastCommandIndex)
	if index <= 0 {
		rf.fatalf("Snapshot failed, lastCommandInex:%d illegal", lastCommandIndex)
	}
	DPrintf("Snapshot before, rf:%s; lastCommandIndex:%d", rf.String(), lastCommandIndex)
	rf.logs = rf.logs[index:]
	rf.logs[0].Command = nil
	rf.lastIncludedIndex = rf.logs[0].Index
	rf.lastIncludedTerm = rf.logs[0].Term
	DPrintf("Snapshot after, rf:%s; lastCommandIndex:%d", rf.String(), lastCommandIndex)
	data := rf.encodeHardState()

	rf.persister.SaveStateAndSnapshot(data, snapshot)

}

// RaftStateSize 给外部获取当前raft log的大小，决定时候需要snapshot
func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}

// ReadSnapshot 给外部读取快照
func (rf *Raft) ReadSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.ReadSnapshot()
}

func (rf *Raft) encodeHardState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.logs)

	data := w.Bytes()
	return data
}

// consistentCheck prevLogIndex和lastIncludedIndex，lastIndex有三种可能
// prevLogIndex小于lastIncludedIndex或者等于，但是term不一样: 这种需要删除快照，所有的log以leader为准
// prevLogIndex大于等于lastIncludedIndex,但是小于等于lastIndex: 这种只要看prevIndex所在log的term是不是一致的
// prevLogIndex大于等于lastIncludedIndex,但是大于lastIndex: 这种返回lastLog的情况
func (rf *Raft) consistentCheck(prevLogIndex int, prevLogTerm int) *consistentCheckResult {
	res := &consistentCheckResult{}
	if prevLogIndex < rf.lastIncludedIndex ||
		(prevLogIndex == rf.lastIncludedIndex && prevLogTerm != rf.lastIncludedTerm) {
		res.Success = false
		res.Snapshot = true
		res.HintIndex = prevLogIndex
		res.HintTerm = prevLogTerm
		return res
	}

	// index >= 0
	index := rf.indexOf(prevLogIndex)
	if index >= len(rf.logs) {
		// 本地没有该index的日志
		res.Success = false
		res.Snapshot = false
		res.Conflict = false
		lastLog := rf.logs[len(rf.logs)-1]
		res.HintIndex = lastLog.Index
		res.HintTerm = lastLog.Term
		return res
	}

	e := rf.logs[index]
	if e.Term == prevLogTerm {
		res.Success = true
		return res
	}

	res.Success = false
	res.Snapshot = false
	res.Conflict = true
	// e.Index一定>=rf.lastIncludedIndex + 1,假如是rf.lastIncludedIndex上面就返回success了
	// 因此下面代码至少会走一次，如果是1，那么一定会设置res(因为index为0，term为0，其他的log的term一定大于0)。也就是说下面的if至少会走一次
	res.HintIndex = rf.logs[1].Index // 这里1的日志必定存在
	res.HintTerm = rf.logs[1].Term
	for i := rf.indexOf(e.Index); i >= 1; i-- {
		if rf.logs[i-1].Term != e.Term {
			res.HintIndex = rf.logs[i].Index
			res.HintTerm = rf.logs[i].Term
		}
	}
	// 确保到这里ConflictIndex肯定大于rf.LastIncludedIndex
	if res.HintIndex <= rf.lastIncludedIndex {
		rf.fatalf("illegal state, prevLogIndex:%d, prevLogTerm:%d", prevLogIndex, prevLogTerm)
	}
	return res
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

func (rf *Raft) fatalf(format string, a ...interface{}) {
	log.Println(stack())
	log.Println(rf.String())
	log.Fatalf(format, a...)
}

func (rf *Raft) String() string {
	bs, _ := json.Marshal(rf.logs)
	return fmt.Sprintf("[me:%d, term:%d, role:%s, vote:%d, commitIndex:%d, lastApplied:%d, lastIncludedIndex:%d, lastIncludedTerm:%d, logs:%s]",
		rf.me, rf.term, rf.role, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastIncludedTerm, string(bs))
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

	time.Sleep(time.Duration(rf.me%10) * time.Millisecond)
	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	// initialize from state persisted before a crash
	// readPersist set the term and logs
	rf.readPersist(persister.ReadRaftState())
	rf.minElectionTimeout = 800
	rf.maxElectionTimeout = 1200
	rf.heartbeatInterval = 100
	rf.companignCond = sync.NewCond(&rf.mu)
	rf.heartbeatCond = sync.NewCond(&rf.mu)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastReceiveHeartbeat = time.Now()
	// rf.becomeFollower(unknownLeader)
	rf.role = follower
	rf.mu.Unlock()

	go rf.compaignPoll()
	go rf.heartbeatPoll()
	go rf.applier()

	return rf
}
