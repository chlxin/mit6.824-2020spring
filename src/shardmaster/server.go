package shardmaster

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sort"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type notification struct {
	Term        int
	Value       Config
	WrongLeader bool
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	roleChangeCh  chan raft.RoleChange
	shutdown      chan struct{}
	configs       []Config                  // indexed by config num
	clients       map[int64]int             // key:clientId, value:已经处理过的最新的requestId
	notifyChanMap map[int]chan notification // 需要用mu来保护并发访问, key为index，一旦notify后就需要删除
}

const (
	OpNop   = "Nop"
	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
	OpQuery = "Query"
)

type Op struct {
	// Your data here.
	Op        string
	ClintID   int64
	RequestID int
	// Query
	Num int
	// Join
	Servers map[int][]string
	// leave
	GIDs []int
	// move
	Shard int
	GID   int
}

func (sm *ShardMaster) String() string {
	return fmt.Sprintf("me:%d , configs:%+v", sm.me, sm.configs)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// DPrintf("server:%d Join args:[servers:%v, Client:%d, requestID:%d]",
	// 	sm.me, args.Servers, args.ClientID, args.RequestID)
	op := Op{
		Op:        OpJoin,
		Servers:   args.Servers,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
	}
	wrongLeader, _ := sm.start(op)

	reply.WrongLeader = wrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// DPrintf("server:%d Leave args:[GIDS:%v, Client:%d, requestID:%d]",
	// 	sm.me, args.GIDs, args.ClientID, args.RequestID)
	op := Op{
		Op:        OpLeave,
		GIDs:      args.GIDs,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
	}
	wrongLeader, _ := sm.start(op)

	reply.WrongLeader = wrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// DPrintf("server:%d Move args:[Shard:%d, GID:%d, Client:%d, requestID:%d]",
	// sm.me, args.Shard, args.GID, args.ClientID, args.RequestID)
	op := Op{
		Op:        OpMove,
		Shard:     args.Shard,
		GID:       args.GID,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
	}
	wrongLeader, _ := sm.start(op)

	reply.WrongLeader = wrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// DPrintf("server:%d Query args:[Num:%d, Client:%d]", sm.me, args.Num, args.ClientID)
	op := Op{
		Op:      OpQuery,
		Num:     args.Num,
		ClintID: args.ClientID,
	}
	wrongLeader, value := sm.start(op)

	reply.WrongLeader = wrongLeader
	reply.Config = value
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
	sm.mu.Lock()
	sm.cleanup()
	sm.mu.Unlock()
}

func (sm *ShardMaster) cleanup() {
	for index, ch := range sm.notifyChanMap {
		reply := notification{
			Term:        0,
			WrongLeader: true,
		}
		delete(sm.notifyChanMap, index)
		ch <- reply
	}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// start 该方法的期望是使用一个独立的协程来处理，结果用notifyCh来传输，但是内部会加锁, 是不是没必要用channel接结果
func (sm *ShardMaster) start(op Op) (wrongLeader bool, value Config) {

	wrongLeader, value = false, emptyConfig

	index, term, ok := sm.rf.Start(op)
	if !ok {
		wrongLeader = true
		return
	}

	sm.mu.Lock()
	notifyCh := make(chan notification, 1)
	sm.notifyChanMap[index] = notifyCh
	sm.mu.Unlock()

	select {

	case result := <-notifyCh:
		if result.Term != term {
			wrongLeader, value = true, emptyConfig
		} else {
			wrongLeader, value = false, result.Value
		}
	}

	return

}

// service background goroutine to execute commands
func (sm *ShardMaster) service() {

	for {
		select {
		case <-sm.shutdown:
			DPrintf("server:%d shutdown sm server", sm.me)
			return
		case roleChange := <-sm.roleChangeCh:
			DPrintf("server:%d role change, isLeader：%v", sm.me, roleChange.IsLeader)
			sm.mu.Lock()
			term, isLeader := sm.rf.GetState()
			if isLeader {
				op := Op{
					Op:      OpNop,
					ClintID: 0,
				}
				go sm.start(op)
			} else {
				for index, ch := range sm.notifyChanMap {
					reply := notification{
						Term:        term,
						Value:       emptyConfig,
						WrongLeader: true,
					}
					delete(sm.notifyChanMap, index)
					ch <- reply
				}
			}
			sm.mu.Unlock()
		case msg := <-sm.applyCh:
			if msg.CommandValid {
				DPrintf("server:%d applyMsg, msg：[index:%d, command:%v]", sm.me, msg.CommandIndex, msg.Command)
				sm.applyMsg(msg)
			} else {
				// 可能是一些事件
				if cmd, ok := msg.Command.(string); ok {
					if cmd == "installSnapshot" {
						DPrintf("server:%d snapshot", sm.me)

					}
				}
			}

		}
	}

}

// applyMsg 必须在不持有sm.mu锁的情况下调用, 内部会尝试获取锁，否则会死锁
func (sm *ShardMaster) applyMsg(msg raft.ApplyMsg) {
	sm.mu.Lock()
	result := notification{Term: msg.CommandTerm, WrongLeader: false}

	op := msg.Command.(Op)
	cmd := op.Op
	if cmd == OpNop {
		// do nothing
	} else if cmd == OpQuery {
		c := sm.query(op.Num)
		// 需要复制c，因为c中的map原因是引用
		result.Value = c.copy()
	} else {
		// duplicate request suppress
		if sm.clients[op.ClintID] < op.RequestID {
			if cmd == OpJoin {
				nc := sm.join(op.Servers)
				sm.configs = append(sm.configs, nc)
			} else if cmd == OpLeave {
				nc := sm.leave(op.GIDs)
				sm.configs = append(sm.configs, nc)
			} else if cmd == OpMove {
				nc := sm.move(op.Shard, op.GID)
				sm.configs = append(sm.configs, nc)
			}
			sm.clients[op.ClintID] = op.RequestID
		}

	}
	DPrintf("applyMsg server state: [%s]", sm.String())
	sm.notifyIfPresent(msg.CommandIndex, result)
	sm.mu.Unlock()
}

// notifyIfPresent 必须持有kv.mu的锁情况下调用
func (sm *ShardMaster) notifyIfPresent(index int, reply notification) {
	if ch, ok := sm.notifyChanMap[index]; ok {
		delete(sm.notifyChanMap, index)
		ch <- reply
	}
}

func (sm *ShardMaster) join(servers map[int][]string) Config {
	current := sm.configs[len(sm.configs)-1]
	num := current.Num + 1
	groups := mergeMap(current.Groups, servers)
	shards := assignShards(groups)
	next := Config{
		Num:    num,
		Shards: shards,
		Groups: groups,
	}

	DPrintf("servers:%v, next:%v", servers, next)
	return next
}

func (sm *ShardMaster) leave(gIDs []int) Config {
	current := sm.configs[len(sm.configs)-1]
	num := current.Num + 1

	groups := mergeMap(current.Groups, nil)
	for _, gID := range gIDs {
		delete(groups, gID)
	}
	shards := assignShards(groups)
	next := Config{
		Num:    num,
		Shards: shards,
		Groups: groups,
	}

	return next
}

func (sm *ShardMaster) move(shard int, gID int) Config {
	current := sm.configs[len(sm.configs)-1]
	num := current.Num + 1
	if _, ok := current.Groups[gID]; !ok {
		log.Fatalf("move failed, config groups does not included gid:%d", gID)
	}

	groups := mergeMap(current.Groups, nil)
	shards := [NShards]int{}
	for i, v := range current.Shards {
		shards[i] = v
	}
	shards[shard] = gID

	next := Config{
		Num:    num,
		Shards: shards,
		Groups: groups,
	}

	return next
}

func (sm *ShardMaster) query(num int) Config {
	if num < 0 || num > len(sm.configs) {
		return sm.configs[len(sm.configs)-1]
	}

	return sm.configs[num]
}

func mergeMap(m map[int][]string, servers map[int][]string) map[int][]string {
	nm := make(map[int][]string)
	for k, v := range m {
		nm[k] = v
	}

	for k, v := range servers {
		if _, ok := nm[k]; ok {
			DPrintf("mergeMap, key:%d already exists", k)
		}
		vc := make([]string, len(v))
		copy(vc, v)
		nm[k] = vc
	}
	return nm
}

func assignShards(groups map[int][]string) [NShards]int {
	gs := make([]int, 0, len(groups))
	for k := range groups {
		gs = append(gs, k)
	}
	lenOfGs := len(gs)

	sort.Ints(gs)
	// 简单起见，使用取模的算法。
	// 假如有3个group, 那么shardsId % 3来分配groupsId
	shards := [NShards]int{}
	if lenOfGs > 0 {
		for i := 0; i < NShards; i++ {
			shards[i] = gs[i%lenOfGs]
		}
	}

	return shards
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.shutdown = make(chan struct{})
	sm.mu.Lock()
	sm.clients = make(map[int64]int)
	sm.notifyChanMap = make(map[int]chan notification)
	nc := sm.rf.RegisterRoleChangeNotify()
	sm.roleChangeCh = nc
	sm.mu.Unlock()
	go sm.service()

	return sm
}
