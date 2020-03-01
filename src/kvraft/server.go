package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        string
	Key       string
	Value     string
	ClintID   int64
	RequestID int
}

type notifyMsg struct {
	Term  int
	Value string
	Err   Err
}

type startResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32 // set by Kill()
	shutdown chan struct{}

	maxraftstate int // snapshot if log grows this big

	commitIndex int
	// Your definitions here.
	kvs           map[string]string
	clients       map[int64]int          // key:clientId, value:已经处理过的最新的requestId
	notifyChanMap map[int]chan notifyMsg // 需要用mu来保护并发访问, key为index，一旦notify后就需要删除

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:        "Get",
		Key:       args.Key,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
	}
	resCh := make(chan startResult, 1)
	go kv.start(op, resCh)

	res := <-resCh

	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
	}
	resCh := make(chan startResult, 1)
	go kv.start(op, resCh)

	res := <-resCh

	reply.Err = res.Err

}

// start 该方法的期望是使用一个独立的协程来处理，结果用notifyCh来传输，但是内部会加锁
func (kv *KVServer) start(op Op, resCh chan startResult) {

	res := startResult{Err: OK, Value: ""}
	defer func() {
		resCh <- res
	}()
	index, term, ok := kv.rf.Start(op)
	if !ok {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := make(chan notifyMsg, 1)
	kv.notifyChanMap[index] = notifyCh
	kv.mu.Unlock()

	select {
	// case <-time.After(StartTimeoutInterval):
	// 	kv.Lock()
	// 	delete(kv.notifyChanMap, index)
	// 	kv.Unlock()
	// 	return ErrWrongLeader, ""
	case result := <-notifyCh:
		if result.Term != term {
			res.Err, res.Value = ErrWrongLeader, ""
		} else {
			res.Err, res.Value = result.Err, result.Value
		}
	}

}

// run background goroutine to execute commands
func (kv *KVServer) run() {

	for {
		select {
		case <-kv.shutdown:
			DPrintf("shutdown kv server")
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.applyMsg(msg)
			} else {
				// 可能是一些事件
				if cmd, ok := msg.Command.(string); ok {
					if cmd == "snapshot" {
						DPrintf("me:%d snapshot", kv.me)
					} else if cmd == "becomeLeader" {
						DPrintf("me:%d becomeLeader", kv.me)
						ch := make(chan startResult, 1)
						op := Op{Op: "nop"}
						go kv.start(op, ch)
					} else if cmd == "stepdown" {
						DPrintf("me:%d stepdown", kv.me)
						// TODO: 要清空所有的resCh
					}

				}
			}

		}
	}

}

// applyMsg 必须在不持有kv.mu锁的情况下调用, 内部会尝试获取锁，否则会死锁
func (kv *KVServer) applyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	result := notifyMsg{Term: msg.CommandTerm, Value: "", Err: OK}

	op := msg.Command.(Op)
	cmd := op.Op
	if cmd == "nop" {
		//
	} else if cmd == "Get" {
		result.Value = kv.kvs[op.Key]
	} else {
		if kv.clients[op.ClintID] < op.RequestID {
			if cmd == "Put" {
				kv.kvs[op.Key] = op.Value
			} else if cmd == "Append" {
				kv.kvs[op.Key] += op.Value
			}
			kv.clients[op.ClintID] = op.RequestID // TODO: 这个修改必须持有锁的情况，也就是applyMsg必须在持有锁的时候调用
		}
	}

	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.mu.Unlock()
}

// notifyIfPresent 必须持有kv.mu的锁情况下调用
func (kv *KVServer) notifyIfPresent(index int, reply notifyMsg) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		delete(kv.notifyChanMap, index)
		ch <- reply
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.shutdown = make(chan struct{})
	kv.mu.Lock()
	kv.kvs = make(map[string]string)
	kv.clients = make(map[int64]int)
	kv.notifyChanMap = make(map[int]chan notifyMsg)
	kv.mu.Unlock()
	go kv.run()
	return kv
}
