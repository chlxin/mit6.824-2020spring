package kvraft

import (
	"bytes"
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

type notification struct {
	Term  int
	Value string
	Err   Err
}

type startResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	roleChangeCh  chan raft.RoleChange
	shutdown      chan struct{}
	kvs           map[string]string
	clients       map[int64]int             // key:clientId, value:已经处理过的最新的requestId
	notifyChanMap map[int]chan notification // 需要用mu来保护并发访问, key为index，一旦notify后就需要删除
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server:%d Get args:[Key:%s, Client:%d]", kv.me, args.Key, args.ClientID)
	op := Op{
		Op:      "Get",
		Key:     args.Key,
		ClintID: args.ClientID,
	}
	err, value := kv.start(op)

	reply.Err = err
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("server:%d PutAppend args:[Key:%s, value:%s, Client:%d, requestID:%d]", kv.me, args.Key, args.Value, args.ClientID, args.RequestID)
	op := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
	}
	err, _ := kv.start(op)

	reply.Err = err

}

// start 该方法的期望是使用一个独立的协程来处理，结果用notifyCh来传输，但是内部会加锁, 是不是没必要用channel接结果
func (kv *KVServer) start(op Op) (err Err, value string) {

	err, value = OK, ""

	index, term, ok := kv.rf.Start(op)
	if !ok {
		err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := make(chan notification, 1)
	kv.notifyChanMap[index] = notifyCh
	kv.mu.Unlock()

	select {

	case result := <-notifyCh:
		if result.Term != term {
			err, value = ErrWrongLeader, ""
		} else {
			err, value = result.Err, result.Value
		}
	}

	return

}

// service background goroutine to execute commands
func (kv *KVServer) service() {

	for {
		select {
		case <-kv.shutdown:
			DPrintf("server:%d shutdown kv server", kv.me)
			return
		case roleChange := <-kv.roleChangeCh:
			DPrintf("server:%d role change, isLeader：%v", kv.me, roleChange.IsLeader)
			kv.mu.Lock()
			term, isLeader := kv.rf.GetState()
			if isLeader {
				op := Op{
					Op:      "nop",
					Key:     "",
					ClintID: 0,
				}
				go kv.start(op)
			} else {
				for index, ch := range kv.notifyChanMap {
					reply := notification{
						Term:  term,
						Value: "",
						Err:   ErrWrongLeader,
					}
					delete(kv.notifyChanMap, index)
					ch <- reply
				}
			}
			kv.mu.Unlock()
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				DPrintf("server:%d applyMsg, msg：[index:%d, command:%v]", kv.me, msg.CommandIndex, msg.Command)
				kv.applyMsg(msg)
			} else {
				// 可能是一些事件
				if cmd, ok := msg.Command.(string); ok {
					if cmd == "installSnapshot" {
						DPrintf("server:%d snapshot", kv.me)
						kv.mu.Lock()
						kv.readSnapshot()
						kv.mu.Unlock()
					}
				}
			}

		}
	}

}

// applyMsg 必须在不持有kv.mu锁的情况下调用, 内部会尝试获取锁，否则会死锁
func (kv *KVServer) applyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	result := notification{Term: msg.CommandTerm, Value: "", Err: OK}

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
			kv.clients[op.ClintID] = op.RequestID
		}
	}

	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.snapshotIfNeeded(msg.CommandIndex)
	kv.mu.Unlock()
}

// notifyIfPresent 必须持有kv.mu的锁情况下调用
func (kv *KVServer) notifyIfPresent(index int, reply notification) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		delete(kv.notifyChanMap, index)
		ch <- reply
	}
}

func (kv *KVServer) snapshotIfNeeded(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	size := kv.rf.RaftStateSize()
	thredshold := int(1.5 * float64(kv.maxraftstate))
	if size >= thredshold {
		snapshot := kv.encodeHardState()
		kv.rf.Snapshot(index, snapshot)
	}
}

func (kv *KVServer) encodeHardState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvs)
	e.Encode(kv.clients)

	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapshot() {
	snapshot := kv.rf.ReadSnapshot()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var (
		kvs     map[string]string
		clients map[int64]int
	)

	if d.Decode(&kvs) != nil ||
		d.Decode(&clients) != nil {
		log.Fatalf("readSnapshot failed, decode error")
	} else {
		kv.kvs = kvs
		kv.clients = clients
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
	// 需要cleanup，清除一下notifyChanMap
	kv.mu.Lock()
	kv.cleanup()
	kv.mu.Unlock()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) cleanup() {
	for index, ch := range kv.notifyChanMap {
		reply := notification{
			Term:  0,
			Value: "",
			Err:   ErrWrongLeader,
		}
		delete(kv.notifyChanMap, index)
		ch <- reply
	}
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
	kv.notifyChanMap = make(map[int]chan notification)
	nc := kv.rf.RegisterRoleChangeNotify()
	kv.roleChangeCh = nc
	kv.mu.Unlock()
	go kv.service()

	return kv
}
