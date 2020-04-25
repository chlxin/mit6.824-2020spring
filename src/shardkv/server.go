package shardkv

import (
	"bytes"
	"encoding/json"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const (
	OpNop               = "nop"
	OpPut               = "Put"
	OpAppend            = "Append"
	OpGet               = "Get"
	OpConfigUpdate      = "ConfigUpdate"
	OpShardTransfer     = "ShardTransfer"
	OpMigrationComplete = "MigrationComplete"
)

type Op interface {
	getOpType() OpType
}

type EmptyOp struct{}

func (op EmptyOp) getOpType() OpType {
	return OpNop
}

type ClientOp struct {
	Cmd       OpType
	Key       string
	Value     string
	RequestID int
	ClientID  int64
}

func (op ClientOp) getOpType() OpType {
	return op.Cmd
}

type ConfigOp struct {
	Cmd    OpType
	Config shardmaster.Config
}

func (op ConfigOp) getOpType() OpType {
	return op.Cmd
}

type ShardTransferOp struct {
	Cmd   OpType
	Shard TransferShard
}

func (op ShardTransferOp) getOpType() OpType {
	return op.Cmd
}

type ShardState string

const (
	NotStore      ShardState = "NotStore"
	Available     ShardState = "Available"
	MigratingData ShardState = "Migrate"
	AwaitingData  ShardState = "Await"
)

type notification struct {
	Term  int
	Value string
	Err   Err
}

type ShardData struct {
	ID      int
	State   ShardState
	Data    map[string]string
	Clients map[int64]int // key:clientId, value:已经处理过的最新的requestId
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead         int32

	// Your definitions here.
	mck *shardmaster.Clerk

	isLeader bool

	roleChangeCh  chan raft.RoleChange
	shutdown      chan struct{}
	configNum     int64
	currentConfig shardmaster.Config
	shardKvs      [shardmaster.NShards]*ShardData
	notifyChanMap map[int]chan notification // 需要用mu来保护并发访问, key为index，一旦notify后就需要删除
}

func (kv *ShardKV) StateDescription() string {
	if kv.rf != nil {
		if _, isLeader := kv.rf.GetState(); isLeader {
			return "Master"
		}
	}
	return "Replica"
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server:%d, gid:%d Get args:[Key:%s, Client:%d]",
		kv.me, kv.gid, args.Key, args.ClientID)
	op := ClientOp{
		Cmd:      OpGet,
		Key:      args.Key,
		ClientID: args.ClientID,
	}

	err, value := kv.startClientOp(op)

	reply.Err = err
	reply.Value = value
	kvInfo("server:%d, gid:%d Get args:[Key:%s, clientID:%d], result:[%v, %s]", kv,
		kv.me, kv.gid, args.Key, args.ClientID, err, value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("server:%d, gid:%d PutAppend args:[Key:%s, value:%s, Client:%d, requestID:%d, configNum:%d]",
	//	kv.me, kv.gid, args.Key, args.Value, args.ClientID, args.RequestID, args.ConfigNum)
	var cmd OpType
	if args.Op == "Put" {
		cmd = OpPut
	} else if args.Op == "Append" {
		cmd = OpAppend
	} else {
		log.Fatalf("unsupport op:%s", args.Op)
	}
	op := ClientOp{
		Cmd:       cmd,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	err, _ := kv.start(op)

	reply.Err = err
	// kvInfo("server:%d, gid:%d PutAppend args:[Key:%s, value:%s, Client:%d, requestID:%d], result:[%v]", kv,
	// 	kv.me, kv.gid, args.Key, args.Value, args.ClientID, args.RequestID, err)
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kvInfo("receiving sid:%d from gid:%d, configNum:%d", kv, args.Shard.ID, args.From, args.ConfigNum)

	kv.mu.Lock()
	if kv.currentConfig.Num == args.ConfigNum {
		op := ShardTransferOp{
			Cmd: OpShardTransfer,
			Shard: TransferShard{
				ID:      args.Shard.ID,
				Data:    copyMap(args.Shard.Data),
				Clients: copyIntMap(args.Shard.Clients),
			},
		}
		kv.mu.Unlock()
		reply.Err, _ = kv.start(op)
	} else if kv.currentConfig.Num > args.ConfigNum {
		kv.mu.Unlock()
		reply.Err = OK
	} else {
		kv.mu.Unlock()
		reply.Err = ErrWaitingConfig
	}

}

func (kv *ShardKV) startClientOp(op ClientOp) (err Err, value string) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		err = ErrWrongLeader
		return
	}

	// 先查看shard的状态
	kv.mu.Lock()
	shard := kv.shardKvs[key2shard(op.Key)]
	switch shard.State {
	case NotStore:
		fallthrough
	case MigratingData:
		err = ErrWrongGroup
	case AwaitingData:
		err = ErrMovingShard
	case Available:
		err = OK

	}
	kv.mu.Unlock()

	if err != OK {
		return
	}

	// shard状态没问题可以apply
	err, value = kv.start(op)

	return
}

// start 该方法的期望是使用一个独立的协程来处理，结果用notifyCh来传输，但是内部会加锁, 是不是没必要用channel接结果
func (kv *ShardKV) start(op Op) (err Err, value string) {
	err, value = OK, ""

	kv.mu.Lock()

	index, term, ok := kv.rf.Start(op)
	if !ok {
		err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

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
func (kv *ShardKV) service() {
	DPrintf("service gid:%d server:%d begin kv server", kv.gid, kv.me)
	for {
		select {
		case <-kv.shutdown:
			DPrintf("service server:%d shutdown kv server", kv.me)
			return
		case roleChange := <-kv.roleChangeCh:
			DPrintf("server:%d gid:%d role change, isLeader：%v", kv.me, kv.gid, roleChange.IsLeader)
			kv.mu.Lock()
			term, isLeader := kv.rf.GetState()
			if isLeader {
				op := EmptyOp{}
				kv.isLeader = true
				go kv.start(op)
			} else {
				kv.isLeader = false
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
				// bs, _ := json.Marshal(msg.Command)
				// kvInfo("applyMsg, msg：[index:%d, command:%s]", kv,
				// 	msg.CommandIndex, string(bs))
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
func (kv *ShardKV) applyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	result := notification{Term: msg.CommandTerm, Value: "", Err: OK}

	switch op := msg.Command.(type) {
	case ClientOp:
		result.Value, result.Err = kv.applyClientOp(op)
	case ConfigOp:
		result.Err = kv.applyConfigOp(op)
	case ShardTransferOp:
		result.Err = kv.applyShardTransferOp(op)
	default:
		// do nothing
		kvInfo("nop", kv)
	}

	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.snapshotIfNeeded(msg.CommandIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) applyClientOp(op ClientOp) (value string, err Err) {
	// shard的状态实际上在start之前就已经check过，但是此处仍然需要判断
	i := key2shard(op.Key)
	shard := kv.shardKvs[i]
	switch shard.State {
	case NotStore:
		fallthrough
	case MigratingData:
		err = ErrWrongGroup
	case AwaitingData:
		err = ErrMovingShard
	case Available:
		err = OK

	}
	if err != OK {
		return
	}

	opType := op.getOpType()
	// 过滤重复的请求
	if opType == OpPut || opType == OpAppend {
		if shard.Clients[op.ClientID] >= op.RequestID {
			return
		}
	}

	if opType == OpPut {
		shard.Data[op.Key] = op.Value
		shard.Clients[op.ClientID] = op.RequestID
	} else if opType == OpAppend {
		shard.Data[op.Key] += op.Value
		shard.Clients[op.ClientID] = op.RequestID
	} else if opType == OpGet {
		value = shard.Data[op.Key]
	}

	return
}

func (kv *ShardKV) applyConfigOp(op ConfigOp) (err Err) {
	// configOp 永远不会失效，但是可能没有任何作用，config操作是一个有副作用的操作，副作用不行保证仅执行一次

	// 先检查所有的shard，如果有正在迁移或者等待的shard，那么就什么都不做
	err = OK
	shardTransferInProgress := false
	for _, shard := range kv.shardKvs {
		if shard.State == MigratingData || shard.State == AwaitingData {
			shardTransferInProgress = true
			break
		}
	}

	if shardTransferInProgress {
		// kvInfo("configNum:%d want to apply, but shardTransferInProgress", kv, op.Config.Num)
		err = ErrMovingShard
		return
	}

	// 只变更当前configNum+1的config, 到这里能保证所有的shard的状态要么是NotStore要么是Available
	if op.Config.Num != kv.currentConfig.Num+1 {
		return
	}
	kv.currentConfig = copyConfig(op.Config)
	bs, _ := json.Marshal(kv.currentConfig)
	kvInfo("apply configNum:%d, config:%s", kv, kv.currentConfig.Num, string(bs))
	atomic.StoreInt64(&kv.configNum, int64(kv.currentConfig.Num))

	for sid, gid := range kv.currentConfig.Shards {
		shard := kv.shardKvs[sid]
		if gid == kv.gid && shard.State == NotStore {
			kvInfo("Configuration change: Now owner of shard %d", kv, sid)
			shard.State = AwaitingData
			// 由于是异步创建并初始化shard，有可能还没成功server就被kill
			go kv.createShardIfNeeded(sid, kv.currentConfig)
		} else if gid != kv.gid && shard.State == Available {
			kvInfo("Configuration change: No longer owner of shard %d", kv, sid)
			shard.State = MigratingData
			go kv.transferShard(sid, kv.currentConfig)
		}
	}

	return
}

func (kv *ShardKV) applyShardTransferOp(op ShardTransferOp) (err Err) {
	err = OK
	opType := op.getOpType()
	switch opType {
	case OpMigrationComplete:
		shard := kv.shardKvs[op.Shard.ID]
		shard.State = NotStore
		shard.Clients = make(map[int64]int)
		shard.Data = make(map[string]string)

	case OpShardTransfer:
		shard := kv.shardKvs[op.Shard.ID]
		if shard.State == AwaitingData {
			shard.Data = copyMap(op.Shard.Data)
			shard.Clients = copyIntMap(op.Shard.Clients)
			shard.State = Available
			kvInfo("Data for shard: %d successfully received", kv, op.Shard.ID)
		} else {
			kvInfo("Data for shard: %d state is not AwaitingData", kv, op.Shard.ID)
		}
	}
	return
}

func (kv *ShardKV) createShardIfNeeded(sid int, config shardmaster.Config) {
	// 进入到这个方法的前提是当前没有sid的shard的数据，即状态为NotStore
	lastConfig := config
	for lastConfig.Num > 1 && lastConfig.Shards[sid] == kv.gid {
		lastConfig = kv.mck.Query(lastConfig.Num - 1)
	}

	if lastConfig.Num == 1 && lastConfig.Shards[sid] == kv.gid {
		kv.mu.Lock()
		kvInfo("Creating new shard: %d", kv, sid)
		kv.createShard(sid)
		kv.mu.Unlock()
	} else {
		kvInfo("Awaiting data for shard: %d from %d [configNum:%d]", kv, sid, lastConfig.Shards[sid], config.Num)
	}
}

// deleteShard 内部有可能阻塞的操作，期望在一个单独的协程中跑
func (kv *ShardKV) transferShard(sid int, config shardmaster.Config) {
	// 这里只有leader才能往下走，去与其他raft group的leader来沟通。
	// 此处更严谨的做法是循环不断看当前shard的状态并查看自己是否变成了leader，防止其他本group的leader由于网络原因不是leader了
	// 但是太复杂了，先不这么搞
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	for {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		shard := kv.shardKvs[sid]
		args := TransferArgs{
			ConfigNum: config.Num,
			From:      kv.gid,
			Shard: TransferShard{
				ID:      sid,
				Data:    copyMap(shard.Data),
				Clients: copyIntMap(shard.Clients),
			},
		}
		kv.mu.Unlock()
		servers := config.Groups[config.Shards[sid]]
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply TransferReply
			ok := srv.Call("ShardKV.Transfer", &args, &reply)
			kvInfo("[configNum:%d] Sending shard %d to client: %s, ok:%v, err:%v",
				kv, config.Num, sid, servers[si], ok, reply.Err)
			if ok && reply.Err == OK {
				kvInfo("Shard: %d successfully transferred", kv, sid)
				err, _ := kv.start(ShardTransferOp{Cmd: OpMigrationComplete, Shard: TransferShard{ID: sid}})
				if err != OK {
					kvInfo("ShardTransferOp failed", kv)
				}
				return
			}
			if reply.Err == ErrWaitingConfig {
				time.Sleep(1 * time.Second)
			}

			// ... not ok, or ErrWrongLeader
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) createShard(sid int) {
	shard := kv.shardKvs[sid]
	shard.State = Available
	shard.Clients = make(map[int64]int)
	shard.Data = make(map[string]string)
}

// notifyIfPresent 必须持有kv.mu的锁情况下调用
func (kv *ShardKV) notifyIfPresent(index int, reply notification) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		delete(kv.notifyChanMap, index)
		ch <- reply
	}
}

func (kv *ShardKV) snapshotIfNeeded(index int) {
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

func (kv *ShardKV) encodeHardState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.currentConfig)
	e.Encode(kv.shardKvs)

	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapshot() {
	snapshot := kv.rf.ReadSnapshot()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var (
		config shardmaster.Config
		shards [shardmaster.NShards]*ShardData
	)

	if d.Decode(&config) != nil ||
		d.Decode(&shards) != nil {
		log.Fatalf("readSnapshot failed, decode error")
	} else {
		kv.currentConfig = config
		atomic.StoreInt64(&kv.configNum, int64(kv.currentConfig.Num))
		kv.shardKvs = shards
		DPrintf("gid:%d server:%d readsnpashot, config:%d", kv.gid, kv.me, kv.currentConfig.Num)
	}

}

func (kv *ShardKV) periodicFetch() {

	DPrintf("service gid:%d server:%d begin periodicFetch", kv.gid, kv.me)

	for {
		select {
		case <-kv.shutdown:
			DPrintf("periodicFetch server:%d, gid:%d shutdown kv server", kv.me, kv.gid)
			return
		case <-time.After(100 * time.Millisecond):

			config := kv.mck.Query(-1)
			kv.mu.Lock()

			if !kv.isLeader {
				kv.mu.Unlock()
				break
			}

			//查到的configNuym比当前要大，要apply这些config的变化，但是变化需要限制一个config一个变
			if lastConfigNum := kv.currentConfig.Num; lastConfigNum < config.Num {
				kvInfo("Batch applying configs from %d -> %d", kv, lastConfigNum, config.Num)
				kv.mu.Unlock()

				for i := lastConfigNum + 1; i <= config.Num; i++ {
					var c shardmaster.Config
					if i == config.Num {
						c = config
					} else {
						c = kv.mck.Query(i)
					}

					var err Err
					for err != OK {
						op := ConfigOp{Cmd: OpConfigUpdate, Config: c}
						kv.start(op)
						kv.mu.Lock()
						if kv.currentConfig.Num == c.Num {
							err = OK
						}
						kv.mu.Unlock()
						time.Sleep(50 * time.Millisecond)
					}
				}
			} else {
				kv.mu.Unlock()
			}

		}
	}
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
	// Your code here, if desired.
	close(kv.shutdown)
	// 需要cleanup，清除一下notifyChanMap
	DPrintf("server:%d, gid:%d, begin cleanup", kv.me, kv.gid)
	kv.mu.Lock()
	DPrintf("server:%d, gid:%d, cleanup", kv.me, kv.gid)
	kv.cleanup()
	kv.mu.Unlock()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) cleanup() {
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

func copyMap(m map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyIntMap(m map[int64]int) map[int64]int {
	res := make(map[int64]int)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyConfig(c shardmaster.Config) shardmaster.Config {
	shards := [shardmaster.NShards]int{}
	group := make(map[int][]string)

	for i, s := range c.Shards {
		shards[i] = s
	}

	for gid, servers := range c.Groups {
		s := make([]string, len(servers))
		copy(s, servers)
		group[gid] = s
	}

	return shardmaster.Config{
		Num:    c.Num,
		Shards: shards,
		Groups: group,
	}
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
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("server:%d, gid:%d, start Server", me, gid)
	labgob.Register(EmptyOp{})
	labgob.Register(ClientOp{})
	labgob.Register(ConfigOp{})
	labgob.Register(ShardTransferOp{})
	labgob.Register(ShardData{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(TransferShard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	var shards [shardmaster.NShards]*ShardData
	for i := range shards {
		shards[i] = &ShardData{
			ID:      i,
			State:   NotStore,
			Data:    make(map[string]string),
			Clients: make(map[int64]int),
		}
	}
	kv.shutdown = make(chan struct{})
	kv.mu.Lock()

	kv.isLeader = false
	if data := persister.ReadSnapshot(); data != nil && len(data) > 0 {
		kv.readSnapshot()
	} else {
		kv.shardKvs = shards
	}

	kv.notifyChanMap = make(map[int]chan notification)
	nc := kv.rf.RegisterRoleChangeNotify()
	kv.roleChangeCh = nc

	// 处理异常退出遗留的问题
	for _, shard := range kv.shardKvs {
		if shard.State == MigratingData {
			kvInfo("sid:%d continue to transfer", kv, shard.ID)
			go kv.transferShard(shard.ID, kv.currentConfig)
		} else if shard.State == AwaitingData {
			kvInfo("sid:%d continue to waiting", kv, shard.ID)
			go kv.createShardIfNeeded(shard.ID, kv.currentConfig)
		}
	}

	kv.mu.Unlock()
	go kv.service()
	go kv.periodicFetch()

	DPrintf("Started group: %d, on node: %d", gid, me)

	return kv
}
