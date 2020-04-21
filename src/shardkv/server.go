package shardkv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"labrpc"
	"log"
	"os"
	"os/signal"
	"runtime"
	"shardmaster"
	"sync/atomic"
	"syscall"
	"time"
)
import "raft"
import "sync"
import "labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	// for user operation
	Key       string
	Value     string
	ClintID   int64
	RequestID int
	ConfigNum int
	// for config change
	Config shardmaster.Config
	// for receive shard,
	Shards map[int]TransferShard
	// for finish sending
	TaskID int
	GID    int
}

func (op Op) String() string {
	return fmt.Sprintf("{Op:%s, Key:%s, Value:%s, ClintID:%d, RequestID:%d, ConfigNum:%d}",
		op.Op, op.Key, op.Value, op.ClintID, op.RequestID, op.ConfigNum)
}

func (op Op) isUserOp() bool {
	return op.Op == "Put" || op.Op == "Get" || op.Op == "Append"
}

type notification struct {
	Term  int
	Value string
	Err   Err
}

type ShardData struct {
	ID          int
	Responsible bool // 当前group是否需要为该分片提供服务
	Own         bool // 当前group是否拥有该分片最新的数据
	Dirty       bool // 当前是否已经被transfer过了
	Data        map[string]string
	Clients     map[int64]int // key:clientId, value:已经处理过的最新的requestId
	ConfigNum   int           // 上一次写入的ConfigNum
}

type transferTask struct {
	TaskID      int
	ConfigNum   int
	Groups      map[int][]string
	GidOfShards map[int][]transferTaskShard
}

type transferTaskShard struct {
	ShardID   int
	Dirty     bool
	Data      map[string]string
	Clients   map[int64]int
	ConfigNum int
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

	lastReceiveConfigTime time.Time
	isLeader              bool

	roleChangeCh  chan raft.RoleChange
	shutdown      chan struct{}
	currentConfig shardmaster.Config
	taskQueue     map[int]transferTask
	shardKvs      [shardmaster.NShards]*ShardData
	notifyChanMap map[int]chan notification // 需要用mu来保护并发访问, key为index，一旦notify后就需要删除
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server:%d, gid:%d Get args:[Key:%s, Client:%d, ConfigNum:%d]",
		kv.me, kv.gid, args.Key, args.ClientID, args.ConfigNum)
	op := Op{
		Op:        "Get",
		Key:       args.Key,
		ClintID:   args.ClientID,
		ConfigNum: args.ConfigNum,
	}

	err, value := kv.start(op)

	reply.Err = err
	reply.Value = value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("server:%d, gid:%d PutAppend args:[Key:%s, value:%s, Client:%d, requestID:%d, configNum:%d]",
	//	kv.me, kv.gid, args.Key, args.Value, args.ClientID, args.RequestID, args.ConfigNum)
	op := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClintID:   args.ClientID,
		RequestID: args.RequestID,
		ConfigNum: args.ConfigNum,
	}

	err, _ := kv.start(op)

	reply.Err = err
	DPrintf("server:%d, gid:%d PutAppend args:[Key:%s, value:%s, Client:%d, requestID:%d, configNum:%d], result:[%v]",
		kv.me, kv.gid, args.Key, args.Value, args.ClientID, args.RequestID, args.ConfigNum, err)
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.mu.Lock()
	DPrintf("server:%d, gid:%d Transfer args:[configNum:%d, data:%v, GID:%d]",
		kv.me, kv.gid, args.ConfigNum, args.Shards, args.GID)
	reply.Err = OK
	//for args.ConfigNum > kv.currentConfig.Num {
	//	kv.mu.Unlock()
	//	DPrintf("server:%d, gid:%d Transfer, currentConfig:[%v], args.ConfigNum:[%d]", kv.me, kv.gid, kv.currentConfig, args.ConfigNum)
	//	time.Sleep(120 * time.Millisecond)
	//	kv.mu.Lock()
	//}

	kv.mu.Unlock()

	op := Op{
		Op:        "receiveShards",
		GID:       args.GID,
		ConfigNum: args.ConfigNum,
		Shards:    args.Shards,
	}

	err, _ := kv.start(op)

	reply.Err = err
}

func (kv *ShardKV) sendTransfer(me int, configNum int, gid int,
	servers []string, data map[int]TransferShard, taskID int) {
	args := TransferArgs{
		ConfigNum: configNum,
		GID:       me,
		Shards:    data,
	}

	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply TransferReply
		ok := srv.Call("ShardKV.Transfer", &args, &reply)
		if ok && reply.Err == OK {
			return
		}

		if kv.killed() {
			goto Success
		}
		// ... not ok, or ErrWrongLeader
	}

Success:
	op := Op{
		Op:     "finishSendingData",
		TaskID: taskID,
		GID:    gid,
	}

	kv.start(op)
}

// start 该方法的期望是使用一个独立的协程来处理，结果用notifyCh来传输，但是内部会加锁, 是不是没必要用channel接结果
func (kv *ShardKV) start(op Op) (err Err, value string) {
	if kv.killed() {
		return ErrWrongLeader, ""
	}
	err, value = OK, ""

	kv.mu.Lock()

	for op.isUserOp() && op.ConfigNum > kv.currentConfig.Num {
		kv.mu.Unlock()
		if kv.killed() {
			return ErrWrongLeader, ""
		}
		DPrintf("server:%d gid:%d configNum outdated, incoming:%d, current:%d, op:%v", kv.me, kv.gid, op.ConfigNum, kv.currentConfig.Num, op)
		time.Sleep(1500 * time.Millisecond)
		kv.mu.Lock()
	}

	for op.isUserOp() && !kv.aliveToMaster() {
		kv.mu.Unlock()
		if kv.killed() {
			return ErrWrongLeader, ""
		}
		time.Sleep(1500 * time.Millisecond)
		DPrintf("server:%d gid:%d aliveToMaster false", kv.me, kv.gid)
		kv.mu.Lock()
	}

	// 要先判断幂等性，不然返回ErrWrongGroup后request会自增，就会重复执行
	i := key2shard(op.Key)
	shard := kv.shardKvs[i]
	if (op.Op == "Put" || op.Op == "Append") && shard.Clients[op.ClintID] >= op.RequestID {
		err = OK
		kv.mu.Unlock()
		return
	}

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
	time.Sleep(1000 * time.Millisecond)

	kv.mu.Lock()
	kv.initConfig()
	kv.mu.Unlock()

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
				op := Op{
					Op:      "nop",
					Key:     "",
					ClintID: 0,
				}
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
				//bs, _ := json.Marshal(msg.Command)
				//DPrintf("server:%d, gid:%d applyMsg, msg：[index:%d, command:%s]",
				//	kv.me, kv.gid, msg.CommandIndex, string(bs))
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

	op := msg.Command.(Op)
	cmd := op.Op
	if cmd == "nop" {
		// do notthing
	} else if cmd == "configChange" {
		result.Value, result.Err = kv.configChange(op)
	} else if cmd == "receiveShards" {
		result.Err = kv.receiveShards(op)
	} else if cmd == "finishSendingData" {
		result.Err = kv.finishSendingData(op)
	} else if cmd == "Get" {
		result.Value, result.Err = kv.get(op)
	} else {
		result.Value, result.Err = kv.putAppend(op, cmd)

	}

	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.snapshotIfNeeded(msg.CommandIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) putAppend(op Op, cmd string) (value string, err Err) {
	value, err = "", OK
	// Put和Append操作
	config := kv.currentConfig
	if op.ConfigNum < config.Num {
		err = ErrWrongGroup
	} else {
		if op.ConfigNum > config.Num {
			// 由于用户的请求在start之前都会确保client的configNum小于等于currentConfig，如果大了，服务器会等一会
			log.Printf("[BUG] server:%d, gid:%d, op.ConfigNum:%d > kv.currentConfig.Num:%d \n",
				kv.me, kv.gid, op.ConfigNum, config.Num)
		}
		// 重复请求不处理
		i := key2shard(op.Key)
		shard := kv.shardKvs[i]
		if shard.Clients[op.ClintID] < op.RequestID {
			if !shard.Responsible {
				DPrintf("gid:%d putAppend key:%s [NotResponsible], config:%v", kv.gid, op.Key, kv.currentConfig)
				err = ErrWrongGroup
				return
			}
			if !shard.Own {
				DPrintf("gid:%d putAppend key:%s [NotOwn], config:%v", kv.gid, op.Key, kv.currentConfig)
				err = ErrWrongGroup
				return
			}

			if cmd == "Put" {
				shard.Data[op.Key] = op.Value
			} else if cmd == "Append" {
				shard.Data[op.Key] += op.Value
			}
			shard.Clients[op.ClintID] = op.RequestID
			shard.ConfigNum = config.Num

		}
	}

	return
}

func (kv *ShardKV) get(op Op) (value string, err Err) {
	value, err = "", OK
	// Get 操作
	config := kv.currentConfig
	if op.ConfigNum < config.Num {
		err = ErrWrongGroup
		return
	} else {
		if op.ConfigNum > config.Num {
			// 由于用户的请求在start之前都会确保client的configNum小于等于currentConfig，如果大了，服务器会等一会
			log.Printf("[BUG] opConfigNum:%d > kv.currentConfig.Num:%d \n", op.ConfigNum, config.Num)
		}

		i := key2shard(op.Key)
		shard := kv.shardKvs[i]
		if !shard.Responsible {
			DPrintf("gid:%d get key:%s [NotResponsible], config:%v", kv.gid, op.Key, config)
			err = ErrWrongGroup
			return
		}
		if !shard.Own {
			DPrintf("gid:%d get key:%s [NotOwn], config:%v", kv.gid, op.Key, config)
			err = ErrWrongGroup
			return
		}

		value = shard.Data[op.Key]
	}

	return
}

func (kv *ShardKV) configChange(op Op) (value string, err Err) {
	value, err = "", OK
	// config change
	current := op.Config
	previous := kv.currentConfig

	if current.Num <= previous.Num {
		DPrintf("[bug] applyMsg incoming configNum:[%v] <= currentConfigNum:[%v]", current.Num, previous.Num)
		return
	}
	DPrintf("server:%d gid:%d config change, from %d to %d", kv.me, kv.gid, previous.Num, current.Num)
	kv.currentConfig = current
	// 需要diff
	for i := 0; i < len(kv.shardKvs); i++ {
		gid := current.Shards[i]
		if gid == kv.gid {
			// 新配置下需要负责该shard
			shard := kv.shardKvs[i]
			shard.Responsible = true

			if shard.Own {
				if shard.Dirty {
					shard.Own = false
				}
			}

		} else {
			shard := kv.shardKvs[i]
			shard.Responsible = false
		}
	}

	// transfer data
	config := kv.currentConfig

	shardMap := make(map[int][]*ShardData) // gid -> []shardData
	for sid, shard := range kv.shardKvs {
		if !shard.Responsible && shard.Own {
			gid := config.Shards[sid]

			if _, ok := shardMap[gid]; !ok {
				shardMap[gid] = make([]*ShardData, 0)
			}
			shardMap[gid] = append(shardMap[gid], shard)
		}
	}

	gidOfShards := make(map[int][]transferTaskShard)
	for gid, shards := range shardMap {
		allData := make([]transferTaskShard, 0)
		for _, shard := range shards {
			data := copyMap(shard.Data)
			tts := transferTaskShard{
				ShardID:   shard.ID,
				Dirty:     shard.Dirty,
				Data:      data,
				Clients:   copyIntMap(shard.Clients),
				ConfigNum: shard.ConfigNum,
			}
			allData = append(allData, tts)
			// 很重要，发送之前要把该分片标记成dirty
			shard.Dirty = true
		}

		gidOfShards[gid] = allData

	}

	if len(gidOfShards) == 0 {
		return
	}

	taskID := int(nrand())

	task := transferTask{
		TaskID:      taskID,
		Groups:      config.Groups,
		ConfigNum:   config.Num,
		GidOfShards: gidOfShards,
	}

	kv.taskQueue[taskID] = task

	return
}

func (kv *ShardKV) receiveShards(op Op) Err {
	config := kv.currentConfig

	gidOfShards := make(map[int][]transferTaskShard)
	for sid, shard := range op.Shards {
		// 发过来的数据更新
		if shard.ConfigNum >= kv.shardKvs[sid].ConfigNum {
			kv.shardKvs[sid].Data = copyMap(shard.Data)
			kv.shardKvs[sid].Clients = copyIntMap(shard.Clients)
			kv.shardKvs[sid].ConfigNum = shard.ConfigNum
			kv.shardKvs[sid].Dirty = false
		} else {
			DPrintf("server:%d, gid:%d shard config:%d < kv.shardKvs[%d].ConfigNum:%d from %d",
				kv.me, kv.gid, shard.ConfigNum, sid, kv.shardKvs[sid].ConfigNum, op.GID)
		}

		if !shard.Dirty {
			kv.shardKvs[sid].Own = true
			kv.shardKvs[sid].Dirty = false
		}

		toGid := config.Shards[sid]
		if toGid != kv.gid && kv.shardKvs[sid].Own {
			DPrintf("server:%d, gid:%d Transfer not belong to me, it belongs to:%d", kv.me, kv.gid, config.Shards[sid])
			if _, ok := gidOfShards[toGid]; !ok {
				gidOfShards[toGid] = make([]transferTaskShard, 0)
			}
			data := copyMap(kv.shardKvs[sid].Data)
			tts := transferTaskShard{
				ShardID:   kv.shardKvs[sid].ID,
				Dirty:     kv.shardKvs[sid].Dirty,
				Data:      data,
				Clients:   copyIntMap(kv.shardKvs[sid].Clients),
				ConfigNum: kv.shardKvs[sid].ConfigNum,
			}
			gidOfShards[toGid] = append(gidOfShards[toGid], tts)
		}

	}

	if len(gidOfShards) > 0 {
		taskID := int(nrand())

		task := transferTask{
			TaskID:      taskID,
			Groups:      config.Groups,
			ConfigNum:   config.Num,
			GidOfShards: gidOfShards,
		}

		kv.taskQueue[taskID] = task
	}

	return OK
}

func (kv *ShardKV) finishSendingData(op Op) Err {
	DPrintf("server:%d, gid:%d finishSendingData",
		kv.me, kv.gid)

	taskID := op.TaskID
	GID := op.GID
	task, ok := kv.taskQueue[taskID]
	if !ok {
		return OK
	}

	shards, ok := task.GidOfShards[GID]
	if !ok {
		return OK
	}
	// 可以垃圾回收了，TODO
	for _, shard := range shards {
		if !kv.shardKvs[shard.ShardID].Responsible {
			kv.shardKvs[shard.ShardID].Own = false
		}
	}

	delete(task.GidOfShards, GID)
	if len(task.GidOfShards) == 0 {
		delete(kv.taskQueue, taskID)
	}

	return OK
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
	e.Encode(kv.taskQueue)

	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapshot() {
	snapshot := kv.rf.ReadSnapshot()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var (
		config    shardmaster.Config
		shards    [shardmaster.NShards]*ShardData
		taskQueue map[int]transferTask
	)

	if d.Decode(&config) != nil ||
		d.Decode(&shards) != nil ||
		d.Decode(&taskQueue) != nil {
		log.Fatalf("readSnapshot failed, decode error")
	} else {
		kv.currentConfig = config
		kv.shardKvs = shards
		kv.taskQueue = taskQueue
	}
}

func (kv *ShardKV) periodicFetch() {
	// 事实上只有master来做这事有效, 可以继续优化
	for {
		select {
		case <-kv.shutdown:
			DPrintf("periodicFetch server:%d, gid:%d shutdown kv server", kv.me, kv.gid)
			return
		case <-time.After(100 * time.Millisecond):

			next := kv.mck.Query(-1)
			kv.mu.Lock()
			//DPrintf("server:%d, gid:%d try to fetch config, config:[%+v]", kv.me, kv.gid, c)
			kv.lastReceiveConfigTime = time.Now()
			if !kv.isLeader {
				kv.mu.Unlock()
				break
			}

			config := kv.currentConfig
			kv.mu.Unlock()

			if next.Num > config.Num {
				DPrintf("server:%d, gid:%d fetch latest config now, config:[%+v]", kv.me, kv.gid, next)
				op := Op{
					Op:     "configChange",
					Config: next,
				}
				kv.start(op)
			}

		}
	}
}

func (kv *ShardKV) periodicTransfer() {

	inflight := make(map[int]bool)

	for {
		select {
		case <-kv.shutdown:
			DPrintf("periodicTransfer server:%d, gid:%d shutdown kv server", kv.me, kv.gid)
			return
		case <-time.After(300 * time.Millisecond):

			kv.mu.Lock()
			if !kv.isLeader {
				kv.mu.Unlock()
				break
			}

			if len(kv.taskQueue) == 0 {
				kv.mu.Unlock()
				break
			}

			for _, task := range kv.taskQueue {
				if inflight[task.TaskID] {
					continue
				}

				gidOfShards := task.GidOfShards

				for gid, shards := range gidOfShards {
					allData := make(map[int]TransferShard)
					for _, shard := range shards {

						allData[shard.ShardID] = TransferShard{
							ID:        shard.ShardID,
							Dirty:     shard.Dirty,
							ConfigNum: shard.ConfigNum,
							Data:      shard.Data,
							Clients:   shard.Clients,
						}

					}
					inflight[task.TaskID] = true
					go kv.sendTransfer(kv.gid, task.ConfigNum, gid, task.Groups[gid], allData, task.TaskID)
				}

				bs, _ := json.Marshal(task)
				DPrintf("server:%d, gid:%d transfer data task :[%s]", kv.me, kv.gid, string(bs))

			}

			kv.mu.Unlock()

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

func (kv *ShardKV) aliveToMaster() bool {
	timeout := 500 * time.Millisecond
	elapsed := time.Now().Sub(kv.lastReceiveConfigTime)
	return elapsed < timeout
}

func (kv *ShardKV) initConfig() {
	var config shardmaster.Config

	for config.Num <= 0 {
		config = kv.mck.Query(1)
	}

	kv.currentConfig = config
	kv.lastReceiveConfigTime = time.Now()
	//kv.transferring = false
	for i, shard := range kv.shardKvs {
		if config.Shards[i] == kv.gid {
			shard.Responsible = true
			shard.Own = true
		} else {
			shard.Responsible = false
			shard.Own = false
		}
	}
	bs, _ := json.Marshal(kv.currentConfig)
	DPrintf("me:%d, gid:%d initConfig,config:(%s)", kv.me, kv.gid, string(bs))
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
	labgob.Register(Op{})
	labgob.Register(ShardData{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(TransferShard{})
	//labgob.Register(transferTask{})

	setupSigusr1Trap()

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
			ID:          i,
			Responsible: false,
			Own:         false,
			Data:        make(map[string]string),
			Clients:     make(map[int64]int),
		}
	}
	kv.shutdown = make(chan struct{})
	kv.mu.Lock()

	kv.isLeader = false
	kv.shardKvs = shards
	kv.taskQueue = make(map[int]transferTask)
	kv.notifyChanMap = make(map[int]chan notification)
	nc := kv.rf.RegisterRoleChangeNotify()
	kv.roleChangeCh = nc
	kv.mu.Unlock()
	go kv.service()
	go kv.periodicFetch()
	go kv.periodicTransfer()

	return kv
}

func setupSigusr1Trap() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for range c {
			DumpStacks()
		}
	}()
}
func DumpStacks() {
	buf := make([]byte, 16384)
	buf = buf[:runtime.Stack(buf, true)]
	fmt.Printf("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)
}
