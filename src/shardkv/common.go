package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrMovingShard   = "ErrMovingShard"
	ErrWaitingConfig = "ErrWaitingConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	RequestID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferArgs struct {
	ConfigNum int
	From      int
	Shard     TransferShard
}

type TransferReply struct {
	Err Err
}

type TransferShard struct {
	ID      int
	Data    map[string]string
	Clients map[int64]int
}
