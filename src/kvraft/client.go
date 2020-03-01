package kvraft

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

const RetryInterval = time.Duration(125 * time.Millisecond)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID  int64
	RequestID int
	leaderID  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderID = 0
	ck.clientID = nrand()
	ck.RequestID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
	}

	for {
		var reply GetReply
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf("call server %d Get key %s reply %#v", ck.leaderID, key, reply)
			return reply.Value
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.RequestID++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		RequestID: ck.RequestID,
	}
	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			if op == "Put" {
				DPrintf("call server %d seq %d PUT key %s value %s reply %v",
					ck.leaderID, args.RequestID, args.Key, args.Value, reply)
			} else {
				DPrintf("call server %d seq %d APPEND key %s value %s reply %v",
					ck.leaderID, args.RequestID, args.Key, args.Value, reply)
			}
			return
		}
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
