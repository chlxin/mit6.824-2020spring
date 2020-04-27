package shardkv

import (
	"log"
	"sync/atomic"
)

const Debug = 0
const LogMasterOnly = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func kvInfo(format string, kv *ShardKV, a ...interface{}) {
	state := kv.StateDescription()
	if LogMasterOnly && state != "Master" {
		return
	}
	if Debug > 0 {
		configNum := atomic.LoadInt64(&kv.configNum)
		args := append([]interface{}{kv.gid, kv.me, state, configNum}, a...)
		log.Printf("[INFO] KV Server: [GId: %d, Id: %d, %s, Config: %d] "+format, args...)
	}
}

func kvDebug(format string, kv *ShardKV, a ...interface{}) {
	state := kv.StateDescription()
	if LogMasterOnly && state != "Master" {
		return
	}
	if Debug > 1 {
		args := append([]interface{}{kv.gid, kv.me, state, kv.currentConfig.Num}, a...)
		log.Printf("[DEBUG] KV Server: [GId: %d, Id: %d, %s, Config: %d] "+format, args...)
	}
}
