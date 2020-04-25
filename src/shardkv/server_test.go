package shardkv

import (
	"fmt"
	"shardmaster"
	"testing"
)

func Test_copyConfig(t *testing.T) {
	c := shardmaster.Config{
		Num:    10,
		Shards: [10]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Groups: map[int][]string{1: []string{"server-1-0", "server-1-1"}},
	}
	cc := copyConfig(c)
	fmt.Println(cc)
}

func Test_11(t *testing.T) {
	sid := key2shard("0")
	fmt.Println(sid)
}
