package shardmaster

import (
	"fmt"
	"testing"
)

func TestShardMaster_Join(t *testing.T) {
	sm := ShardMaster{}
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	servers := make(map[int][]string)
	servers[1] = []string{"x", "y", "z"}
	c := sm.join(servers)
	sm.configs = append(sm.configs, c)
	fmt.Println(c)

	servers = make(map[int][]string)
	servers[2] = []string{"x2", "y2", "z2"}
	c = sm.join(servers)
	sm.configs = append(sm.configs, c)
	fmt.Println(c)

	servers = make(map[int][]string)
	servers[3] = []string{"x3", "y3", "z3"}
	c = sm.join(servers)
	sm.configs = append(sm.configs, c)
	fmt.Println(c)

	c = sm.leave([]int{1})
	sm.configs = append(sm.configs, c)
	fmt.Println(c)

	servers = make(map[int][]string)
	servers[1] = []string{"x", "y", "z"}
	c = sm.join(servers)
	sm.configs = append(sm.configs, c)
	fmt.Println(c)
}
