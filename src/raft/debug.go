package raft

import (
	"encoding/json"
	"log"
)

const (
	debug = false
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	// fd, err := os.Create("o2.log")
	// if err != nil {
	// 	log.Fatalf("os.create failed: %v", err)
	// }
	// log.SetOutput(fd)
}

// DEBUG 辅助打印日志
func DEBUG(fmt string, args ...interface{}) {
	if debug {
		log.Printf(fmt, args...)
	}
}

func ENTRY_STRING(entry *LogEntry) string {
	bs, _ := json.Marshal(entry)
	return string(bs)
}

func ENTRIES_STRING(entries []*LogEntry) string {
	bs, _ := json.Marshal(entries)
	return string(bs)
}

func CHECK_LOGS(entries []*LogEntry) {
	if !debug {
		return
	}
	var (
		last    *LogEntry
		maxTerm int
	)
	for _, entry := range entries {
		if (last != nil && last.Index+1 != entry.Index) ||
			(maxTerm > 0 && entry.Term < maxTerm) {
			log.Fatalf("entries are illegal, entries:%s", ENTRIES_STRING(entries))
		}

		last = entry
		if maxTerm < entry.Term {
			maxTerm = entry.Term
		}
	}
}
