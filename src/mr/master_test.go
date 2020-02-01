package mr

import (
	"fmt"
	"time"
	"testing"
)

func TestMakeMaster(t *testing.T) {
	files := []string{"pg-tom_sawyer.txt"}
	master := MakeMaster(files, 10)
	time.Sleep(10 * time.Second)
	fmt.Println(master.Done())
}
