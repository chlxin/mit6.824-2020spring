package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterArgs struct {
	
}

type RegisterReply struct {
	Finish bool // 任务是否已经结束
	WorkerID int
	TaskType int
	MapNums int   
	ReduceNums int 
	FileNames []string // 
}

type FinishArgs struct {
	WorkerID int
	TaskType int
	FileDescs []*FileDesc
}

type FinishReply struct {
	Success bool
}

type FileDesc struct {
	Key      int
	FileName string
}