package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"

	"sync"
	"time"
)

const (
	workerStatusWorking = 0
	workerStatusExpire  = 1
	workerStatusSuccess = 2
)

// Master Master需要监控所有worker的运行状态，分配的任务完成情况，并且需要负责最后的结果处理
// 优先分配Map任务，Reduce来的时候挂起，等到所有Map任务完成，回复Reduce任务
// 这里假设Master是不会挂，这一点很重要
// 分配的workderID仅仅是为了表示worker的，与最终的文件产物无关
type Master struct {
	// Your definitions here.
	M             int
	R             int
	WorkerCounter int // 自增发号器，给workderID编号
	TaskCounter   int
	Files         []string //

	// MapOutputFiles    map[int][]string // 一个workder可以产生多个中间产物
	// ReduceOutputFiles map[int]string   // reduce产物一个workder产生一个文件

	// 一下信息更改时候需要同步 TODO: 换成读写锁
	lock sync.Mutex
	// 任务记录
	WorkerInfo map[int]*workerStatus
	Tasks      map[int]*taskInfo

	TaskTodo         chan *taskInfo // 未分配的任务队列
	WorkerAssignment map[int]int    // workerID => taskID
	// TaskAssignment   map[int]int    // taskID => workerID

	DoneCh         chan struct{}
	TaskDone       int64
	MapTaskDone    int
	ReduceTaskDone int
}

type workerStatus struct {
	WorkderID    int
	RegisterTime time.Time // 目的为了检测超时
	Status       int
}

type taskInfo struct {
	TaskID      int
	TaskType    int
	InputFiles  []string
	OutputFiles []*FileDesc
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	// 注册worker, 区分任务类型，分配任务
	reply.MapNums = m.M
	reply.ReduceNums = m.R

	var task *taskInfo
	select {
	case task = <-m.TaskTodo:
		reply.Finish = false

	case <-m.DoneCh:
		reply.Finish = true

	}
	if reply.Finish {
		return nil
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	reply.TaskType = task.TaskType
	workerID := m.WorkerCounter
	m.WorkerCounter++
	reply.WorkerID = workerID
	m.WorkerAssignment[workerID] = task.TaskID
	// m.TaskAssignment[task.TaskID] = workerID

	worker := &workerStatus{
		WorkderID:    workerID,
		RegisterTime: time.Now(),
		Status:       workerStatusWorking,
	}
	m.WorkerInfo[workerID] = worker
	return nil
}

func (m *Master) Finish(args *FinishArgs, reply *FinishReply) error {
	if m.TaskDone == 0 {
		reply.Success = false
		return nil
	}
	m.lock.Lock()
	defer m.lock.Unlock()

	worker := m.WorkerInfo[args.WorkerID]
	if worker.Status != workerStatusWorking {
		reply.Success = false
		return nil
	}
	worker.Status = workerStatusSuccess
	taskID := m.WorkerAssignment[worker.WorkderID]
	task := m.Tasks[taskID]
	task.OutputFiles = args.FileDescs

	if args.TaskType == TaskTypeMap {
		m.MapTaskDone++
		if m.MapTaskDone == m.M {
			// init all the reduce task
			reduceFiles := make(map[int][]string) // 其实用数组也可以，刚开始没想对
			for _, task := range m.Tasks {
				if task.TaskType != TaskTypeMap {
					continue
				}
				fileDescs := task.OutputFiles
				for _, fd := range fileDescs {
					if _, ok := reduceFiles[fd.Key]; !ok {
						reduceFiles[fd.Key] = make([]string, 0)
					}
					reduceFiles[fd.Key] = append(reduceFiles[fd.Key], fd.FileName)
				}
			}
			for _, rf := range reduceFiles {
				task := &taskInfo{
					TaskID:      m.TaskCounter,
					TaskType:    TaskTypeReduce,
					InputFiles:  rf,
					OutputFiles: []*FileDesc{},
				}
				m.TaskCounter++
				m.Tasks[task.TaskID] = task
				m.TaskTodo <- task
			}
		}
	} else if args.TaskType == TaskTypeReduce {
		m.ReduceTaskDone++
		if m.ReduceTaskDone == m.R {
			// handle all the result
			reduceTasks := make([]*taskInfo, 0)
			for _, task := range m.Tasks {
				if task.TaskType != TaskTypeReduce {
					continue
				}
				reduceTasks = append(reduceTasks, task)
			}
			finalFiles := make([]string, 0)
			for _, task := range reduceTasks {
				fns := make([]string, 0)
				for _, fd := range task.OutputFiles {
					fns = append(fns, fd.FileName)
				}
				finalFiles = append(finalFiles, fns...)
			}
			for i, filename := range finalFiles {
				err := os.Rename(filename, fmt.Sprintf(fileRuleOutput, i))
				if err != nil {
					log.Fatalf("os rename failed, filename:%v, err:%v", filename, err)
				}
			}
			
			atomic.StoreInt64(&m.TaskDone, 1)
			close(m.DoneCh)
		}
	} else {
		return fmt.Errorf("illegal argument")
	}
	return nil
}

func (m *Master) background() {

	for {
		select {
		case <-m.DoneCh:
			return
		case <-time.Tick(200 * time.Millisecond):
			m.lock.Lock()
			fiveSecondsAgo := time.Now().Add(-5 * time.Second)
			for _, worker := range m.WorkerInfo {
				if worker.Status == workerStatusWorking && fiveSecondsAgo.After(worker.RegisterTime) {
					worker.Status = workerStatusExpire
					workerID := worker.WorkderID
					taskID := m.WorkerAssignment[workerID]
					// delete(m.TaskAssignment, taskID)
					delete(m.WorkerAssignment, workerID)
					task := m.Tasks[taskID]
					m.TaskTodo <- task
				}
			}
			m.lock.Unlock()
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.

	return m.TaskDone == 1
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.M = len(files)
	m.R = nReduce
	m.Files = files
	m.WorkerCounter = 0
	m.TaskCounter = 0
	m.TaskDone = 0
	// init all map task
	m.lock.Lock()
	m.WorkerInfo = make(map[int]*workerStatus)
	m.Tasks = make(map[int]*taskInfo)

	m.TaskTodo = make(chan *taskInfo, m.M+m.R)
	m.WorkerAssignment = make(map[int]int)
	// m.TaskAssignment = make(map[int]int)

	for _, file := range files {
		task := &taskInfo{
			TaskID:      m.TaskCounter,
			TaskType:    TaskTypeMap,
			InputFiles:  []string{file},
			OutputFiles: []*FileDesc{},
		}
		m.TaskCounter++
		m.Tasks[task.TaskID] = task
		m.TaskTodo <- task

	}

	m.lock.Unlock()

	m.server()
	go m.background()
	return &m
}
