package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// 任务类型
const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
)

const (
	fileRuleIntermediate = "mr-%d-%d"
	fileRuleReduce       = "mr-reduce-%d"
	fileRuleOutput       = "mr-out-%d"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker 启动worker，处理任务
// 主要流程:
// 重复执行:
// 1. 注册worker。 2. 得知自己的任务类型后，读取文件, 执行map或者reduce函数，写入文件
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply, err := register()
		if err != nil {
			log.Fatalf("register failed, err:%v", err)
		}
		// 任务已经完成，worker可以退出了
		if reply.Finish {
			break
		}
		workerID := reply.WorkerID
		fileDescs := process(reply, mapf, reducef)
		// notify master
		notifyMaster(workerID, reply.TaskType, fileDescs)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func register() (*RegisterReply, error) {
	args := RegisterArgs{}
	reply := RegisterReply{}

	if !call("Master.Register", &args, &reply) {
		return nil, fmt.Errorf("register call failed")
	}
	return &reply, nil
}

func process(
	registerReply *RegisterReply,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) []*FileDesc {
	nReduce := registerReply.ReduceNums
	workderID := registerReply.WorkerID

	// 1. read file
	// 2. process map/reduce
	// 3. write file

	if registerReply.TaskType == TaskTypeMap {
		fileContents := make(map[string]string)
		fileNames := registerReply.FileNames
		for _, fileName := range fileNames {
			content, err := ioutil.ReadFile(fileName)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			fileContents[fileName] = string(content)
		}

		intermediate := []KeyValue{}
		for filename, content := range fileContents {
			kva := mapf(filename, content)
			intermediate = append(intermediate, kva...)
		}

		splitKvs := make(map[int][]KeyValue)
		// init first
		for i := 0; i < nReduce; i++ {
			splitKvs[i] = make([]KeyValue, 0)
		}

		for _, kv := range intermediate {
			h := ihash(kv.Key) % nReduce
			splitKvs[h] = append(splitKvs[h], kv)
		}

		rFiles := make([]*FileDesc, 0)
		for i, kvs := range splitKvs {
			filename := fmt.Sprintf(fileRuleIntermediate, workderID, i)
			file, err := os.Create(filename)
			if err != nil {
				log.Fatalf("open file: %v failed, err: %v", filename, err)
			}
			fileDesc := &FileDesc{
				Key: i,
				FileName: filename,
			}
			rFiles = append(rFiles, fileDesc)
			enc := json.NewEncoder(file)
			for _, kv := range kvs {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("failed encode, kv:%v", kv)
				}
			}
		}
		return rFiles

	} else if registerReply.TaskType == TaskTypeReduce {
		// decode
		fileNames := registerReply.FileNames
		kva := make([]KeyValue, 0) // store all file's kv
		for _, fileName := range fileNames {
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("open %v failed, err: %v", file, err)
			}

			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}

		}

		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf(fileRuleReduce, workderID)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("create output file failed, oname:%v, err: %v", ofile, err)
		}

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}
		return []*FileDesc{
			&FileDesc{
				Key: workderID,
				FileName: oname,
			},
		}
	} else {
		log.Fatalf("unknown task %v", registerReply.TaskType)
	}

	return []*FileDesc{}
}

// func mapProcess(
// 	contents map[string]string,
// 	mapf func(string, string) []KeyValue,
// ) []KeyValue {
// 	return nil
// }

// func reduceProcess() []string {
// 	// 1. decode file content into KeyValue
// 	// 2. sort all the content into one []KeyValue
// 	// 3. per key process reduce
// 	return nil
// }

func notifyMaster(workderID int, taskType int, fileDescs []*FileDesc) *FinishReply {
	args := FinishArgs{
		WorkerID:  workderID,
		TaskType:  taskType,
		FileDescs: fileDescs,
	}

	reply := FinishReply{}

	if !call("Master.Finish", &args, &reply) {
		log.Fatalf("call failed, workderID: %d", workderID)
	}

	return &reply
}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
