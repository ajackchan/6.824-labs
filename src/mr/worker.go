package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// Request a task from coordinator
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			// Coordinator has exited, worker should exit
			break
		}

		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf, reply.TaskId, reply.FileName, reply.NReduce)
			reportTask(MapTask, reply.TaskId)
		case ReduceTask:
			doReduceTask(reducef, reply.TaskId, reply.NMap)
			reportTask(ReduceTask, reply.TaskId)
		case WaitTask:
			// Wait a bit and try again
			time.Sleep(100 * time.Millisecond)
		case ExitTask:
			// All tasks done, exit
			return
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, mapId int, filename string, nReduce int) {
	// Read input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Call Map function
	kva := mapf(filename, string(content))

	// Partition intermediate key/value pairs into nReduce buckets
	buckets := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		buckets[i] = make([]KeyValue, 0)
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write each bucket to intermediate file mr-X-Y
	for i := 0; i < nReduce; i++ {
		intermediateFile := fmt.Sprintf("mr-%d-%d", mapId, i)
		tmpFile, err := ioutil.TempFile("", "mr-tmp-")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}

		enc := json.NewEncoder(tmpFile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode key/value pair")
			}
		}

		tmpFile.Close()
		err = os.Rename(tmpFile.Name(), intermediateFile)
		if err != nil {
			log.Fatalf("cannot rename temp file to %v", intermediateFile)
		}
	}
}

func doReduceTask(reducef func(string, []string) string, reduceId int, nMap int) {
	// Collect all key/value pairs for this reduce task from all map tasks
	kva := []KeyValue{}
	for i := 0; i < nMap; i++ {
		intermediateFile := fmt.Sprintf("mr-%d-%d", i, reduceId)
		file, err := os.Open(intermediateFile)
		if err != nil {
			// File might not exist (map task might have failed)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// Sort by key
	sort.Sort(ByKey(kva))

	// Create output file
	oname := fmt.Sprintf("mr-out-%d", reduceId)
	tmpFile, err := ioutil.TempFile("", "mr-out-tmp-")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	// Call Reduce on each distinct key
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

		// Write output in correct format
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tmpFile.Close()
	err = os.Rename(tmpFile.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename temp file to %v", oname)
	}
}

func reportTask(taskType TaskType, taskId int) {
	args := ReportTaskArgs{
		TaskType: taskType,
		TaskId:   taskId,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
