package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	mapTasks     []Task
	reduceTasks  []Task
	nMap         int
	nReduce      int
	mapDone      bool
	reduceDone   bool
}

type Task struct {
	Type      TaskType
	Id        int
	State     TaskState
	FileName  string
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask RPC handler - worker calls this to get a task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if all tasks are done
	if c.mapDone && c.reduceDone {
		reply.TaskType = ExitTask
		return nil
	}

	// First, handle timeouts (10 seconds)
	now := time.Now()
	for i := range c.mapTasks {
		if c.mapTasks[i].State == TaskInProgress {
			if now.Sub(c.mapTasks[i].StartTime) > 10*time.Second {
				c.mapTasks[i].State = TaskIdle
			}
		}
	}
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == TaskInProgress {
			if now.Sub(c.reduceTasks[i].StartTime) > 10*time.Second {
				c.reduceTasks[i].State = TaskIdle
			}
		}
	}

	// Assign map tasks first
	if !c.mapDone {
		for i := range c.mapTasks {
			if c.mapTasks[i].State == TaskIdle {
				c.mapTasks[i].State = TaskInProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskId = c.mapTasks[i].Id
				reply.FileName = c.mapTasks[i].FileName
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
		// All map tasks are in progress or done, wait
		if c.checkMapDoneLocked() {
			c.mapDone = true
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}

	// All map tasks are done, assign reduce tasks
	if !c.reduceDone {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == TaskIdle {
				c.reduceTasks[i].State = TaskInProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskId = c.reduceTasks[i].Id
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
		// All reduce tasks are in progress or done
		if c.checkReduceDoneLocked() {
			c.reduceDone = true
			reply.TaskType = ExitTask
		} else {
			reply.TaskType = WaitTask
		}
		return nil
	}

	reply.TaskType = ExitTask
	return nil
}

// ReportTask RPC handler - worker calls this to report task completion
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if args.TaskId >= 0 && args.TaskId < len(c.mapTasks) {
			c.mapTasks[args.TaskId].State = TaskCompleted
		}
	} else if args.TaskType == ReduceTask {
		if args.TaskId >= 0 && args.TaskId < len(c.reduceTasks) {
			c.reduceTasks[args.TaskId].State = TaskCompleted
		}
	}

	return nil
}

func (c *Coordinator) checkMapDoneLocked() bool {
	for i := range c.mapTasks {
		if c.mapTasks[i].State != TaskCompleted {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkReduceDoneLocked() bool {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State != TaskCompleted {
			return false
		}
	}
	return true
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Your code here.
	return c.mapDone && c.reduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, c.nMap)
	c.reduceTasks = make([]Task, nReduce)
	c.mapDone = false
	c.reduceDone = false

	// Initialize map tasks
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i] = Task{
			Type:     MapTask,
			Id:       i,
			State:    TaskIdle,
			FileName: files[i],
		}
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type:  ReduceTask,
			Id:    i,
			State: TaskIdle,
		}
	}

	c.server()
	return &c
}
