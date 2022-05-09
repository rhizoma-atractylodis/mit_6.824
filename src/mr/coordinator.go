package mr

import (
	"fmt"
	"github.com/go-basic/uuid"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import _ "github.com/go-basic/uuid"

//task type
const (
	MAP = iota
	REDUCE
)

// task status
const (
	CREATE = iota
	RUNNING
	DONE
	CRASH
)

// master status
const (
	// RunMap Init = iota
	RunMap = iota
	RunReduce
	ApplicationDone
	//MasterCrash
	//MasterIdle
)

type TaskType int

type TaskStat int

type CoordinatorStat int

type Task struct {
	TaskId    string
	TaskType  TaskType
	InputFile []string
	ReduceNum int
}

type TaskMeta struct {
	taskId         string
	taskType       TaskType
	inputFile      []string
	reduceNum      int
	taskStartTime  time.Time
	taskCreateTime time.Time
	taskStat       TaskStat
	worker         WorkerInfo
}

type WorkerMeta struct {
	workerId      string
	workerAddress string
	available     bool
}

type Coordinator struct {
	Mu              *sync.Mutex
	app             string    // application name
	workersManager  *sync.Map // worker list, master admin all workers, worker obj:true means the worker is enable.
	taskMetaManager *sync.Map // task table, means every task is allotted to which worker.
	//mapNum int
	//mapDoneNum int
	reduceNum int // number of workers run reduce task
	//reduceDoneNum int
	mapTaskQueue     chan *Task // map task queue
	reduceTaskQueue  chan *Task // reduce task queue
	InputFile        []string   // input file
	IntermediateFile *sync.Map
	output           []string
	Stat             CoordinatorStat // master status
	//MapSignal        bool
	//ReduceSignal     bool
	MapSignal    *sync.Cond
	ReduceSignal *sync.Cond
	MapLock      *sync.Mutex
	ReduceLock   *sync.Mutex
	Group        *sync.WaitGroup
}

// Register receive a worker, allocate worker id to it, and record worker meta
func (c *Coordinator) Register(args *WorkerInfo, reply *WorkerInfo) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	workerId := uuid.New()
	(*reply).WorkerId = workerId
	(*reply).WorkerName = "worker-" + workerId
	manager := c.workersManager
	meta := WorkerMeta{
		workerId:      workerId,
		workerAddress: reply.WorkerAddress,
		available:     reply.Stat != BROKEN,
	}
	manager.Store(workerId, meta)
	//if (*reply).WorkerName != "new worker" {
	//	fmt.Printf((*reply).WorkerName)
	//	panic("It isn't new worker, please check worker meta table.")
	//} else {
	//	workerId := uuid.New()
	//	(*reply).WorkerId = workerId
	//	(*reply).WorkerName = "worker-" + workerId
	//	manager := c.workersManager
	//	meta := WorkerMeta{
	//		workerId:      workerId,
	//		workerAddress: reply.WorkerAddress,
	//		available:     reply.Stat != BROKEN,
	//	}
	//	manager.Store(workerId, meta)
	//}
	return nil
}

// RequestTask Worker will request task by RPC
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestArgs, reply *Task) error {
	fmt.Printf("Master: %d \n", c.Stat)
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch c.Stat {
	//case Init:
	//	c.Stat = RunMap
	//	//c.ReduceLock.Lock()
	//	c.Group.Add(1)
	//	fmt.Printf("distribute map task...\n")
	//	task := *<-c.mapTaskQueue
	//	if v, ok := c.taskMetaManager.Load(task.TaskId); ok {
	//		meta := v.(TaskMeta)
	//		meta.taskStat = RUNNING
	//		meta.taskStartTime = time.Now()
	//		meta.worker = args.Worker
	//		c.taskMetaManager.Store(task.TaskId, meta)
	//		*reply = task
	//	} else {
	//		panic("task meta error!")
	//	}
	//	break
	case RunMap:
		fmt.Printf("distribute map task...\n")
		if !c.checkTasksDone() {
			//c.ReduceSignal.Wait()
			task := *<-c.mapTaskQueue
			if v, ok := c.taskMetaManager.Load(task.TaskId); ok {
				meta := v.(TaskMeta)
				meta.taskStat = RUNNING
				meta.taskStartTime = time.Now()
				meta.worker = args.Worker
				c.taskMetaManager.Store(task.TaskId, meta)
			} else {
				panic("task meta error!")
			}
			*reply = task
		} else {
			c.Stat = RunReduce
			//c.Group.Done()
			//c.MapLock.Lock()
			//_ = c.RequestTask(args, reply)
		}
		break
	case RunReduce:
		fmt.Printf("distribute reduce task...\n")
		if !c.checkTasksDone() {
			//c.MapSignal.Wait()
			task := *<-c.reduceTaskQueue
			if v, ok := c.taskMetaManager.Load(task.TaskId); ok {
				meta := v.(TaskMeta)
				meta.taskStat = RUNNING
				meta.taskStartTime = time.Now()
				meta.worker = args.Worker
				c.taskMetaManager.Store(task.TaskId, meta)
				*reply = task
			} else {
				panic("task meta error!")
			}
		} else {
			c.Stat = RunMap
			//c.Group.Done()
		}
		break
	//case MasterCrash:
	//	break
	//case MasterIdle:
	//	c.Stat = RunMap
	//	c.Group.Add(1)
	//	//c.ReduceLock.Lock()
	//	fmt.Printf("distribute map task...\n")
	//	task := *<-c.mapTaskQueue
	//	if v, ok := c.taskMetaManager.Load(task.TaskId); ok {
	//		meta := v.(TaskMeta)
	//		meta.taskStat = RUNNING
	//		meta.taskStartTime = time.Now()
	//		meta.worker = args.Worker
	//		c.taskMetaManager.Store(task.TaskId, meta)
	//		*reply = task
	//	} else {
	//		panic("task meta error!")
	//	}
	//	break
	default:
		panic("coordinator status error!")
	}
	args.Worker.Stat = BUSY
	return nil
}

// FinishMapTask When a worker finish its task, notify master by RPC.
// After master acknowledge, query worker whether it can do other task.
func (c *Coordinator) FinishMapTask(args *MapDoneArgs, reply *Task) error {
	c.Mu.Lock()
	intermediate := c.IntermediateFile
	paths := args.IntermediateFilePaths
	for _, path := range paths {
		index, _ := strconv.Atoi(strings.Split(path, "_")[4])
		store, _ := intermediate.LoadOrStore(index, []string{})
		newStore := append(store.([]string), path)
		c.IntermediateFile.Store(index, newStore)
	}
	c.Mu.Unlock()
	load, ok := c.taskMetaManager.Load(args.TaskId)
	if ok {
		meta := load.(TaskMeta)
		if args.TaskDone {
			meta.taskStat = DONE
			c.taskMetaManager.Store(args.TaskId, meta)
		} else {
			meta.taskStat = CRASH
			taskType := meta.taskType
			c.taskMetaManager.Store(args.TaskId, meta)
			task := Task{
				TaskId:    meta.taskId,
				TaskType:  taskType,
				InputFile: meta.inputFile,
				ReduceNum: meta.reduceNum,
			}
			if taskType == MAP {
				c.mapTaskQueue <- &task
			} else if taskType == REDUCE {
				c.reduceTaskQueue <- &task
			} else {
				panic("task type error!")
			}
		}
	}
	if c.checkTasksDone() {
		c.Mu.Lock()
		c.Stat = RunReduce
		c.Mu.Unlock()
	}
	fmt.Printf("Map task %s has done...\n", args.TaskId)
	c.Group.Done()
	return nil
}

func (c *Coordinator) FinishReduceTask(args *ReduceDoneArgs, reply *interface{}) error {
	load, ok := c.taskMetaManager.Load(args.TaskId)
	if ok {
		meta := load.(TaskMeta)
		if args.TaskDone {
			meta.taskStat = DONE
			c.taskMetaManager.Store(args.TaskId, meta)
		} else {
			meta.taskStat = CRASH
			taskType := meta.taskType
			c.taskMetaManager.Store(args.TaskId, meta)
			task := Task{
				TaskId:    meta.taskId,
				TaskType:  taskType,
				InputFile: meta.inputFile,
				ReduceNum: meta.reduceNum,
			}
			if taskType == MAP {
				c.mapTaskQueue <- &task
			} else if taskType == REDUCE {
				c.reduceTaskQueue <- &task
			} else {
				panic("task type error!")
			}
		}
	}
	c.output = []string{args.output}
	fmt.Printf("Reduce task %s has done...\n", args.TaskId)
	if c.checkTasksDone() {
		c.Mu.Lock()
		c.Stat = RunMap
		c.Mu.Unlock()
	}
	c.Group.Done()
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	_ = os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		_ = http.Serve(l, nil)
	}()
}

// CreateMapTask Task Create
func (c *Coordinator) CreateMapTask(files []string) {
	fmt.Printf("map task create start...\n")
	c.Mu.Lock()
	c.taskMetaManager = &sync.Map{}
	c.Mu.Unlock()
	for _, input := range files {
		id := generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  MAP,
			InputFile: []string{input},
			ReduceNum: c.reduceNum,
		}
		meta := TaskMeta{
			taskId:         id,
			taskType:       MAP,
			inputFile:      []string{input},
			reduceNum:      c.reduceNum,
			taskCreateTime: time.Now(),
			taskStat:       CREATE,
		}
		c.taskMetaManager.Store(id, meta)
		c.mapTaskQueue <- &task
		c.Group.Add(1)
	}
	c.output = []string{}
	c.Group.Done()
	fmt.Printf("map task create done...\n")
}

func (c *Coordinator) CreateReduceTask(files *sync.Map) {
	fmt.Printf("reduce task create start...\n")
	c.Mu.Lock()
	c.taskMetaManager = &sync.Map{}
	c.Mu.Unlock()
	files.Range(func(key, value interface{}) bool {
		id := generateTaskId()
		input := value.([]string)
		task := Task{
			TaskId:    id,
			TaskType:  REDUCE,
			InputFile: input,
			ReduceNum: c.reduceNum,
		}
		meta := TaskMeta{
			taskId:         id,
			taskType:       REDUCE,
			inputFile:      input,
			reduceNum:      c.reduceNum,
			taskCreateTime: time.Now(),
			taskStat:       CREATE,
		}
		c.taskMetaManager.Store(id, meta)
		c.reduceTaskQueue <- &task
		c.Group.Add(1)
		return true
	})
	c.Group.Done()
	fmt.Printf("reduce task create done...\n")
}

func (c *Coordinator) checkTasksDone() bool {
	var isDone = true
	c.taskMetaManager.Range(func(key, value interface{}) bool {
		meta := value.(TaskMeta)
		if meta.taskStat != DONE {
			isDone = false
			return false
		}
		return true
	})
	return isDone
}

// Done Application finish
func (c *Coordinator) Done() bool {
	return (c.Stat == REDUCE) && c.checkTasksDone()
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Mu:               &sync.Mutex{},
		app:              "",
		workersManager:   &sync.Map{},
		taskMetaManager:  &sync.Map{},
		reduceNum:        nReduce,
		mapTaskQueue:     make(chan *Task),
		reduceTaskQueue:  make(chan *Task),
		InputFile:        files,
		IntermediateFile: &sync.Map{},
		Stat:             RunMap,
		//MapSignal:        false,
		//ReduceSignal:     false,
		MapLock:    &sync.Mutex{},
		ReduceLock: &sync.Mutex{},
		output:     []string{},
		Group:      &sync.WaitGroup{},
	}
	c.MapSignal = sync.NewCond(c.MapLock)
	c.ReduceSignal = sync.NewCond(c.ReduceLock)
	fmt.Printf("create master...\n")
	c.server()
	return &c
}

func generateTaskId() string {
	return uuid.New()
}
