package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type OrderedKeyValues []KeyValue

type WorkerStat int

const (
	IDLE = iota
	BUSY
	BROKEN
)

type WorkerInfo struct {
	WorkerId      string
	WorkerName    string
	WorkerAddress string
	Stat          WorkerStat
}

func (o OrderedKeyValues) Len() int {
	return len(o)
}

func (o OrderedKeyValues) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o OrderedKeyValues) Less(i, j int) bool {
	return o[i].Key < o[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(task Task, mapF func(string, string) []KeyValue) ([]string, error) {
	intermediateFiles := make([]string, task.ReduceNum)
	intermediateValue := make([][]KeyValue, task.ReduceNum, task.ReduceNum)
	filePath := task.InputFile[0]
	file, err := os.Open(filePath)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	if err != nil {
		panic(err)
	}
	content, _ := ioutil.ReadAll(file)
	keyValues := mapF(filePath, string(content))
	for _, entry := range keyValues {
		key := entry.Key
		index := ihash(key) % task.ReduceNum
		intermediateValue[index] = append(intermediateValue[index], entry)
	}

	for i := 0; i < task.ReduceNum; i++ {
		filename := fmt.Sprintf("mr-intermediate/mr_%s_%d", task.TaskId, i)
		file, err := os.Create("/root/mit_ds_2021/src/main/" + filename)
		intermediateFiles[i] = file.Name()
		if err != nil {
			panic(err)
		}
		data, _ := json.Marshal(intermediateValue[i])
		_, _ = file.Write(data)
		_ = file.Close()
	}
	return intermediateFiles, nil
}

func doReduce(task Task, reduceF func(string, []string) string) (string, error) {
	fmt.Printf("do reduce task...\n")
	filePaths := task.InputFile
	var allKeyValues []KeyValue
	reduceIndex := strings.Split(filePaths[0], "_")[2]
	for _, path := range filePaths {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		bytes, _ := ioutil.ReadAll(file)
		var keyValues []KeyValue
		_ = json.Unmarshal(bytes, &keyValues)
		allKeyValues = append(allKeyValues, keyValues...)
		file.Close()
	}
	fmt.Printf("finish load data...\n")
	sort.Sort(OrderedKeyValues(allKeyValues))
	fmt.Printf("finish sort data...\n")
	result := aggregation(allKeyValues)
	outputPath := "/root/mit_ds_2021/src/main/mr-tmp/mr-out-" + reduceIndex
	oFile, _ := os.Create(outputPath)
	for key, values := range result {
		output := reduceF(key, values)
		fmt.Printf("worker:start store key:%s...\n", key)
		_, _ = fmt.Fprintf(oFile, "%v %v\n", key, output)
		fmt.Printf("worker:finish store key:%s...\n", key)
	}
	oFile.Close()
	fmt.Printf("worker:reduce task is done...\n")
	return outputPath, nil
}

func CreateWorker() (*WorkerInfo, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		panic(err)
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip := strings.Split(localAddr.String(), ":")[0]
	worker := WorkerInfo{
		WorkerId:      "",
		WorkerName:    "new worker",
		WorkerAddress: ip,
		Stat:          IDLE,
	}
	masterNotify := Register(&worker)
	if !masterNotify {
		panic("register failed!")
	}
	return &worker, nil
}

// Worker main/mrworker.go calls this function.
func (w *WorkerInfo) Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) error {
	task := RequestTask(*w)
	if task.TaskType == MAP {
		intermediate, err := doMap(*task, mapF)
		if err == nil {
			MapTaskDone(*w, intermediate, true, (*task).TaskId)
		} else {
			MapTaskDone(*w, nil, false, (*task).TaskId)
			panic(err)
		}
	} else if task.TaskType == REDUCE {
		output, err := doReduce(*task, reduceF)
		if err == nil {
			ReduceTaskDone(*w, output, true, (*task).TaskId)
		} else {
			ReduceTaskDone(*w, "", false, (*task).TaskId)
			panic(err)
		}
	} else {
		panic("task type error!")
	}
	return nil
}

func Register(worker *WorkerInfo) bool {
	call("Coordinator.Register", &WorkerInfo{}, worker)
	return (*worker).WorkerName != "new worker"
}

// RequestTask request task by rpc
func RequestTask(worker WorkerInfo) *Task {
	args := RequestArgs{
		Worker: worker,
	}
	reply := Task{}
	call("Coordinator.RequestTask", &args, &reply)
	fmt.Printf(reply.TaskId + "-" + strconv.Itoa(int(reply.TaskType)) + " has be distribute...\n")
	for _, file := range reply.InputFile {
		fmt.Printf(file + "\n")
	}
	return &reply
}

// MapTaskDone notify to master, current task is complete. maybe return a new task.
func MapTaskDone(worker WorkerInfo, files []string, isDone bool, taskId string) *Task {
	requestArgs := MapDoneArgs{
		Worker:                worker,
		TaskId:                taskId,
		IntermediateFilePaths: files,
		TaskDone:              isDone,
	}
	reply := Task{}
	call("Coordinator.FinishMapTask", &requestArgs, &reply)
	return &reply
}

func ReduceTaskDone(worker WorkerInfo, output string, isDone bool, taskId string) *Task {
	requestArgs := ReduceDoneArgs{
		Worker:   worker,
		TaskId:   taskId,
		output:   output,
		TaskDone: isDone,
	}
	reply := Task{}
	call("Coordinator.FinishReduceTask", &requestArgs, &reply)
	return &reply
}

// aggregation by key
func aggregation(keyValues []KeyValue) map[string][]string {
	result := make(map[string][]string)
	i := 0
	for i < len(keyValues) {
		fmt.Printf("worker:%s aggregation...\n", keyValues[i].Key)
		j := i + 1
		for j < len(keyValues) && keyValues[i].Key == keyValues[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyValues[k].Value)
		}
		result[keyValues[i].Key] = values
		i = j
	}
	fmt.Printf("worker:finish aggregation...\n")
	return result
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func(c *rpc.Client) {
		_ = c.Close()
	}(c)
	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return true
}
