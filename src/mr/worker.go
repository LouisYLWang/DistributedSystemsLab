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

//
// Map functions return a slice of KeyValue.
//
type (
	Workerman struct {
		workerID int
		mapf     func(string, string) []KeyValue
		reducef  func(string, []string) string
	}

	KeyValue struct {
		Key   string
		Value string
	}

	ByKey []KeyValue
)

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	w := Workerman{
		mapf:    mapf,
		reducef: reducef,
	}
	res := w.request(RegisterWorker)
	w.workerID = res.WorkerID

	for {
		res = w.request(RegisterWorker)
		if res.Header == OK {
			w.performMapTask(res)
		} else if res.Header == NoNewTask {
			log.Printf("all Task has been assigned")
			return
		}
	}
}

// register Task or worker server
func (w *Workerman) request(reqHeader string, args ...int) *Res {
	req := Req{
		Header:   reqHeader,
		WorkerID: -1,
		FileName: "",
		TaskID:   NotAssignedWorker,
	}

	switch reqHeader {
	case FinishTask:
		req.TaskID = args[0]
	}
	res := Res{}
	if ok := call("Master.RPCHandler", &req, &res); !ok {
		log.Fatal("req fail")
	}
	return &res
}

func (w *Workerman) performMapTask(res *Res) {
	taskID := res.TaskID
	fileName := res.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := w.mapf(fileName, string(content))
	sort.Sort(ByKey(kva))

	intrmedFileName := fmt.Sprintf("mr-%d", taskID)
	intrmedFile, _ := os.Create(intrmedFileName)
	enc := json.NewEncoder(intrmedFile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("error writing %s", intrmedFileName)
		}
	}
	log.Printf("worker %d finish Task %d on file %s", w.workerID, taskID, fileName)
}

//
// example function to show how to make an RPC call to the master.
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
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
