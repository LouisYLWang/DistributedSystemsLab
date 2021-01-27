package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type (
	Workerman struct {
		workerID int
		mapf     func(string, string) []KeyValue
		reducef  func(string, []string) string
		nReduce  int
		nMap     int
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
		mapf:     mapf,
		reducef:  reducef,
		workerID: -1,
	}
	res := w.request(RegisterWorker, NotAssignedWorker)
	w.workerID = res.WorkerID
	w.nMap = res.NMap
	w.nReduce = res.NReduce
	for {
		res = w.request(RequestTask, res.TaskID)
		switch res.Header {
		case Map:
			log.Printf("worker %d received Map Task %d on file %s", w.workerID, res.TaskID, res.FileName)
			w.performMapTask(res)
			w.request(FinishTask, res.TaskID)
			break

		case Reduce:
			log.Printf("worker %d received Reduce Task %d on file %s", w.workerID, res.TaskID, res.FileName)
			w.performReduceTask(res)
			w.request(FinishTask, res.TaskID)
			break

		case NoNewTask:
			log.Printf("no new Task now yet, wait")
			time.Sleep(1 * time.Second)
			break
		case AllTaskFinished:
			log.Printf("all Task has been assigned")
			return

		}
	}
}

// register Task or worker server
func (w *Workerman) request(reqHeader string, taskID int) *Res {
	req := Req{
		Header:   reqHeader,
		WorkerID: w.workerID,
		FileName: "",
		TaskID:   NotAssignedWorker,
	}

	req.TaskID = taskID
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
	reduces := make([][]KeyValue, w.nReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % w.nReduce
		reduces[i] = append(reduces[i], kv)
	}

	for i, kvs := range reduces {
		intrmedFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		intrmedFile, _ := os.Create(intrmedFileName)
		enc := json.NewEncoder(intrmedFile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("error writing %s", intrmedFileName)
			}
		}
		intrmedFile.Close()
	}
	log.Printf("worker %d finish map Task %d", w.workerID, taskID)
}

func (w *Workerman) performReduceTask(res *Res) {
	reduceMap := make(map[string][]string)
	for i := 0; i < w.nMap; i++ {
		intrmedFile, err := os.Open(fmt.Sprintf("mr-%d-%d", i, res.TaskID))
		if err != nil {
			log.Fatalf("cannot open mr-%d-%d: %v ", i, res.TaskID, err)
		}
		dec := json.NewDecoder(intrmedFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, existed := reduceMap[kv.Key]; !existed {
				reduceMap[kv.Key] = make([]string, 0)
			}
			reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
		}
	}

	// exportString := ""
	exportFileName := fmt.Sprintf("mr-out-%d", res.TaskID)
	exportFile, _ := os.Create(exportFileName)
	for key, val := range reduceMap {
		// exportString = exportString + fmt.Sprintf("%v %v\n", key, w.reducef(key, val))
		fmt.Fprintf(exportFile, "%v %v\n", key, w.reducef(key, val))
	}
	exportFile.Close()
	log.Printf("worker %d finish reduce Task %d", w.workerID, res.TaskID)
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
