package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	Files       []string
	NReduce     int
	Tasks       []*Task
	TaskInitNum int
	TaskEndNum  int
	Timers      chan int
	WorkerNum   int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RPCHandler(req *Req, res *Res) error {
	res.Header = OK
	switch req.Header {
	case RequestTask:
		m.handleRegisterTask(req, res)
		break

	case FinishTask:
		m.handleFinishTask(req, res)
		break

	case RegisterWorker:
		m.handleRegisterWorker(req, res)
		break

	default:
		res.Header = ErrInvalidReq
		break
	}
	return nil
}

func (m *Master) handleRegisterWorker(req *Req, res *Res) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WorkerNum++
	res.WorkerID = m.WorkerNum
	res.NReduce = m.NReduce
	return nil
}

func (m *Master) handleRegisterTask(req *Req, res *Res) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Printf("handleRegisterTask - %v", req.WorkerID)

	var taskID int
	select {
	case overtimeTaskID := <-m.Timers:
		taskID = overtimeTaskID
		log.Printf("overTime!ID - %v", overtimeTaskID)
		break
	default:
		if m.TaskInitNum == len(m.Tasks)-1 {
			fmt.Printf("No new task num:%v\n", m.TaskEndNum)
			res.Header = NoNewTask
			return nil
		} else if m.TaskEndNum == len(m.Tasks)-1 {
			fmt.Printf("All task end num:%v\n", m.TaskEndNum)
			res.Header = AllTaskFinished
			return nil
		}
		m.TaskInitNum++
		taskID = m.TaskInitNum
		log.Printf("default assign - %v", taskID)
		break
	}
	task, _ := m.assign(taskID, req.WorkerID, res)
	go m.taskTimer(task)
	return nil
}

func (m *Master) handleFinishTask(req *Req, res *Res) error {
	log.Printf("handleFinishTask - %v", m.Tasks[req.TaskID].workerID)
	if m.Tasks[req.TaskID].workerID != FinishedWorker {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.Tasks[req.TaskID].workerID = FinishedWorker
		m.TaskEndNum++
	}
	return nil
}

func (m *Master) assign(taskID int, workerID int, res *Res) (*Task, error) {
	res.Header = OK
	assignedTask := m.Tasks[taskID]
	assignedTask.ID = taskID
	assignedTask.workerID = workerID
	log.Printf("Assign - job %v to %v", assignedTask.ID, assignedTask.workerID)

	if assignedTask.workerID != NotAssignedWorker && assignedTask.workerID != workerID {
		//TODO: handle evit a Task from old worker
	}
	res.FileName = assignedTask.file
	res.TaskID = taskID
	return assignedTask, nil
}

func (m *Master) taskTimer(t *Task) error {
	time.Sleep(20 * time.Second)
	if t.workerID != FinishedWorker {
		m.Timers <- t.ID
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.TaskEndNum == len(m.Tasks)-1
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	var (
		tasks = make([]*Task, len(files))
	)

	for i := 0; i < len(files); i++ {
		tasks[i] = &Task{
			ID:       i,
			file:     files[i],
			workerID: NotAssignedWorker,
		}
		fmt.Printf("load file names%v\n", files[i])
	}

	m := Master{
		mu:          sync.Mutex{},
		Files:       files,
		NReduce:     nReduce,
		Tasks:       tasks,
		TaskInitNum: -1,
		Timers:      make(chan int, 10),
		WorkerNum:   0,
	}

	// Your code here.
	m.server()
	return &m
}
