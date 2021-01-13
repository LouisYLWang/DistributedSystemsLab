package mr

import (
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
	switch req.Header {
	case RequestTask:
		m.handleRegisterTask(req, res)

	case FinishTask:
		m.handleFinishTask(req, res)
		m.handleRegisterTask(req, res)

	case RegisterWorker:
		m.handleRegisterWorker(req, res)
	}
	return nil
}

func (m *Master) handleRegisterWorker(req *Req, res *Res) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WorkerNum++
	res.WorkerID = m.WorkerNum
	return nil
}

func (m *Master) handleRegisterTask(req *Req, res *Res) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var taskID int
	select {
	case overtimeTaskID := <-m.Timers:
		taskID = overtimeTaskID
	default:
		if m.TaskEndNum == len(m.Tasks) {
			res.Header = NoNewTask
			return nil
		}
		m.TaskInitNum++
		taskID = m.TaskInitNum
	}
	task, _ := m.assign(taskID, req.WorkerID, res)
	go m.taskTimer(task)
	return nil
}

func (m *Master) handleFinishTask(req *Req, res *Res) error {
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
	if assignedTask.workerID != NotAssignedWorker && assignedTask.workerID != workerID {
		//TODO: handle evit a Task from old worker
	}
	res.FileName = assignedTask.file
	res.TaskID = taskID
	return assignedTask, nil
}

func (m *Master) taskTimer(t *Task) error {
	time.Sleep(10 * time.Millisecond)
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
	return m.TaskEndNum == len(m.Tasks)
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
	}

	m := Master{
		mu:          sync.Mutex{},
		Files:       files,
		NReduce:     nReduce,
		Tasks:       tasks,
		TaskInitNum: 1,
		Timers:      make(chan int, 10),
		WorkerNum:   0,
	}

	// Your code here.
	m.server()
	return &m
}
