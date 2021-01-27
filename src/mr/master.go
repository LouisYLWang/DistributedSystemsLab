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
	NMap        int
	Tasks       []*Task
	TaskInitNum int
	TaskEndNum  int
	Timers      chan int
	WorkerNum   int
	Phase       string
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
	res.TaskID = NotAssignedWorker
	res.NReduce = m.NReduce
	res.NMap = m.NMap
	return nil
}

func (m *Master) handleRegisterTask(req *Req, res *Res) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Printf("handleRegisterTask - worker %v - task %v", req.WorkerID, req.TaskID)
	var taskID int
	select {
	case overtimeTaskID := <-m.Timers:
		taskID = overtimeTaskID
		log.Printf("overTime!ID - %v", overtimeTaskID)
		break
	default:
		if m.TaskEndNum == len(m.Tasks) {
			if m.Phase == Reduce {
				fmt.Printf("All task end num:%v\n", m.TaskEndNum)
				res.Header = AllTaskFinished
				return nil
			}
			m.initReduce()
		} else if m.TaskInitNum == len(m.Tasks)-1 {
			fmt.Printf("No new task num:%v\n", m.TaskEndNum)
			res.Header = NoNewTask
			return nil
		}
		m.TaskInitNum++
		taskID = m.TaskInitNum
		log.Printf("default assign - %v", taskID)
		break
	}
	res.Header = m.Phase
	task, _ := m.assign(taskID, req.WorkerID, res, m.Phase)
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

func (m *Master) assign(taskID int, workerID int, res *Res, taskType string) (*Task, error) {
	res.Header = taskType
	assignedTask := m.Tasks[taskID]
	assignedTask.ID = taskID
	assignedTask.workerID = workerID
	log.Printf("Assign - job %v to %v", assignedTask.ID, assignedTask.workerID)
	log.Printf("file %v", assignedTask.file)

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

func (m *Master) initReduce() {
	m.Phase = Reduce
	m.TaskEndNum = 0
	m.TaskInitNum = -1

	var (
		tasks = make([]*Task, m.NReduce)
	)
	m.Tasks = tasks

	for i := 0; i < m.NReduce; i++ {
		m.Tasks[i] = &Task{
			ID:       i,
			file:     fmt.Sprintf("mr-*-%d", i),
			workerID: NotAssignedWorker,
		}
		fmt.Printf("load reduce task%v\n", tasks[i].file)
	}
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
	return m.Phase == Reduce && m.TaskEndNum == len(m.Tasks)-1
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
		NMap:        len(tasks),
		Tasks:       tasks,
		TaskInitNum: -1,
		Timers:      make(chan int, nReduce),
		WorkerNum:   0,
		Phase:       Map,
	}

	// Your code here.
	m.server()
	return &m
}
