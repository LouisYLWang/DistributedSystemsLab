package mr

const (
	OK     = "OK"
	Map    = "MAP"
	Reduce = "RED"

	NoNewTask       = "NoNewTask"
	AllTaskFinished = "AllTaskFinished"

	ErrNoKey        = "ErrNoKey"
	ErrNoConnection = "ErrNoConnection"
	ErrInvalidReq   = "ErrInvalidReq"

	// request type
	RequestTask    = "RequestTask"
	FinishTask     = "FinishTask"
	RegisterWorker = "RegisterWorker"
	AbortTask      = "AbortTask"

	// Task WorkerID
	NotAssignedWorker = -1
	FinishedWorker    = -2

	// File type
	taskFile         = 1
	intermediateFile = 2
	reduceOutputFile = 3
)
