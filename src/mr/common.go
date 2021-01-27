package mr

type (
	Task struct {
		ID       int
		file     string
		workerID int
		taskType int
	}

	File string
	//File struct {
	//	Filename string
	//	FileType int // mr-out-X (map), mr-X-Y (intermediate)
	//}
)
