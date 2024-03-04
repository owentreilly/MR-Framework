package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}
type UpdateTaskArgs struct {
	//sent from worker to coordinator to signify that the task is done and should be moved from in progress to done
	Number   int    //number of the task
	TaskType string //type of the task, map or reduce

}
type UpdateTaskReply struct {
}
type GetTaskArgs struct {
	//just notifiy the coordinator that the worker is ready to accept a task
}
type RescheduleTaskArgs struct {
	//sent from goroutine spawned by worker to signify task is taking too long and should be reassigned
	Number   int    //number of the task
	TaskType string //type of the task, map or reduce
	Name     string //name of the file to be processed

}
type RescheduleTaskReply struct {
	//empty reply
}
type GetExitResponse struct {
	//sent from worker to signify it does not see a reduce task and ask if it is done
	Exit bool //true if worker is done
}

// Add your RPC definitions here.
type GetTaskResponse struct {
	//coordinator sends response to the get task request from the worker
	//contains the task type, file name, task # and number of reduce tasks used for hashing
	Name     string //name of the file to be processed
	TaskType string //type of the task, map or reduce
	Number   int    //number of the task
	NReduce  int    //number of reduce tasks and buckets for hashing
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
