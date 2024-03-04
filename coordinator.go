package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	//"strconv"
	"sync"
	"time"
)

type TaskMetaData struct {
	TaskID   int    //task id
	fileName string //name of the file to read from
	//startTime time.Time //time when task started
	//status    string    //completed, inprogress, notstarted
}

type Coordinator struct {
	// Your definitions here.
	mapTasks              map[int]TaskMetaData //map of map tasks that are not started
	mapTasksInProgress    map[int]TaskMetaData //map of map tasks that are in progress
	mapTasksDone          map[int]TaskMetaData //map of map tasks that are done
	reduceTasks           map[int]TaskMetaData //map of reduce tasks that are not started
	reduceTasksInProgress map[int]TaskMetaData //map of reduce tasks that are in progress
	reduceTasksDone       map[int]TaskMetaData //map of reduce tasks that are done
	mutex                 *sync.RWMutex        //mutex to lock the map and reduce tasks
	mapsRemaining         int                  //number of Map tasks left
	reducesRemaining      int                  //number of reduce tasks left
	nReduce               int                  //number of reduce tasks
	phase                 string               //map or reduce
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CheckExit(args *GetExitResponse, reply *GetExitResponse) error {
	c.mutex.Lock()
	if c.reducesRemaining == 0 {
		reply.Exit = true
		c.mutex.Unlock()
		go func() {
			time.Sleep(3 * time.Second)
			os.Exit(0)
		}()
		return nil
	}
	reply.Exit = false
	c.mutex.Unlock()
	return nil
}
func rescheduler(c *Coordinator, taskType string, taskNumber int, name string) {
	//reschedule the task if a worker crashes

	//if the worker crashes, the rescheduler will time out, notify the coordinator and the coordinator will assign the task to another worker
	timer := time.NewTimer(20 * time.Second)
	<-timer.C //blocks until 10 seconds have passed
	//check if the task is still in progress
	c.mutex.Lock()
	if taskType == "map" {
		if _, ok := c.mapTasksInProgress[taskNumber]; ok {
			//reschedule the tasks in progress
			c.mapTasks[taskNumber] = TaskMetaData{taskNumber, name}
			delete(c.mapTasksInProgress, taskNumber)
		}

		c.mutex.Unlock()
	}
}

// called by workers to notify the coordinator that they are ready to accept a task
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskResponse) error {
	c.mutex.Lock()

	//check if there are any map tasks remaining
	if c.mapsRemaining > 0 {
		k := getMapTask(c)
		if k.fileName == "" {
			reply.Name = ""
			reply.TaskType = "wait"
			reply.Number = 0
			reply.NReduce = 0
			c.mutex.Unlock()
			go rescheduler(c, "map", k.TaskID, k.fileName)
			return nil
		}
		file := k.fileName
		c.mapTasksInProgress[k.TaskID] = TaskMetaData{k.TaskID, file}
		delete(c.mapTasks, k.TaskID)
		c.mutex.Unlock()
		//go rescheduler(k.TaskID, "map", c)
		fmt.Println("sending map task", k, "to worker")
		reply.Name = file
		reply.TaskType = "map"
		reply.Number = k.TaskID
		reply.NReduce = c.nReduce

		return nil

	}

	//check if there are any reduce tasks remaining
	if c.reducesRemaining > 0 && c.mapsRemaining == 0 && len(c.mapTasksInProgress) == 0 {
		if c.phase == "map" {
			c.phase = "reduce"
		}
		fmt.Println("sending reduce task to worker")

		k := getReduceTask(c)

		file := k.fileName
		c.reduceTasksInProgress[k.TaskID] = TaskMetaData{k.TaskID, file}
		delete(c.reduceTasks, k.TaskID)

		//go rescheduler(k.TaskID, "reduce", c)
		reply.Name = file
		reply.TaskType = "reduce"
		reply.Number = k.TaskID
		reply.NReduce = c.nReduce
		c.mutex.Unlock()
		//go rescheduler(k.TaskID, "reduce", c)

		return nil

	}
	return nil
}
func getMapTask(c *Coordinator) TaskMetaData {

	for _, k := range c.mapTasks {
		return k
	}
	return TaskMetaData{}
}
func getReduceTask(c *Coordinator) TaskMetaData {
	if len(c.reduceTasks) == 0 {
		//if there are no reduce tasks left, return an empty task
		return TaskMetaData{0, ""}

	}
	for _, k := range c.reduceTasks {
		return k
	}
	return TaskMetaData{}
}

// called by workers to notify the coordinator that they have completed a task
func (c *Coordinator) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	c.mutex.Lock()

	//update the task status from in progress to done
	if args.TaskType == "map" {
		file := c.mapTasksInProgress[args.Number].fileName
		delete(c.mapTasksInProgress, args.Number)
		c.mapTasksDone[args.Number] = TaskMetaData{args.Number, file}
		c.mapsRemaining--
		c.mutex.Unlock()
		fmt.Println("map task", args.Number, "completed")
		return nil

	} else if args.TaskType == "reduce" {
		file := c.reduceTasksInProgress[args.Number].fileName
		delete(c.reduceTasksInProgress, args.Number)
		c.reduceTasksDone[args.Number] = TaskMetaData{args.Number, file}
		c.reducesRemaining--
		c.mutex.Unlock()
		return nil
	}
	c.mutex.Unlock()
	return nil
}

func rescheduleMaps(c *Coordinator) {
	//reschedule the task if a worker crashes

	//if the worker crashes, the rescheduler will time out, notify the coordinator and the coordinator will assign the task to another worker
	timer := time.NewTimer(10 * time.Second)
	<-timer.C //blocks until 10 seconds have passed
	//check if the task is still in progress
	c.mutex.Lock()
	if len(c.mapTasksInProgress) > 0 {
		for k, v := range c.mapTasksInProgress {
			//reschedule the tasks in progress
			c.mapTasks[k] = v
		}

	}
	c.mutex.Unlock()
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mutex.Lock()
	if len(c.reduceTasksInProgress) == 0 && c.reducesRemaining == 0 && len(c.reduceTasksDone) == c.nReduce {
		ret = true
	}
	c.mutex.Unlock()
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(map[int]TaskMetaData)
	reduceTasks := make(map[int]TaskMetaData)
	mapTasksInProgress := make(map[int]TaskMetaData)
	mapTasksDone := make(map[int]TaskMetaData)
	reduceTasksInProgress := make(map[int]TaskMetaData)
	reduceTasksDone := make(map[int]TaskMetaData)
	// initialize map tasks map
	for i := 0; i < len(files); i++ {
		mapTasks[i] = TaskMetaData{i, files[i]}
	}
	// initialize reduce tasks map
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = TaskMetaData{i, ""}
	}
	lock := &sync.RWMutex{} //lock for the coordinator
	c := Coordinator{}
	c.mapTasks = mapTasks
	c.reduceTasks = reduceTasks
	c.mapTasksInProgress = mapTasksInProgress
	c.mapTasksDone = mapTasksDone
	c.reduceTasksInProgress = reduceTasksInProgress
	c.reduceTasksDone = reduceTasksDone
	c.mutex = lock
	c.mapsRemaining = len(mapTasks)
	c.reducesRemaining = len(reduceTasks)
	c.nReduce = nReduce
	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
