package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {

		reply, err := CallGetTask()

		if err != nil {
			log.Fatal("Error in getting task")
		}
		if reply.TaskType == "map" {
			executeMapTask(mapf, reply.Name, reply)
			CallUpdateTask(reply.Number, "map")

		} else if reply.TaskType == "reduce" {
			executeReduceTask(reducef, reply)

			CallUpdateTask(reply.Number, "reduce")
		} else if reply.TaskType == "wait" {
			//fmt.Println("No task, waiting for .5 seconds before asking again")
			time.Sleep(500 * time.Millisecond)
		} else {
			exit, _ := CallCheckExit()
			if exit.Exit {
				os.Exit(0)
			}
		}
		time.Sleep(1 * time.Second)
	}

}
func executeMapTask(mapf func(string, string) []KeyValue, filename string, reply *GetTaskResponse) {

	file, err := os.Open(filename)
	fmt.Println("opening", filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Name)
	}
	//go rescheduler(reply.Number, reply.TaskType)
	//content := make([]byte, reply.NReduce)
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//var mu sync.Mutex
	m := map[int]*os.File{}
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % reply.NReduce
		//write to the intermediate file
		//check if intermediate file has already been created, if not create it
		//if created, append to it/write to it
		//if not created, create it and write to it
		//name := "mr-" + strconv.Itoa(reply.Number) + "-" + strconv.Itoa(reduceTaskNumber)
		x, ok := m[reduceTaskNumber] //gets the corresponding temp file for the reduce task if it exists, if not make it
		if !ok {
			temp, err := os.CreateTemp("./", "tmp")
			if err != nil {
				log.Fatalf("cannot create temp file")
			}
			m[reduceTaskNumber] = temp
			x = temp
		}

		enc := json.NewEncoder(x)
		//fmt.Println("file is size")
		err = enc.Encode(&kv)
		//fmt.Println("writing", kv, "to", x.Name())
		if err != nil {
			log.Fatalf("cannot encode intermediate file")
		}
		//fmt.Println(reply.Number, "map done")

	}

	for ReduceNum, file := range m {

		//rename the temp files to the final files
		//expected format is mr-final-<taskNumber>-<reduceTaskNumber>
		file.Close()
		os.Rename(file.Name(), "mr-"+strconv.Itoa(reply.Number)+"-"+strconv.Itoa(ReduceNum))

	}

}

func CallCheckExit() (*GetExitResponse, error) {
	args := GetExitResponse{}
	reply := GetExitResponse{}
	ok := call("Coordinator.CheckExit", &args, &reply)
	if ok {
		if reply.Exit {
			os.Exit(0)
		}
		return &reply, nil
	} else {
		//if the RPC call fails, we assume the coordinator is done and we can exit
		fmt.Println("Coordinator unreachable, exiting")
		os.Exit(0)
		return &reply, nil
	}
}
func executeReduceTask(reducef func(string, []string) string, reply *GetTaskResponse) {
	// Code for reduce task goes here
	// read the intermediate files and call the reduce function on them
	// write the output to the output file with the format mr-out-<reduceTaskNumber>
	//expected input is from the intermediate files renamed as mr-final-<taskNumber>-<reduceTaskNumber>
	//go rescheduler(reply.Number, reply.TaskType)
	files, err := os.ReadDir("./")

	fileNames := make([]string, 0)
	data := []KeyValue{}
	if err != nil {
		log.Fatalf("cannot read directory")
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), strconv.Itoa(reply.Number)) {
			//check if the file is the right intermediate file for the reduce task
			fileNames = append(fileNames, file.Name())
			//fmt.Println("appending file", file.Name()+"to fileNames")
		}

	}
	for _, file := range fileNames {
		//read the intermediate file
		intermediateFile, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open intermediate file")
		}
		dec := json.NewDecoder(intermediateFile)

		for {
			var kv KeyValue

			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("cannot decode intermediate file")
			}
			//fmt.Println("appending", kv)
			data = append(data, kv)
		}
	}
	//sort the data
	sort.Sort(ByKey(data))
	oname := "mr-out-" + strconv.Itoa(reply.Number)
	ofile, _ := os.Create(oname)
	//fmt.Println("created file", oname+"data is size", len(data))
	//groups the data by key and calls the reduce function on the values
	//writes the output to the output file
	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := reducef(data[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)

		i = j
	}
	ofile.Close()

}

// function to call the get task RPC to get a task from the coordinator
func CallGetTask() (*GetTaskResponse, error) {
	args := GetTaskArgs{}      //no arguments required
	reply := GetTaskResponse{} //reply from the coordinator with the task details
	//starts empty but will be filled with the task details via RPC
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		//if the RPC call fails, we assume the coordinator is done and we can exit
		os.Exit(0)
		return nil, fmt.Errorf("Error in getting task")
	} else {
		return &reply, nil

	}
}

func CallUpdateTask(taskNumber int, taskType string) (*UpdateTaskReply, error) {
	args := UpdateTaskArgs{}
	args.Number = taskNumber
	args.TaskType = taskType
	reply := UpdateTaskReply{}
	ok := call("Coordinator.UpdateTask", &args, &reply)
	if !ok {
		//if the RPC call fails, we assume the coordinator is done and we can exit
		fmt.Println("exit")
		os.Exit(0)
		return nil, fmt.Errorf("Error in updating task")
	} else {
		return &reply, nil
	}
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)

	return false
}
