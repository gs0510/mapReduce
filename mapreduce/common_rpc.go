package mapreduce

import (
	"fmt"
	"net/rpc"
)

type DoTaskArgs struct {
	JobName    string
	File       string   // only for map, the input file
	Phase      jobPhase // are we in mapPhase or reducePhase?
	TaskNumber int      // this task's index in the current phase

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int
}

// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string // the worker's UNIX-domain socket name, i.e. its RPC address
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
