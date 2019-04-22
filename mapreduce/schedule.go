package mapreduce

import "fmt"

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	buildTaskArgs := func(phase jobPhase, task int) DoTaskArgs {
		var taskArg DoTaskArgs
		taskArg.JobName = jobName
		taskArg.NumOtherPhase = nOther
		taskArg.Phase = phase
		taskArg.TaskNumber = task
		if phase == mapPhase {
			taskArg.File = mapFiles[task]
		}
		return taskArg
	}

	tasks := make(chan int)

	go func() {
		for i := 0; i < ntasks; i++ {
			tasks <- i
		}
	}()

	successTasks := 0
	success := make(chan int)

loop:

	for {
		select {
		case task := <-tasks:
			go func() {
				worker := <-registerChan
				status := call(worker, "Worker.DoTask", buildTaskArgs(phase, task), nil)
				if status {
					success <- 1
					go func() { registerChan <- worker }()
				} else {
					tasks <- task
				}
			}()

		case <-success:
			successTasks++

		default:
			if successTasks == ntasks {
				break loop
			}

		}

	}

	fmt.Printf("Schedule: %v done\n", phase)
}
