package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//1.apply workers' request 2.gen Map&Reduce Task 3.decide which stage it is
//4. recycle Task
type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex
	stage          string
	nReduce        int
	nMap           int
	tasks          map[string]Task
	availableTasks chan Task
}

//RPC handler
//see if  there's a last task ,if there's, change its status, rename its outputfile,
//and then delete it from c's tasks, change coordinator's status
//last assign a task to the  worker
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	//if there's a last task
	if args.LastTaskType != "" {
		c.lock.Lock()
		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerID == args.WorkerID {
			log.Printf("Mark %s task %d as finished on worker %s\n",
				task.Type, task.Index, args.WorkerID)
		}
		if args.LastTaskType == MAP {
			for ri := 0; ri < c.nReduce; ri++ {
				err := os.Rename(tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri),
					finalMapOutFile(args.LastTaskIndex, ri))
				if err != nil {
					log.Fatalf(
						"Failed to mark map output file %s as final: %e",
						tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri), err)
				}
			}
		} else if args.LastTaskType == REDUCE {
			err := os.Rename(tmpReduceOutFile(args.WorkerID, args.LastTaskIndex),
				finalReduceOutFile(args.LastTaskIndex))
			if err != nil {
				log.Fatalf("Failed to mark reduce output file %s as final: %e",
					tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), err)
			}
		}

		delete(c.tasks, lastTaskID)

		if len(c.tasks) == 0 {
			c.transit()
		}
		c.lock.Unlock()
	}

	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, args.WorkerID)

	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	return nil
}

//change current state
func (c *Coordinator) transit() {
	if c.stage == MAP {
		log.Printf("All Map tasks finished. Transit to Reduce stage\n")
		c.stage = REDUCE

		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  REDUCE,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == REDUCE {
		log.Printf("All Reduce tasks finished. Prepare to exit\n")
		close(c.availableTasks)
		c.stage = ""
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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
 
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stage == ""
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	//gen map task, and make it available for coodinator
	for i, file := range files {
		task := Task{
			Type:         MAP,
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordiantor start\n")
	c.server()

	//goroutine for recycling tasks
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()
			for _, task := range c.tasks {
				//if the task has been used
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf("Found time-out %s task %d previously running on worker %s. Prepare to re-assign",
						task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func GenTaskID(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
