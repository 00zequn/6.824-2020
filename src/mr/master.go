package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MaxTaskRunInterval = time.Second * 10
)

type Task struct {
	filename  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type Master struct {
	files   []string
	nReduce int
	nMap    int
	phase   SechedulePhase
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *HeartBeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

func (m *Master) Heartbeat(request *HeartBeatRequest, reponse *HeartBeatResponse) error {
	msg := heartbeatMsg{reponse, make(chan struct{})}
	m.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (m *Master) Report(request *ReportRequest, reponse *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	m.reportCh <- msg
	<-msg.ok
	return nil
}
func (m *Master) selectTask(response *HeartBeatResponse) bool {
	allFinished, hasNewJob := true, false
	for id, task := range m.tasks {
		switch task.status {
		case Idle:
			allFinished, hasNewJob = false, true
			m.tasks[id].status = Working
			m.tasks[id].startTime = time.Now()
			response.NReduce = m.nReduce
			response.Id = id
			if m.phase == MapPhase {
				response.JobType = MapJob
				response.FilePath = m.files[id]
			} else {
				response.JobType = ReduceJob
				response.NMap = m.nMap
			}
		case Working:
			allFinished = false
			if time.Now().Sub(task.startTime) > MaxTaskRunInterval {
				hasNewJob = true
				m.tasks[id].startTime = time.Now()
				response.NReduce = m.nReduce
				response.Id = id
				if m.phase == MapPhase {
					response.JobType = MapJob
					response.FilePath = m.files[id]
				} else {
					response.JobType = ReduceJob
					response.NMap = m.nMap
				}
			}
		case Finished:
		}
		if hasNewJob {
			break
		}
	}
	if !hasNewJob {
		response.JobType = WaitJob
	}
	return allFinished
}

func (m *Master) scheule() {
	m.initMapPhase()
	for {
		select {
		case msg := <-m.heartbeatCh:
			if m.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if m.selectTask(msg.response) {
				switch m.phase {
				case MapPhase:
					log.Printf("master: %v finished, start %v \n", MapPhase, ReducePhase)
					m.initReducePhase()
					m.selectTask(msg.response)
				case ReducePhase:
					log.Printf("master: %v finished, Congraulation.\n", ReducePhase)
					m.initCompletePhase()
					msg.response.JobType = CompleteJob
				case CompletePhase:
					panic(fmt.Sprintf("master: enter unexpected branch.\n"))
				}
			}
			log.Printf("master: assigned a task %v to worker", msg.response)
			msg.ok <- struct{}{}

		case msg := <-m.reportCh:
			if msg.request.Phase == m.phase {
				log.Printf("Master: Worker has executed task %v\n", msg.request)
				m.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}

func (m *Master) initMapPhase() {
	m.phase = MapPhase
	m.tasks = make([]Task, len(m.files))
	for i, file := range m.files {
		m.tasks[i] = Task{
			filename: file,
			id:       i,
			status:   Idle,
		}
	}
}

func (m *Master) initReducePhase() {
	m.phase = ReducePhase
	m.tasks = make([]Task, m.nReduce)
	for i := 0; i < m.nReduce; i++ {
		m.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}
func (m *Master) initCompletePhase() {
	m.phase = CompletePhase
	m.doneCh = make(chan struct{})
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	<-m.doneCh
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}),
	}

	// Your code here.
	m.server()
	go m.scheule()
	return &m
}
