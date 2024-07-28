package mr

import "fmt"

type SechedulePhase uint8

const (
	MapPhase SechedulePhase = iota
	ReducePhase
	CompletePhase
)

func (phase SechedulePhase) String() string {
	switch phase {
	case MapPhase:
		return "MapPhase"
	case ReducePhase:
		return "ReducePhase"
	case CompletePhase:
		return "CompletePhase"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}

type JobType uint8

const (
	MapJob JobType = iota
	ReduceJob
	WaitJob
	CompleteJob
)

type TaskStatus uint8

const (
	Idle TaskStatus = iota
	Working
	Finished
)

func generateMapResultFileName(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func generateReduceResultFileName(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceNumber)
}
