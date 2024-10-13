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

func (phase JobType) String() string {
	switch phase {
	case MapJob:
		return "MapJob"
	case ReduceJob:
		return "ReduceJob"
	case WaitJob:
		return "WaitJob"
	case CompleteJob:
		return "CompleteJob"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}

type TaskStatus uint8

const (
	Idle TaskStatus = iota
	Working
	Finished
)

func (phase TaskStatus) String() string {
	switch phase {
	case Idle:
		return "Idle"
	case Working:
		return "Working"
	case Finished:
		return "Finished"
	}
	panic(fmt.Sprintf("unexpected SchedulePhase %d", phase))
}

func generateMapResultFileName(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func generateReduceResultFileName(reduceNumber int) string {
	return fmt.Sprintf("mr-out-%d", reduceNumber)
}
