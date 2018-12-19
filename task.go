package backend

import (
	"context"
	"sync"
	"time"
)

//--------------------------------------
// job
//--------------------------------------

type Job interface {
	TaskName
	Prepare() bool
	Do() bool
	Encode() []byte
	Decode([]byte) Job
}

//--------------------------------------
// broker
//--------------------------------------

type Broker interface {
	Init()
	Set(string, string, []byte)
	Get(string, string) []byte
	Delete(string, string)
}

//--------------------------------------
// task
//--------------------------------------

type TaskName interface {
	TaskName() string
}

const (
	default_max_task = 100
)

type Task struct {
	name     string
	doing    int
	interal  time.Duration
	duration time.Duration
	manager  *Manager
	lock     *sync.RWMutex
}

func NewTask(j Job, mode TaskModel, interal time.Duration, duration time.Duration, broker Broker) *Task {
	return &Task{
		name:     j.TaskName(),
		doing:    0,
		lock:     &sync.RWMutex{},
		interal:  interal,
		duration: duration,
		manager:  NewManager(j, mode, broker),
	}
}

func (t *Task) TaskAdd() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.doing++
}

func (t *Task) TaskReduce() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.doing--
}

func (t *Task) TaskDoing() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.doing
}

func (t *Task) TaskOverload() bool {
	return t.TaskDoing() > default_max_task
}

func (this *Task) Enqueue(job Job) {
	this.manager.Enqueue(this.name, job)
}

func (this *Task) Do(ctx context.Context) {
	if this.TaskOverload() {
		return
	}

	job, sign, success := this.manager.Dequeue(this.name)
	if !success {
		return
	}

	this.TaskAdd()
	for {
		select {
		case <-ctx.Done():
			this.TaskReduce()
			this.manager.JobFailed(this.name, job, sign)
			return

		default:
			var expire = time.Now().Add(this.duration)
			if success = job.Prepare(); !success {
				goto Exit
			}

			for now := time.Now(); now.Before(expire); {
				if success = job.Do(); success {
					goto Exit
				}
				if time.Now().After(now.Add(this.interal)) {
					continue
				}
				time.Sleep(this.interal - time.Now().Sub(now))
			}
		Exit:
			this.TaskReduce()
			if !success {
				this.manager.JobFailed(this.name, job, sign)
				return
			}
			this.manager.JobSucceed(this.name, sign)
			return
		}
	}
}
