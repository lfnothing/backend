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
	BackendName
	Prepare() bool
	Do() bool
	Save()
	Encode() []byte
	Decode([]byte) Job
}

//--------------------------------------
// task
//--------------------------------------

const (
	default_max_task = 100
)

type Task struct {
	job      Job
	jobs     []Job
	count    int
	interal  time.Duration
	duration time.Duration
	lock     *sync.RWMutex
}

func NewTask(job Job, interal time.Duration, duration time.Duration) *Task {
	return &Task{
		job:      job,
		jobs:     make([]Job, 0),
		count:    0,
		lock:     &sync.RWMutex{},
		interal:  interal,
		duration: duration,
	}
}

func (t *Task) AppendJob(job Job) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.jobs = append(t.jobs, job)
}

func (t *Task) NextJob() (job Job) {
	t.lock.Lock()
	defer t.lock.Unlock()
	job = t.jobs[0]
	t.jobs = t.jobs[1:]
	return
}

func (t *Task) TaskCount() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.count
}

func (t *Task) Add() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.count++
}

func (t *Task) Reduce() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.count--
}

func (t *Task) Overload() bool {
	return t.TaskCount() > default_max_task
}

func (t *Task) SetInteral(interal time.Duration) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.interal = interal
}

func (t *Task) GetInteral() time.Duration {
	t.lock.RLock()
	t.lock.RUnlock()
	return t.interal
}

func (t *Task) SetDuration(duration time.Duration) {
	t.lock.Lock()
	t.lock.Unlock()
	t.duration = duration
}

func (t *Task) GetDuration() time.Duration {
	t.lock.RLock()
	t.lock.RUnlock()
	return t.duration
}

func (t *Task) Do(ctx context.Context) {
	t.Add()
	defer t.Reduce()
	job := t.NextJob()

	for {
		select {
		case <-ctx.Done():
			t.AppendJob(job)
			return

		default:
			if success := job.Prepare(); !success {
				job.Save()
				return
			}

			var fail = true
			var interal = t.GetInteral()
			var expire = time.Now().Add(t.GetDuration())
			for now := time.Now(); now.Before(expire); {
				if finsh := job.Do(); finsh {
					fail = false
					break
				}
				if time.Now().After(now.Add(interal)) {
					continue
				}
				time.Sleep(interal - time.Now().Sub(now))
			}

			if fail {
				job.Save()
			}
			return
		}
	}
}
