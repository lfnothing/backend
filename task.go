package backend

import (
	"sync"
	"time"
)

//--------------------------------------
// task
//--------------------------------------

const (
	default_max_task = 100
)

type Task struct {
	count    int
	interal  time.Duration
	duration time.Duration
	lock     *sync.RWMutex
}

func NewTask(interal time.Duration, duration time.Duration) *Task {
	return &Task{
		count:    0,
		lock:     &sync.RWMutex{},
		interal:  interal,
		duration: duration,
	}
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

type Job interface {
	BackendName
	Prepare() bool
	Do() bool
	Save()
	Encode() []byte
	Decode([]byte) Job
}

func (t *Task) Do(job Job) {
	if success := job.Prepare(); !success {
		job.Save()
		return
	}

	t.Add()
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
	t.Reduce()
}
