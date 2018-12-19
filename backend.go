package backend

import (
	"backend/broker"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

//--------------------------------------
// backends
//--------------------------------------

const (
	default_backend_task_interal          = 2 * time.Minute
	default_backend_task_duration         = 10 * time.Minute
	default_backend_storage_rotation_time = 10 * time.Second
)

type Backend struct {
	tasks    map[string]*Task
	jobChan  map[string]chan Job
	closing  map[string]chan bool
	groups   *sync.WaitGroup
	rotation time.Duration
}

func NewBackend() *Backend {
	return &Backend{
		tasks:    make(map[string]*Task),
		jobChan:  make(map[string]chan Job),
		groups:   &sync.WaitGroup{},
		closing:  make(map[string]chan bool),
		rotation: default_backend_storage_rotation_time,
	}
}

var (
	backend = NewBackend()
)

func (this *Backend) Serve() {
	defer this.groups.Done()

	ctx, cancel := context.WithCancel(context.Background())
	for k, v := range this.tasks {
		this.groups.Add(1)

		// pass param
		go func(key string, task *Task) {
			defer this.groups.Done()

			ticker := time.NewTicker(this.rotation)
			defer ticker.Stop()
			for {
				select {
				// exit all task then save all jobs
				case <-this.closing[key]:
					cancel()
					return

				case input := <-this.jobChan[key]:
					this.tasks[key].Enqueue(input)

				case <-ticker.C:
					go task.Do(ctx)
				}
			}
		}(k, v)
	}
}

func (this *Backend) Stop() {
	for _, v := range this.closing {
		v <- true
	}
	this.groups.Wait()
}

func BackendStart() {
	backend.groups.Add(1)
	go backend.Serve()

	// recevie program quit
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	backend.Stop()
}

//--------------------------------------
// backend register
//--------------------------------------

func RegisterBackend(job Job, taskMode TaskModel, times ...time.Duration) {
	var interal time.Duration
	var duration time.Duration

	name := job.TaskName()
	if _, ok := backend.tasks[name]; ok {
		panic("backend task has register")
	}

	switch len(times) {
	case 0:
		interal = default_backend_task_interal
		duration = default_backend_task_duration
	case 1:
		interal = times[0]
		duration = default_backend_task_duration
	case 2:
		interal = times[0]
		duration = times[1]
	default:
		interal = times[0]
		duration = times[1]
	}

	backend.tasks[name] = NewTask(job, taskMode, interal, duration, broker.NewRedisBroker())
	backend.jobChan[name] = make(chan Job)
	backend.closing[name] = make(chan bool)
}

func SendJob(data Job) {
	backend.jobChan[data.TaskName()] <- data
}
