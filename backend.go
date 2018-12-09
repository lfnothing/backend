package backend

import (
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
	default_backend_task_interal          = 60 * time.Second
	default_backend_task_duration         = 10 * time.Minute
	default_backend_storage_rotation_time = 10 * time.Second
)

type TaskQueue interface {
	InitQueue()
	Enqueue([]byte)
	Dequeue() []byte
	Save()
}

type Backend struct {
	tasks         map[string]*Task
	tasksInteral  map[string]time.Duration
	tasksDuration map[string]time.Duration
	jobChannel    map[string]chan Job
	queue         map[string]TaskQueue
	filepath      map[string]string
	groups        *sync.WaitGroup
	closing       map[string]chan bool
	rotation      time.Duration
}

func NewBackend() *Backend {
	return &Backend{
		tasks:         make(map[string]*Task),
		queue:         make(map[string]TaskQueue),
		filepath:      make(map[string]string),
		tasksInteral:  make(map[string]time.Duration),
		tasksDuration: make(map[string]time.Duration),
		jobChannel:    make(map[string]chan Job),
		groups:        &sync.WaitGroup{},
		closing:       make(map[string]chan bool),
		rotation:      default_backend_storage_rotation_time,
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
					queue := this.queue[key]
					taskCount := this.tasks[key].TaskCount()
					for i := 0; i < taskCount; i++ {
						queue.Enqueue(this.tasks[key].NextJob().Encode())
					}
					this.queue[key].Save()
					return

				case input := <-this.jobChannel[key]:
					this.queue[key].Enqueue(input.Encode())

				case <-ticker.C:
					if this.tasks[key].Overload() {
						break
					}
					data := this.queue[key].Dequeue()
					if len(data) != 0 {
						task.AppendJob(task.job.Decode(data))
						go task.Do(ctx)
					}
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

type BackendName interface {
	BackendName() string
}

func RegisterBackend(filepath string, job Job, times ...time.Duration) {
	var interal time.Duration
	var duration time.Duration

	name := job.BackendName()
	if _, ok := backend.tasks[name]; ok {
		panic("backend task has register")
	}

	if _, ok := backend.filepath[filepath]; ok {
		panic("storage filepath has register")
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

	backend.tasks[name] = NewTask(job, interal, duration)
	backend.filepath[name] = filepath
	backend.queue[name] = NewQueue(filepath)
	backend.tasksInteral[name] = interal
	backend.tasksDuration[name] = duration
	backend.jobChannel[name] = make(chan Job)
	backend.closing[name] = make(chan bool)
	for k, _ := range backend.queue {
		backend.queue[k].InitQueue()
	}
}

func SendJob(data Job) {
	backend.jobChannel[data.BackendName()] <- data
}
