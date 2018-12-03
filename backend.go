package backend

import (
	"fmt"
	syncfile "github.com/lfonthing/sync_file"
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
	default_backend_storage_rotation_time = 5 * time.Second
)

type BackendServiceData struct {
	Name string
	Task Job
}

type Backend struct {
	tasks         map[string]*Task
	taskJobs      map[string]Job
	tasksInteral  map[string]time.Duration
	tasksDuration map[string]time.Duration
	jobChannel    map[string]chan Job
	storage       map[string]*syncfile.SyncFile
	filepath      map[string]string
	groups        *sync.WaitGroup
	closing       chan bool
	rotation      time.Duration
}

func NewBackend() *Backend {
	return &Backend{
		tasks:         make(map[string]*Task),
		taskJobs:      make(map[string]Job),
		storage:       make(map[string]*syncfile.SyncFile),
		filepath:      make(map[string]string),
		tasksInteral:  make(map[string]time.Duration),
		tasksDuration: make(map[string]time.Duration),
		jobChannel:    make(map[string]chan Job),
		groups:        &sync.WaitGroup{},
		closing:       make(chan bool),
		rotation:      default_backend_storage_rotation_time,
	}
}

var (
	backend = NewBackend()
)

func (this *Backend) Serve() {
	defer this.groups.Done()
	for k, v := range this.tasks {
		this.groups.Add(2)

		// pass param
		go func(key string, task *Task) {
			defer this.groups.Done()

			ticker := time.NewTicker(this.rotation)
			defer ticker.Stop()
			for {
				select {
				case <-this.closing:
					return

				case input := <-this.jobChannel[key]:
					sf := this.storage[key]
					if err := sf.Write(input.Encode()); err != nil {
						fmt.Printf("Failed to store task: %s\n", string(input.Encode()))
					}

				case <-ticker.C:
					sf := this.storage[key]
					if sf.GetFileSize() != 0 {
						data, err := sf.Cut()
						if err != nil {
							fmt.Printf("Failed to get task [%s] from filepath [%s]\n", key, this.filepath[key])
						} else {
							go this.taskJobs[key].Decode(data).Do()
						}
					}
				}
			}
		}(k, v)
	}
}

func (this *Backend) Stop() {
	this.closing <- true
	this.groups.Wait()
}

func BackendStart() {
	backend.groups.Add(1)
	go backend.Serve()

	// recevie program quit
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	// 携程退出
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

	backend.tasks[name] = NewTask(interal, duration)
	backend.taskJobs[name] = job
	backend.filepath[name] = filepath
	backend.storage[name] = syncfile.NewSyncFile(filepath, true)
	backend.tasksInteral[name] = interal
	backend.tasksDuration[name] = duration
	backend.jobChannel[name] = make(chan Job)
}

func SendJob(data Job) {
	backend.jobChannel[data.BackendName()] <- data
}
