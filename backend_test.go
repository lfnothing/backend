package backend

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

type TaskJob struct {
	Seq int `json:"seq"`
}

type Backend1TaskJob TaskJob

func (this *Backend1TaskJob) BackendName() string {
	return "backend1taskjob"
}

func (this *Backend1TaskJob) Save() {
	SendJob(this)
}

func (this *Backend1TaskJob) Encode() (data []byte) {
	data, _ = json.MarshalIndent(this, "", "  ")
	return
}

func (this *Backend1TaskJob) Decode(data []byte) (j Job) {
	var job Backend1TaskJob
	json.Unmarshal(data, &job)
	j = &job
	return
}

func (this *Backend1TaskJob) Prepare() bool {
	return false
}

func (this *Backend1TaskJob) Do() bool {
	fmt.Fprintf(os.Stderr, "Backend [%s] task with seq [%d]\n", this.BackendName(), this.Seq)
	return true
}

type Backend2TaskJob TaskJob

func (this *Backend2TaskJob) BackendName() string {
	return "backend2taskjob"
}

func (this *Backend2TaskJob) Save() {
	SendJob(this)
}

func (this *Backend2TaskJob) Encode() (data []byte) {
	data, _ = json.MarshalIndent(this, "", "  ")
	return
}

func (this *Backend2TaskJob) Decode(data []byte) (j Job) {
	var job Backend2TaskJob
	json.Unmarshal(data, &job)
	j = &job
	return
}

func (this *Backend2TaskJob) Prepare() bool {
	return true
}

func (this *Backend2TaskJob) Do() bool {
	fmt.Fprintf(os.Stderr, "Backend [%s] task with seq [%d]\n", this.BackendName(), this.Seq)
	return true
}

func TestRegisterBackend(t *testing.T) {
	RegisterBackend("backend1task.json", new(Backend1TaskJob))
	RegisterBackend("backend2task.json", new(Backend2TaskJob))
}

func TestBackend_Serve(t *testing.T) {
	TestRegisterBackend(t)
	go BackendStart()

	go func() {
		for i := 0; i < 100; i++ {
			job1 := Backend1TaskJob{
				Seq: i,
			}
			SendJob(&job1)
		}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			job2 := Backend2TaskJob{
				Seq: i,
			}
			SendJob(&job2)
		}
	}()

	time.Sleep(100 * time.Second)
}
