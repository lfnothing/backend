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

func (this *Backend1TaskJob) TaskName() string {
	return "backend1taskjob"
}

func (this *Backend1TaskJob) Encode() (data []byte) {
	data, _ = json.Marshal(this)
	return
}

func (this *Backend1TaskJob) Decode(data []byte) (j Job) {
	var job Backend1TaskJob
	json.Unmarshal(data, &job)
	j = &job
	return
}

func (this *Backend1TaskJob) Prepare() bool {
	return true
}

func (this *Backend1TaskJob) Do() bool {
	if this.Seq > 10 {
		return false
	}
	fmt.Fprintf(os.Stderr, "%-10s task [%s] with seq [%d]\n", "Parallel", this.TaskName(), this.Seq)
	return true
}

type Backend2TaskJob TaskJob

func (this *Backend2TaskJob) TaskName() string {
	return "backend2taskjob"
}

func (this *Backend2TaskJob) Encode() (data []byte) {
	data, _ = json.Marshal(this)
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
	if this.Seq > 10 {
		return false
	}
	fmt.Fprintf(os.Stderr, "%-10s task [%s] with seq [%d]\n", "Serial", this.TaskName(), this.Seq)
	return true
}

func TestRegisterBackend(t *testing.T) {
	RegisterBackend(new(Backend1TaskJob), Parallel)
	RegisterBackend(new(Backend2TaskJob), Serial)
}

func TestBackend_Serve(t *testing.T) {
	TestRegisterBackend(t)
	go BackendStart()

	go func() {
		for i := 0; i < 12; i++ {
			job1 := Backend1TaskJob{
				Seq: i,
			}
			SendJob(&job1)
		}
	}()

	go func() {
		for i := 0; i < 12; i++ {
			job2 := Backend2TaskJob{
				Seq: i,
			}
			SendJob(&job2)
		}
	}()

	time.Sleep(200 * time.Second)
}
