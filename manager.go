package backend

import "sync"

type TaskModel int

type SerialJobStatus int

const (
	Parallel TaskModel = iota
	Serial

	Unallocated SerialJobStatus = iota
	Doing
	UnDone
)

type Manager struct {
	job       Job
	count     uint64
	model     TaskModel
	broker    Broker
	sign      *Signature
	lock      *sync.RWMutex
	JobStatus SerialJobStatus
}

func NewManager(job Job, model TaskModel, broker Broker) *Manager {
	return &Manager{
		job:       job,
		count:     0,
		model:     model,
		broker:    broker,
		sign:      NewSignture(),
		lock:      &sync.RWMutex{},
		JobStatus: Unallocated,
	}
}

func (this *Manager) AddCount() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.count++
}

func (this *Manager) ReduceCount() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.count--
}

func (this *Manager) Count() uint64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.count
}

func (this *Manager) SerialJobStatus() SerialJobStatus {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.JobStatus
}

func (this *Manager) SetSerialJobStatus(s SerialJobStatus) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.JobStatus = s
}

func (this *Manager) Enqueue(name string, job Job) {
	this.AddCount()
	this.broker.Set(name, this.sign.Tail(), job.Encode())
	this.sign.SetTailNext()
}

func (this *Manager) Dequeue(name string) (job Job, sign string, success bool) {
	if this.Count() == 0 {
		return
	}

	if this.model == Serial && this.SerialJobStatus() == Doing {
		return
	}

	success = true
	sign = this.sign.Head()
	job = this.job.Decode(this.broker.Get(name, sign))

	if this.model == Parallel {
		this.JobNext()
		return
	}

	this.SetSerialJobStatus(Doing)
	return
}

func (this *Manager) JobNext() {
	this.ReduceCount()
	this.sign.SetHeadNext()
}

func (this *Manager) JobFailed(name string, job Job, sign string) {
	if this.model == Parallel {
		this.Enqueue(name, job)
		return
	}

	this.SetSerialJobStatus(UnDone)
}

func (this *Manager) JobSucceed(name string, sign string) {
	this.broker.Delete(name, sign)

	if this.model == Serial {
		this.JobNext()
		this.SetSerialJobStatus(Unallocated)
	}
}
