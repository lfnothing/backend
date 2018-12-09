package backend

import (
	syncfile "github.com/lfonthing/sync_file"
	"sync"
)

const (
	default_max_task_queue_size = 100
)

type Queue struct {
	lock    *sync.RWMutex
	data    [][]byte
	front   int
	tail    int
	max     int
	count   int
	storage *syncfile.SyncFile
}

func NewQueue(fp string, maxOption ...int) TaskQueue {
	max := default_max_task_queue_size
	if len(maxOption) != 0 {
		max = maxOption[0]
	}

	return &Queue{
		lock:    &sync.RWMutex{},
		data:    make([][]byte, max),
		front:   0,
		tail:    0,
		count:   0,
		max:     max,
		storage: syncfile.NewSyncFile(fp, true),
	}
}

func (this *Queue) empty() bool {
	return this.count == 0
}

func (this *Queue) overload() bool {
	return this.count == this.max
}

func (this *Queue) enqueue(data []byte) {
	if !this.overload() {
		this.data[this.tail] = data
		this.tail++
		this.count++
		this.tail = this.tail % this.max
		return
	}
	this.storage.Write(true, data)
}

func (this *Queue) InitQueue() {
	for this.storage.GetFileSize() != 0 {
		if this.overload() {
			break
		}
		data, _ := this.storage.Cut()
		this.enqueue(data)
	}
	return
}

func (this *Queue) Enqueue(data []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.enqueue(data)
}

func (this *Queue) Dequeue() (data []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()
	if this.empty() {
		return
	}
	data = this.data[this.front]
	this.front++
	this.count--
	this.front = this.front % this.max
	if this.storage.GetFileSize() != 0 {
		nextData, _ := this.storage.Cut()
		this.enqueue(nextData)
	}
	return
}

func (this *Queue) Save() {
	this.lock.Lock()
	defer this.lock.Unlock()

	rest := make([][]byte, 0, this.count)
	rever := make([][]byte, 0, this.count)
	for !this.empty() {
		rest = append(rest, this.data[this.front])
		this.front++
		this.count--
		this.front = this.front % this.max
	}

	for i := len(rest) - 1; i > -1; i-- {
		rever = append(rever, rest[i])
	}
	this.storage.Write(true, rever...)
}
