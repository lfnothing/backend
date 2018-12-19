package backend

import "sync"

const (
	signature_array_length   = 36
	default_signature_length = 10
)

var (
	signatureArray = [signature_array_length]byte{
		'0',
		'1',
		'2',
		'3',
		'4',
		'5',
		'6',
		'7',
		'8',
		'9',
		'a',
		'b',
		'c',
		'd',
		'e',
		'f',
		'g',
		'h',
		'i',
		'j',
		'k',
		'l',
		'm',
		'n',
		'o',
		'p',
		'q',
		'r',
		's',
		't',
		'u',
		'v',
		'w',
		'x',
		'y',
		'z',
	}
)

type Signature struct {
	length int
	head   []byte
	tail   []byte
	lock   *sync.RWMutex
}

func NewSignture() (this *Signature) {
	this = &Signature{
		length: default_signature_length,
		head:   make([]byte, default_signature_length),
		tail:   make([]byte, default_signature_length),
		lock:   &sync.RWMutex{},
	}
	for i := 0; i < default_signature_length; i++ {
		this.head[i] = signatureArray[0]
		this.tail[i] = signatureArray[0]
	}
	return
}

func (this *Signature) Next(head bool) (s []byte) {
	this.lock.Lock()
	defer this.lock.Unlock()

	s = this.tail
	if head {
		s = this.head
	}

	carry := 1
	for i := this.length - 1; i > -1; i-- {
		for j := 0; j < signature_array_length; j++ {
			if s[i] == signatureArray[j] {
				index := (j + carry) % signature_array_length
				s[i] = signatureArray[index]

				if index != 0 {
					return
				}
			}
		}
	}

	// auto expand
	this.length++
	h := this.head

	s = make([]byte, this.length)
	this.head = make([]byte, this.length)
	this.tail = make([]byte, this.length)

	s[0] = signatureArray[carry]
	this.head[0] = signatureArray[0]
	for i := 1; i < this.length; i++ {
		s[i] = signatureArray[0]
		this.head[i] = h[i-1]
	}
	return
}

func (this *Signature) Head() (s string) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return string(this.head)
}

func (this *Signature) SetHeadNext() {
	s := this.Next(true)
	this.lock.Lock()
	this.head = s
	this.lock.Unlock()
}

func (this *Signature) Tail() (s string) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return string(this.tail)
}

func (this *Signature) SetTailNext() {
	s := this.Next(false)
	this.lock.Lock()
	this.tail = s
	this.lock.Unlock()
}
