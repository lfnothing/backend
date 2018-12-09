package backend

import (
	"encoding/json"
	"fmt"
	"testing"
)

const (
	queue_file = "queue.json"
	queue_size = 10
)

type QueueData struct {
	Seq int `json:"seq"`
}

var (
	testQueue            = NewQueue(queue_file, queue_size)
	queueData1           = QueueData{1}
	queueData2           = QueueData{2}
	queueData3           = QueueData{3}
	queueData4           = QueueData{4}
	queueData5           = QueueData{5}
	queueData6           = QueueData{6}
	queueData7           = QueueData{7}
	queueData8           = QueueData{8}
	queueData9           = QueueData{9}
	queueData10          = QueueData{10}
	queueData11          = QueueData{11}
	encodeQueueData1, _  = json.MarshalIndent(queueData1, "", "  ")
	encodeQueueData2, _  = json.MarshalIndent(queueData2, "", "  ")
	encodeQueueData3, _  = json.MarshalIndent(queueData3, "", "  ")
	encodeQueueData4, _  = json.MarshalIndent(queueData4, "", "  ")
	encodeQueueData5, _  = json.MarshalIndent(queueData5, "", "  ")
	encodeQueueData6, _  = json.MarshalIndent(queueData6, "", "  ")
	encodeQueueData7, _  = json.MarshalIndent(queueData7, "", "  ")
	encodeQueueData8, _  = json.MarshalIndent(queueData8, "", "  ")
	encodeQueueData9, _  = json.MarshalIndent(queueData9, "", "  ")
	encodeQueueData10, _ = json.MarshalIndent(queueData10, "", "  ")
	encodeQueueData11, _ = json.MarshalIndent(queueData11, "", "  ")
	decodeQueueData1     QueueData
	decodeQueueData2     QueueData
	decodeQueueData3     QueueData
	decodeQueueData4     QueueData
	decodeQueueData5     QueueData
	decodeQueueData6     QueueData
	decodeQueueData7     QueueData
	decodeQueueData8     QueueData
	decodeQueueData9     QueueData
	decodeQueueData10    QueueData
	decodeQueueData11    QueueData
)

func TestQueue_Enqueue(t *testing.T) {
	for i := 0; i < 2; i++ {
		testQueue.Enqueue(encodeQueueData1)
		testQueue.Enqueue(encodeQueueData2)
		testQueue.Enqueue(encodeQueueData3)
		testQueue.Enqueue(encodeQueueData4)
		testQueue.Enqueue(encodeQueueData5)
		testQueue.Enqueue(encodeQueueData6)
		testQueue.Enqueue(encodeQueueData7)
		testQueue.Enqueue(encodeQueueData8)
		testQueue.Enqueue(encodeQueueData9)
		testQueue.Enqueue(encodeQueueData10)
		testQueue.Enqueue(encodeQueueData11)
	}

	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData1)
	if decodeQueueData1 != queueData1 {
		fmt.Println("Queue data1 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData2)
	if decodeQueueData2 != queueData2 {
		fmt.Println("Queue data2 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData3)
	if decodeQueueData3 != queueData3 {
		fmt.Println("Queue data3 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData4)
	if decodeQueueData4 != queueData4 {
		fmt.Println("Queue data4 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData5)
	if decodeQueueData5 != queueData5 {
		fmt.Println("Queue data5 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData6)
	if decodeQueueData6 != queueData6 {
		fmt.Println("Queue data6 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData7)
	if decodeQueueData7 != queueData7 {
		fmt.Println("Queue data7 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData8)
	if decodeQueueData8 != queueData8 {
		fmt.Println("Queue data8 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData9)
	if decodeQueueData9 != queueData9 {
		fmt.Println("Queue data9 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData10)
	if decodeQueueData10 != queueData10 {
		fmt.Println("Queue data10 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData11)
	if decodeQueueData11 != queueData11 {
		fmt.Println("Queue data11 don't match")
	}
	testQueue.Save()
}

func TestQueue_InitQueue(t *testing.T) {
	TestQueue_Enqueue(t)
	testQueue.InitQueue()
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData1)
	if decodeQueueData1 != queueData1 {
		fmt.Println("Queue data1 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData2)
	if decodeQueueData2 != queueData2 {
		fmt.Println("Queue data2 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData3)
	if decodeQueueData3 != queueData3 {
		fmt.Println("Queue data3 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData4)
	if decodeQueueData4 != queueData4 {
		fmt.Println("Queue data4 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData5)
	if decodeQueueData5 != queueData5 {
		fmt.Println("Queue data5 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData6)
	if decodeQueueData6 != queueData6 {
		fmt.Println("Queue data6 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData7)
	if decodeQueueData7 != queueData7 {
		fmt.Println("Queue data7 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData8)
	if decodeQueueData8 != queueData8 {
		fmt.Println("Queue data8 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData9)
	if decodeQueueData9 != queueData9 {
		fmt.Println("Queue data9 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData10)
	if decodeQueueData10 != queueData10 {
		fmt.Println("Queue data10 don't match")
	}
	json.Unmarshal(testQueue.Dequeue(), &decodeQueueData11)
	if decodeQueueData11 != queueData11 {
		fmt.Println("Queue data11 don't match")
	}
}
