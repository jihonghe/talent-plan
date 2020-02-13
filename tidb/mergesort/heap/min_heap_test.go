package heap

import (
    "fmt"
    "testing"
)

func TestNewMinHeap(t *testing.T) {
    capacity := 4
    minHeap := NewMinHeap(capacity)
    
    t.Run("NewMinHeap", testNewMinHeapFunc(minHeap, capacity))
}

func TestAppendNode(t *testing.T) {
    t.Run("[4, 2, 5, 1, 3]", testAppendNodeFunc(4, []int64{4, 2, 5, 1, 3},
    1, []int64{1, 2, 4, 5}))
    t.Run("[3, 5, 4, 2]", testAppendNodeFunc(4, []int64{3, 5, 4, 2},
    1, []int64{2, 3, 4, 5}))
    t.Run("[3, 5, 2]", testAppendNodeFunc(4, []int64{3, 5, 2},
        1, []int64{2, 3, 5}))
}

func TestPopRoot(t *testing.T) {
    t.Run("[4, 2, 5, 1, 3]", testPopRoot(4, []int64{4, 2, 5, 1, 3}, []int64{1, 2, 4, 5}))
    t.Run("[2, 5, 1, 3]", testPopRoot(4, []int64{2, 5, 1, 3}, []int64{1, 2, 3, 5}))
    t.Run("[2, 1, 3]", testPopRoot(4, []int64{2, 1, 3}, []int64{1, 2, 3}))
}

func testNewMinHeapFunc(minHeap *MinHeap, expectedCap int) func(t *testing.T) {
    return func(t *testing.T) {
        if minHeap == nil {
            t.Error("NewMinHeap() error: the MinHeap instance should not be nil")
        }
        if minHeap.Cap() != expectedCap {
            t.Errorf("NewMinHeap() error: the MinHeap instance`s capacity is not equal to %d.\n", expectedCap)
        }
        if !minHeap.IsEmpty() || minHeap.Len() != 0 {
            t.Error("NewMinHeap() error: the initialized MinHeap instance should be empty")
        }
    }
}

func testPopRoot(heapCapacity int, nodes []int64, expectedPopSequence []int64) func(t *testing.T) {
    // 初始化堆
    minHeap := NewMinHeap(heapCapacity)
    for _, node := range nodes {
        minHeap.AppendNode(node, 0)
    }
    
    return func(t *testing.T) {
        // 测试堆顶元素的弹出顺序是否正确
        for index := 0; !minHeap.IsEmpty(); index++ {
            if minHeap.PopRoot() != expectedPopSequence[index] {
                t.Errorf("PopRoot() error: the pop sequence of heap root nodes is invalid, expected sequence: %s",
                    fmt.Sprintf("%v", expectedPopSequence))
            }
        }
    }
}

func testAppendNodeFunc(heapCapacity int, nodes []int64,
    expectedArrayId int, expectedPopSequence []int64) func(t *testing.T) {
    // 初始化堆
    minHeap := NewMinHeap(heapCapacity)
    for _, node := range nodes {
        minHeap.AppendNode(node, expectedArrayId)
    }
    
    return func(t *testing.T) {
        // 测试堆的长度是否正确
        nodesLength := len(nodes)
        if nodesLength > minHeap.Cap() {
            if minHeap.Len() > minHeap.Cap() {
                t.Errorf("AppendNode() error: nodesLength=%d, heapLength=%d, heapCap=%d, expectedHeapLength=%d",
                    nodesLength, minHeap.Len(), minHeap.Cap(), minHeap.Cap())
            }
        } else {
            if minHeap.Len() != nodesLength {
                t.Errorf("AppendNode() error: nodesLength=%d, heapLength=%d, heapCap=%d, expectedHeapLength=%d",
                    nodesLength, minHeap.Len(), minHeap.Cap(), nodesLength)
            }
        }
        // 测试堆节点所属的序列的编号
        for !minHeap.IsEmpty() {
            arrayId := minHeap.GetRootNodeArrayId()
            minHeap.PopRoot()
            if arrayId != expectedArrayId {
                t.Errorf("AppendNode() error: elementArrayId=%d, expectedArrayId=%d", arrayId, expectedArrayId)
            }
        }
        for _, node := range nodes {
            minHeap.AppendNode(node, expectedArrayId)
        }
        // 测试堆顶元素的弹出顺序是否正确
        for index := 0; !minHeap.IsEmpty(); index++ {
            if minHeap.PopRoot() != expectedPopSequence[index] {
                t.Errorf("AppendNode() error: the pop sequence of heap root nodes is invalid, expected sequence: %s",
                    fmt.Sprintf("%v", expectedPopSequence))
            }
        }
    }
}

