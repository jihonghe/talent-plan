package main

import (
    "errors"
    "fmt"
)

type MinHeap struct {
    elements []*UrlCount
    // length用于记录elements中有效的元素范围
    length int
    capacity int
}

// NewMinHeap: 创建一个容量为capacity的最小堆
// param capacity: 作为最小堆底层序列的容量，是堆结点个数的限制
// return: 返回MinHeap实例的指针
func NewMinHeap(capacity int, ucs... *UrlCount) *MinHeap {
    minHeap := &MinHeap{
        // 直接分配len和cap是为了避免使用append()带来的内存分配资源消耗
        elements: make([]*UrlCount, capacity, capacity),
        length: 0,
        capacity: capacity,
    }
    for _, uc := range ucs {
        minHeap.AppendNode(uc)
    }
    
    return minHeap
}

func (minHeap MinHeap) Len() int {
    return minHeap.length
}

func (minHeap MinHeap) Cap() int {
    return minHeap.capacity
}

func (minHeap MinHeap) IsEmpty() bool {
    return minHeap.length == 0
}

func (minHeap MinHeap) IsFull() bool {
    return minHeap.length == minHeap.capacity
}

// AppendNode: 向堆中添加新节点并调整新节点在堆中的位置
// param element: 节点值
// param elementArrayId: 节点值所属的有序子序列在二维切片中的下标，用于后续执行PopRoot()操作时获取堆顶元素的所属子序列
// return: nil
func (minHeap *MinHeap) AppendNode(element *UrlCount) {
    if minHeap.IsFull() {
        return
    }
    
    // 在二叉堆末尾添加新节点
    minHeap.elements[minHeap.length] = element
    minHeap.length++
    
    minHeap.siftUp(minHeap.length - 1)
}

func (minHeap MinHeap) GetRoot() *UrlCount {
    if minHeap.IsEmpty() {
        return nil
    }
    return minHeap.elements[0]
}

// PopRoot: 取出堆结点并调整堆，需要注意的是取堆顶元素时不会校验堆是否为空，因此需要在调用该函数前校验堆是否为空
// return: 返回堆顶元素值
func (minHeap *MinHeap) PopRoot() *UrlCount {
    element := minHeap.elements[0]
    
    // 删除根节点，并将二叉堆的最后一个叶节点放到根节点上
    minHeap.elements[0] = minHeap.elements[minHeap.length - 1]
    minHeap.length--
    
    minHeap.siftDown(0)
    
    return element
}

// ReplaceRoot: 直接替换堆顶元素并进行相应的位置调整
func (minHeap *MinHeap) ReplaceRoot(uc *UrlCount) {
    minHeap.elements[0] = uc
    minHeap.siftDown(0)
}

func (minHeap MinHeap) GetReverseElements() []*UrlCount {
    InsertionSort(minHeap.elements)
    return minHeap.elements
    // copy(ucs, minHeap.elements)
    // InsertionSort(ucs)
}

// siftUp: 对给出的在堆中对应下标的节点进行向上位置调整，以维持堆的有效性
// param nodeIndex: 堆结点在底层序列对应的下标
// return: nil
func (minHeap *MinHeap) siftUp(nodeIndex int) {
    if minHeap.length <= 1 {
        return
    }
    
    for {
        parentIndex := getParentIndex(nodeIndex)
        if less(minHeap.elements[nodeIndex], minHeap.elements[parentIndex]) {
            minHeap.exchangeTwoNodes(nodeIndex, parentIndex)
            nodeIndex = parentIndex
        } else {
            break
        }
    }
    
}

// siftDown: 对给出的在堆中对应下表的节点进行向下位置调整，维持堆的有效性
// param nodeIndex: 堆结点在底层序列对应的下标
// return: nil
func (minHeap *MinHeap) siftDown(nodeIndex int) {
    if minHeap.length <= 1 {
        return
    }
    
    for {
        lessNodeIndex := nodeIndex
        
        leftNodeIndex := getLeftNodeIndex(nodeIndex)
        if !minHeap.isValidIndex(leftNodeIndex) {
            return
        }
        
        rightNodeIndex := getRightNodeIndex(nodeIndex)
        if !minHeap.isValidIndex(rightNodeIndex) {
            lessNodeIndex = leftNodeIndex
        } else {
            lessNodeIndex = minHeap.getLessNodeIndex(leftNodeIndex, rightNodeIndex)
        }
        
        if !less(minHeap.elements[nodeIndex], minHeap.elements[lessNodeIndex]) {
            minHeap.exchangeTwoNodes(nodeIndex, lessNodeIndex)
            nodeIndex = lessNodeIndex
        } else {
            return
        }
    }
}

func getParentIndex(nodeIndex int) int {
    if nodeIndex == 0 {
        return 0
    }
    
    return (nodeIndex - 1) / 2
}

func getLeftNodeIndex(parentIndex int) int {
    return 2 * parentIndex + 1
}

func getRightNodeIndex(parentIndex int) int {
    return 2 * parentIndex + 2
}

func (minHeap MinHeap) getLessNodeIndex(nodeIndex1, nodeIndex2 int) int {
    if nodeIndex1 >= minHeap.length || nodeIndex2 >= minHeap.length {
        panic(errors.New(fmt.Sprintf("index out of range: nodeIndex1=%d, nodeIndex2=%d\n", nodeIndex1, nodeIndex2)))
    }
    
    if less(minHeap.elements[nodeIndex1], minHeap.elements[nodeIndex2]) {
        return nodeIndex1
    }
    
    return nodeIndex2
}

func (minHeap MinHeap) isValidIndex(nodeIndex int) bool {
    return nodeIndex < minHeap.length
}

func (minHeap *MinHeap) exchangeTwoNodes(nodeIndex1, nodeIndex2 int) {
    // 交换元素
    minHeap.elements[nodeIndex1], minHeap.elements[nodeIndex2] = minHeap.elements[nodeIndex2],
        minHeap.elements[nodeIndex1]
}
