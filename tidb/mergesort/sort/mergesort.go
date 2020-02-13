package sort

import (
    "math"
    "pingcap/talentplan/tidb/mergesort/heap"
    "runtime"
    "sort"
    "sync"
)

var (
    // 子序列的最小长度，若分组所得的子序列的最小长度小于该值则原序列无需使用归并排序，直接使用内置的排序算法排序即可
    // 该值的设置仅是为了筛掉长度较小的序列，没有经过特别的论证
    arrayMinLength = int(math.Pow(2, float64(runtime.NumCPU())))
)

// MergeSort：使用多路归并排序给出的无序序列
// param src: 待排序的无序序列
// return: nil
func MergeSort(src []int64) {
    // 获取由src分割排序所得的有序子数组
    sortedSubArrays := GetSortedSubArrays(src)
    
    // 若待归并子序列长度小于等于1，则无序执行归并，直接返回即可
    if len(sortedSubArrays) <= 1 {
        return
    }
    
    // 执行多路归并
    dst := make([]int64, len(src))
    Merge(dst, sortedSubArrays)
    
    // 将结果复制到src中
    copy(src, dst)
}

// Merge: 归并多个有序序列，并将归并结果放入指定的切片中
// param dst: 用于存放归并后的结果序列
// param sortedArrays: 存放多个有序子序列的二维切片
// return: nil
func Merge(dst []int64, sortedArrays [][]int64) {
    k := len(sortedArrays)
    
    // 记录存在有效待处理元素的sortedArrays中的子数组的下标及子数组中的待处理的元素的下标
    arrayIdValidIndexMap := make(map[int]int)
    // 存储sortedArrays中的子数组的长度，用于校验arrayIdValidIndexMap中的子数组中的下标的有效性
    arrayLengthListInArrays := make([]int, k, k)
    // 构建一个容量为k的最小堆
    minHeap := heap.NewMinHeap(k)
    // 初始化堆结点，向堆中添加每个子序列的首元素
    for index := 0; index < k; index++ {
        arrayLength := len(sortedArrays[index])
        minHeap.AppendNode(sortedArrays[index][0], index)
        arrayIdValidIndexMap[index] = 1
        // 记录每一个序列的长度
        arrayLengthListInArrays[index] = arrayLength
    }
    
    // 合并多个子序列
    dstLength := k * len(sortedArrays[0])
    for index := 0; index < dstLength; {
        // 若所有序列中的元素已处理或在堆中，则将堆中剩余的节点按照顺序放入finalArray中后退出循环，完成所有元素排序
        if len(arrayIdValidIndexMap) == 0 && !minHeap.IsEmpty() {
            for !minHeap.IsEmpty() {
                dst[index] = minHeap.PopRoot()
                index++
            }
            break
        }
        // 取出堆顶节点及其对应的序列id，将其插入finalArray中
        arrayIdOfNextValidIndex := minHeap.GetRootNodeArrayId()
        rootNodeElement := minHeap.PopRoot()
        dst[index] = rootNodeElement
        index++
        // 获取下一个待插入堆中的元素所在的序列
        if _, ok := arrayIdValidIndexMap[arrayIdOfNextValidIndex]; !ok {
            for arrayIndex := range arrayIdValidIndexMap {
                arrayIdOfNextValidIndex = arrayIndex
                break
            }
        }
        // 补充堆节点
        minHeap.AppendNode(
            sortedArrays[arrayIdOfNextValidIndex][arrayIdValidIndexMap[arrayIdOfNextValidIndex]],
            arrayIdOfNextValidIndex)
        // 修改对应列表中的处理元素的下标并校验下标是否有效，若无效则从map中删除该序列
        arrayIdValidIndexMap[arrayIdOfNextValidIndex]++
        // 在对arrayIdValidIndexMap中的下标做了自增后，需要校验其有效性，若无效则需从map中删除
        if arrayIdValidIndexMap[arrayIdOfNextValidIndex] >= arrayLengthListInArrays[arrayIdOfNextValidIndex] {
            delete(arrayIdValidIndexMap, arrayIdOfNextValidIndex)
        }
    }
}

// GetSortedSubArrays: 通过给出的无序序列，将其分为若干段子序列，通过排序得出有序的子序列组并返回
// param array: 待处理的无序子序列
// return: 返回以有序子序列为元素的二维切片
func GetSortedSubArrays(array []int64) [][]int64 {
    arrayLength := len(array)
    
    // 以cpu核数作为子数组的个数
    subArrayAmount := runtime.NumCPU()
    // 获取每个子数组的最小长度(最后一个子数组需要将余下的数连接在一起)
    subArrayLength := arrayLength / subArrayAmount
    // 规定：若子数组的最小长度subArrayLength在[0, arrayMinLength]范围内时，无需分组并发排序，直接排序并返回结果即可
    if subArrayLength <= arrayMinLength {
        InMemorySort(array, nil)
        return [][]int64{array}
    }
    // 存储子数组
    subArrays := make([][]int64, subArrayAmount, subArrayAmount)
    // 设置等待组
    wg := new(sync.WaitGroup)
    wg.Add(subArrayAmount)
    
    // 并行排序各个子序列
    subLeftIndex, subRightIndex := 0, subArrayLength
    for index := 0; index < subArrayAmount; index++ {
        if subRightIndex + subArrayLength > arrayLength {
            subRightIndex = arrayLength
        }
        subArray := array[subLeftIndex: subRightIndex]
        subArrays[index] = subArray
        go InMemorySort(subArray, wg)
        subLeftIndex, subRightIndex = subRightIndex, subRightIndex + subArrayLength
    }
    wg.Wait()
    
    return subArrays
}

// InMemorySort: 使用原地排序对切片元素进行排序
// param array: 待排序的子序列
// param wg: 等待组实例，当排序完成时执行Done()操作
// return: nil
func InMemorySort(array []int64, wg *sync.WaitGroup) {
    if wg != nil {
        defer wg.Done()
    }
    sort.Slice(array, func(i, j int) bool {
        return array[i] < array[j]
    })
}
