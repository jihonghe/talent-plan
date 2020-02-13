package sort

import (
    "fmt"
    "sort"
    "testing"
)

var (
    lens = []int{1, 3, 5, 7, 11, 13, 17, 19, 23, 29, 1024, 1 << 13, 1 << 17, 1 << 19, 1 << 20}
)

func TestMergeSort(t *testing.T) {
    for _, srcLen := range lens {
        t.Run(fmt.Sprintf("[srcLength=%d]", srcLen), testMergeSortFunc(srcLen))
    }
}

func TestInMemorySort(t *testing.T) {
    for _, srcLen := range lens {
        t.Run(fmt.Sprintf("[srcLength=%d]", srcLen), testInMemorySortFunc(srcLen))
    }
}

func TestGetSortedSubArrays(t *testing.T) {
    for _, srcLen := range lens {
        t.Run(fmt.Sprintf("[srcLength=%d]", srcLen), testGetSortedSubArraysFunc(srcLen))
    }
}

func TestMerge(t *testing.T) {
    for _, srcLen := range lens {
        t.Run(fmt.Sprintf("[srcLength=%d]", srcLen), testMergeFunc(srcLen))
    }
}

func testMergeFunc(srcLength int) func(t *testing.T) {
    return func(t *testing.T) {
        src := make([]int64, srcLength)
        dst := make([]int64, srcLength)
        expect := make([]int64, srcLength)
        Prepare(src)
        copy(expect, src)
    
        sort.Slice(expect, func(i, j int) bool { return expect[i] < expect[j] })
        subSortedArrays := GetSortedSubArrays(src)
        // 子数组数目小于等于1时，直接用src与expect比较即可
        if len(subSortedArrays) <= 1 {
            for index := 0; index < srcLength; index++ {
                if src[index] != expect[index] {
                    t.Error("Merge() failed")
                }
            }
            return
        }
        
        Merge(dst, subSortedArrays)
    
        for index := 0; index < srcLength; index++ {
            if dst[index] != expect[index] {
                t.Error("Merge() failed")
            }
        }
    }
}

func testInMemorySortFunc(srcLength int) func(t *testing.T) {
    return func(t *testing.T) {
        src := make([]int64, srcLength)
        expect := make([]int64, srcLength)
        Prepare(src)
        copy(expect, src)
    
        sort.Slice(expect, func(i, j int) bool { return expect[i] < expect[j] })
        InMemorySort(src, nil)
    
        for index := 0; index < srcLength; index++ {
            if src[index] != expect[index] {
                t.Error("InMemorySort() failed")
            }
        }
    }
}

func testGetSortedSubArraysFunc(srcLength int) func(t *testing.T) {
    return func(t *testing.T) {
        src := make([]int64, srcLength)
        expect := make([]int64, srcLength)
        Prepare(src)
        copy(expect, src)
        
        subSortedArrays := GetSortedSubArrays(src)
    
        leftIndex, rightIndex := 0, 0
        for _, sortedArray := range subSortedArrays {
            leftIndex, rightIndex = rightIndex, rightIndex + len(sortedArray)
            
            expectSubSortedArray := src[leftIndex: rightIndex]
            sort.Slice(expectSubSortedArray, func(i, j int) bool {
                return expectSubSortedArray[i] < expectSubSortedArray[j]
            })
            
            for index, value := range sortedArray {
                if value != expectSubSortedArray[index] {
                    t.Error("GetSortedSubArrays() failed")
                }
            }
        }
    }
}

func testMergeSortFunc(srcLength int) func(t *testing.T) {
    return func(t *testing.T) {
        src := make([]int64, srcLength)
        expect := make([]int64, srcLength)
    
        Prepare(src)
        copy(expect, src)
        sort.Slice(expect, func(i, j int) bool { return expect[i] < expect[j] })
        MergeSort(src)
    
        for i := 0; i < len(src); i++ {
            if src[i] != expect[i] {
                t.Errorf("MergeSort() failed: src[%d]=%d, expect[%d]=%d", i, src[i], i, expect[i])
            }
        }
    }
}
