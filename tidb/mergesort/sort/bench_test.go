package sort

import "testing"

var (
    srcLength = 16 << 20
    src = make([]int64, srcLength)
    sortedArrays = make([][]int64, 0)
)

func init() {
    Prepare(src)
    sortedArrays = GetSortedSubArrays(src)
}

func BenchmarkMergeSort(b *testing.B) {
    MergeSort(src)
}

func BenchmarkGetSortedSubArrays(b *testing.B) {
    GetSortedSubArrays(src)
}

func BenchmarkMerge(b *testing.B) {
    dst := make([]int64, srcLength)
    Merge(dst, sortedArrays)
}
