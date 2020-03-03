package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

const (
	insertSortLimitLen = 20
)

// RoundArgs contains arguments used in a map-reduce round.
type RoundArgs struct {
	MapFunc    MapF
	ReduceFunc ReduceF
	NReduce    int
}

// RoundsArgs represents arguments used in multiple map-reduce rounds.
type RoundsArgs []RoundArgs

type UrlCount struct {
	url string
	cnt int
}

func InsertionSort(ucs []*UrlCount) {
	var tmp *UrlCount
	var j int
	for i := 1; i < len(ucs); i++ {
	    tmp = ucs[i]
		for j = i; j > 0 && less(ucs[j - 1], tmp) ; j-- {
			ucs[j] = ucs[j - 1]
		}
		ucs[j] = tmp
	}
}

func SelectTopNByHash(urlCntMap map[string]int, n int) ([]string, []int) {
	urlsLen := len(urlCntMap)
	ucs := make([]*UrlCount, 0, urlsLen)
	for k, v := range urlCntMap {
		ucs = append(ucs, &UrlCount{k, v})
	}
	sortedUcs := make([]*UrlCount, 0, n)
	if urlsLen <= insertSortLimitLen && insertSortLimitLen >= n {
		InsertionSort(ucs)
		if urlsLen > n {
			sortedUcs = ucs[: n]
		} else {
			sortedUcs = ucs
		}
	} else {
		valueUrlsMap := make(map[int][]*UrlCount, urlsLen / 2)
		// hash处理
		for _, uc := range ucs {
			valueUrlsMap[uc.cnt] = append(valueUrlsMap[uc.cnt], uc)
		}
		// 排序url的cnt
		keys := make([]int, 0, n)
		for key, _ := range valueUrlsMap {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] > keys[j]
		})
		// 计数排序
		count := 0
		for _, key := range keys {
			if count >= n {
				break
			}
			if len(valueUrlsMap[key]) > n {
				sort.Slice(valueUrlsMap[key], func(i, j int) bool {
					return !less(valueUrlsMap[key][i], valueUrlsMap[key][j])
				})
			} else {
				InsertionSort(valueUrlsMap[key])
			}
			sortedUcs = append(sortedUcs, valueUrlsMap[key]...)
			count += len(valueUrlsMap[key])
		}
		sortedUcs = sortedUcs[: n]
	}
	
	urls := make([]string, 0, n)
	cnts := make([]int, 0, n)
	for _, uc := range sortedUcs {
		urls = append(urls, uc.url)
		cnts = append(cnts, uc.cnt)
	}
	
	return urls, cnts
}

func SelectTopNByMinHeap(urlCntMap map[string]int, n int) ([]string, []int)  {
	urlsLen := len(urlCntMap)
	ucs := make([]*UrlCount, 0, urlsLen)
	for k, v := range urlCntMap {
		ucs = append(ucs, &UrlCount{k, v})
	}
	var minHeap *MinHeap
	if urlsLen > insertSortLimitLen && insertSortLimitLen > n {
		minHeap = NewMinHeap(n, ucs[: n]...)
		for left, right := n, urlsLen - 1; left < right; left, right = left + 1, right - 1 {
			if !less(ucs[left], minHeap.GetRoot()) {
				minHeap.ReplaceRoot(ucs[left])
			}
			if !less(ucs[right], minHeap.GetRoot()) {
				minHeap.ReplaceRoot(ucs[right])
			}
		}
		ucs = minHeap.GetReverseElements()
	} else {
	    InsertionSort(ucs)
	    if urlsLen > n {
	    	ucs = ucs[: n]
	    }
	}
	
	urls := make([]string, 0, n)
	cnts := make([]int, 0, n)
	for _, uc := range ucs {
		urls = append(urls, uc.url)
		cnts = append(cnts, uc.cnt)
	}
	
	return urls, cnts
}

// TopN returns topN urls in the urlCntMap.
func TopN(urlCntMap map[string]int, n int) ([]string, []int) {
	ucs := make([]*UrlCount, 0, len(urlCntMap))
	for k, v := range urlCntMap {
		ucs = append(ucs, &UrlCount{k, v})
	}
	sort.Slice(ucs, func(i, j int) bool {
		if ucs[i].cnt == ucs[j].cnt {
			return ucs[i].url < ucs[j].url
		}
		return ucs[i].cnt > ucs[j].cnt
	})
	urls := make([]string, 0, n)
	cnts := make([]int, 0, n)
	for i, u := range ucs {
		if i == n {
			break
		}
		urls = append(urls, u.url)
		cnts = append(cnts, u.cnt)
	}
	return urls, cnts
}

// CheckFile checks if these two files are same.
func CheckFile(expected, got string) (string, bool) {
	c1, err := ioutil.ReadFile(expected)
	if err != nil {
		panic(err)
	}
	c2, err := ioutil.ReadFile(got)
	if err != nil {
		panic(err)
	}
	s1 := strings.TrimSpace(string(c1))
	s2 := strings.TrimSpace(string(c2))
	if s1 == s2 {
		return "", true
	}

	errMsg := fmt.Sprintf("expected:\n%s\n, but got:\n%s\n", c1, c2)
	return errMsg, false
}

// CreateFileAndBuf opens or creates a specific file for writing.
func CreateFileAndBuf(fpath string) (*os.File, *bufio.Writer) {
	dir := path.Dir(fpath)
	os.MkdirAll(dir, 0777)
	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	return f, bufio.NewWriterSize(f, 1<<20)
}

// OpenFileAndBuf opens a specific file for reading.
func OpenFileAndBuf(fpath string) (*os.File, *bufio.Reader) {
	f, err := os.OpenFile(fpath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	return f, bufio.NewReader(f)
}

// WriteToBuf write strs to this buffer.
func WriteToBuf(buf *bufio.Writer, strs ...string) {
	for _, str := range strs {
		if _, err := buf.WriteString(str); err != nil {
			panic(err)
		}
	}
}

// SafeClose flushes this buffer and closes this file.
func SafeClose(f *os.File, buf *bufio.Writer) {
	if buf != nil {
		if err := buf.Flush(); err != nil {
			panic(err)
		}
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
}

// FileOrDirExist tests if this file or dir exist in a simple way.
func FileOrDirExist(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func PanicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func BytesToString(byteList []byte) string {
	return *(*string)(unsafe.Pointer(&byteList))
}

func kvSliceSortByKey(kvs []KeyValue) {
	sort.Slice(kvs, func(i, j int) bool {
	    iValue, err := strconv.Atoi(kvs[i].Value)
	    PanicErr(err)
		jValue, err := strconv.Atoi(kvs[j].Value)
		PanicErr(err)
		
		return iValue > jValue
	})
}

func kvSliceSortByValue(kvs []KeyValue) {
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key > kvs[j].Key
	})
}

func selectTop2(kvs []KeyValue, MaxValueStr string) string {
    top2Value := -1
    maxValue, err := strconv.Atoi(MaxValueStr)
    PanicErr(err)
	for _, kv := range kvs {
		value, err := strconv.Atoi(kv.Value)
		PanicErr(err)
		if value < maxValue && top2Value < value {
			top2Value = value
		}
	}
	
	return strconv.Itoa(top2Value)
}

func less(value1, value2 *UrlCount) bool {
	if value1.cnt == value2.cnt {
		return value1.url > value2.url
	}
	return value1.cnt < value2.cnt
}
