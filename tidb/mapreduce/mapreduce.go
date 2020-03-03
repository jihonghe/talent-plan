package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// ReduceF function from MIT 6.824 LAB1
type ReduceF func(key string, values []string) string

// MapF function from MIT 6.824 LAB1
type MapF func(filename string, contents string) []KeyValue

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

var (
	kvSplitChar = "+"
)

type task struct {
	dataDir    string
	jobName    string
	mapFile    string   // only for map, the input file
	phase      jobPhase // are we in mapPhase or reducePhase?
	taskNumber int      // this task's index in the current phase
	nMap       int      // number of map tasks
	nReduce    int      // number of reduce tasks
	mapF       MapF     // map function used in this job
	reduceF    ReduceF  // reduce function used in this job
	wg         sync.WaitGroup
}

// MRCluster represents a map-reduce cluster.
type MRCluster struct {
	nWorkers int
	wg       sync.WaitGroup
	taskCh   chan *task
	exit     chan struct{}
}

var singleton = &MRCluster{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *task),
	exit:     make(chan struct{}),
}

func init() {
	singleton.Start()
}

// GetMRCluster returns a reference to a MRCluster.
func GetMRCluster() *MRCluster {
	return singleton
}

// NWorkers returns how many workers there are in this cluster.
func (c *MRCluster) NWorkers() int { return c.nWorkers }

// Start starts this cluster.
func (c *MRCluster) Start() {
	for i := 0; i < c.nWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *MRCluster) worker() {
	defer c.wg.Done()
	for {
		select {
		case t := <-c.taskCh:
			if t.phase == mapPhase {
				// 准备文件的读写对象
				fs := make([]*os.File, t.nReduce)
				bs := make([]*bufio.Writer, t.nReduce)
				for i := range fs {
					fs[i], bs[i] = CreateFileAndBuf(reduceName(t.dataDir, t.jobName, t.taskNumber, i))
				}
				// 从文件读取数据并执行mapF()，将mapF()的结果存储到对应的文件中
				content, err := ioutil.ReadFile(t.mapFile)
				PanicErr(err)
				results := t.mapF(t.mapFile, BytesToString(content))
				// 用map存储不同key设置唯一一个ihash()值，减少ihash()的调用
				bsIndexMap := make(map[string]int)
				for _, kv := range results {
					if _, ok := bsIndexMap[kv.Key]; !ok {
						bsIndexMap[kv.Key] = ihash(kv.Key) % t.nReduce
					}
					fmt.Fprintf(bs[bsIndexMap[kv.Key]], "%s\n", kv.Key+kvSplitChar+kv.Value)
				}
				// 关闭文件读写对象
				for i := range fs {
					SafeClose(fs[i], bs[i])
				}
			} else {
				mergeFileName := mergeName(t.dataDir, t.jobName, t.taskNumber)
				fs, bs := CreateFileAndBuf(mergeFileName)
				var kvMap = make(map[string][]string, t.nMap)
				// shuffle处理
				for index := 0; index < t.nMap; index++ {
					fileName := reduceName(t.dataDir, t.jobName, index, t.taskNumber)
					content, err := ioutil.ReadFile(fileName)
					PanicErr(err)
					bytesLines := bytes.Split(content, []byte("\n"))
					for _, bytesLine := range bytesLines {
						if len(bytesLine) == 0 || len(bytesLine) == len(kvSplitChar) {
							continue
						}
						kvSlice := strings.Split(BytesToString(bytesLine), kvSplitChar)
						if len(kvSlice) <= 1 {
							continue
						}
						kvMap[kvSlice[0]] = append(kvMap[kvSlice[0]], kvSlice[1])
					}
				}
				// 写入文件
				buffer := make([]string, 0, len(kvMap))
				for key, values := range kvMap {
					buffer = append(buffer, t.reduceF(key, values))
				}
				_, err := bs.WriteString(strings.Join(buffer, ""))
				PanicErr(err)
				SafeClose(fs, bs)
			}
			t.wg.Done()
		case <-c.exit:
			return
		}
	}
}

// Shutdown shutdowns this cluster.
func (c *MRCluster) Shutdown() {
	close(c.exit)
	c.wg.Wait()
}

// Submit submits a job to this cluster.
func (c *MRCluster) Submit(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int) <-chan []string {
	notify := make(chan []string)
	go c.run(jobName, dataDir, mapF, reduceF, mapFiles, nReduce, notify)
	return notify
}

func (c *MRCluster) run(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int, notify chan<- []string) {
	// map phase
	nMap := len(mapFiles)
	tasks := make([]*task, 0, nMap)
	for i := 0; i < nMap; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    mapFiles[i],
			phase:      mapPhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			mapF:       mapF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range tasks {
		t.wg.Wait()
	}
	
	// reduce phase
	tasks = make([]*task, 0, nReduce)
	for index := 0; index < nReduce; index++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			phase:      reducePhase,
			taskNumber: index,
			nReduce:    nReduce,
			nMap:       nMap,
			reduceF:    reduceF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() { c.taskCh <- t }()
	}
	notifies := make([]string, 0, nReduce)
	for _, t := range tasks {
		t.wg.Wait()
		mergedFileName := mergeName(t.dataDir, t.jobName, t.taskNumber)
		notifies = append(notifies, mergedFileName)
	}
	
	notify <- notifies
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func reduceName(dataDir, jobName string, mapTask int, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

func mergeName(dataDir, jobName string, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}
