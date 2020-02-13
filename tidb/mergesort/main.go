package main

import (
    "flag"
    "log"
    "os"
    "pingcap/talentplan/tidb/mergesort/sort"
    "runtime/pprof"
)

var (
    cpuProfile = flag.Bool("cpuProf", false, "")
    memoryProfile = flag.Bool("memProf", false, "")
    cpuProfPath = "./profile_data/cpuProf.prof"
    memProfPath = "./profile_data/memProf.prof"
    
    srcLength = 16 << 20
    src = make([]int64, srcLength)
)

func init() {
    sort.Prepare(src)
}

func main() {
    flag.Parse()
    // 满足条件则启动CPU监控
    if *cpuProfile {
        cpuFile, err := os.Create(cpuProfPath)
        if err != nil {
            log.Fatal("Create cpu profile file failed, filepath:", cpuProfPath)
        }
        if err := pprof.StartCPUProfile(cpuFile); err != nil {
            log.Fatal("Start CPU profile failed")
        }
        defer pprof.StopCPUProfile()
        // defer cpuFile.Close()
    }
    
    // 执行被监测的函数
    sort.MergeSort(src)
    
    if *memoryProfile {
        memoryFile, err := os.Create(memProfPath)
        if err != nil {
            log.Fatal("Create memory profile file failed, filepath:", memProfPath)
        }
        // runtime.GC()
        if err := pprof.WriteHeapProfile(memoryFile); err != nil {
            log.Fatal("Start memory profile failed")
        }
        defer memoryFile.Close()
    }
}
