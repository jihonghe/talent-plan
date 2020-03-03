package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 generates RoundsArgs for getting the 10 most frequent URLs.
// There are two rounds in this approach.
// The first round will do url count.
// The second will sort results generated in the first round and
// get the 10 most frequent URLs.
func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs = append(kvs, KeyValue{Key: l})
	}
	return kvs
}

// URLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	return key + " " + strconv.Itoa(len(values)) + "\n"
}

// URLTop10Map is the map function in the second round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	cnts := getUrlCountMap(lines)
	
	us, cs := SelectTopNByHash(cnts, 10)
	kvs := make([]KeyValue, 0, len(us))
	for i, u := range us {
		kvs = append(kvs, KeyValue{Value: u + " " + strconv.Itoa(cs[i])})
	}
	return kvs
}

// URLTop10Reduce is the reduce function in the second round
func URLTop10Reduce(key string, values []string) string {
	cnts := getUrlCountMap(values)
	us, cs := SelectTopNByHash(cnts, 10)
	
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}

func getUrlCountMap(values []string) map[string]int {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		PanicErr(err)
		cnts[tmp[0]] = n
	}
	
	return cnts
}
