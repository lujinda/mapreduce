package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func GetRole() string {
	return os.Args[1]
}

func mapF(document, value string) (res []mapreduce.KeyValue) {
	f := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	time.Sleep(10 * time.Second)
	words := strings.FieldsFunc(value, f)
	for _, word := range words {
		kv := mapreduce.KeyValue{
			Key:   word,
			Value: " ",
		}
		res = append(res, kv)
	}
	return
}

func reduceF(key string, values []string) string {
	s := strconv.Itoa(len(values))
	return s
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("%s: see usage comments in file\n", os.Args[0])
	} else if GetRole() == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
