package mapreduce

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"

	"github.com/pkg/errors"
)

type MapFileLocation struct {
	Address string
	File    string
}

// What follows are RPC types and methods.
// Field names must start with capital letters, otherwise RPC will break.

// DoTaskArgs holds the arguments that are passed to a worker when a job is
// scheduled on it.
type DoTaskArgs struct {
	JobName    string
	File       string   // the file to process
	Phase      jobPhase // are we in mapPhase or reducePhase?
	TaskNumber int      // this task's index in the current phase

	// NumOtherPhase is the total number of tasks in other phase; mappers
	// need this to compute the number of output bins, and reducers needs
	// this to know how many input files to collect.
	NumOtherPhase int

	// map结果中间文件分布机器映射表
	MapFileAddressByFile map[string]string
}

type TaskReply struct {
	Files []string // map任务的中间结果所在的文件路径需要推送给master
}

type FileArgs struct {
	File string
}

type FileReply struct {
	File    string
	Content []byte
}

// ShutdownReply is the response to a WorkerShutdown.
// It holds the number of tasks this worker has processed since it was started.
type ShutdownReply struct {
	Ntasks int
}

// RegisterArgs is the argument passed when a worker registers with the master.
type RegisterArgs struct {
	Worker string
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// call() returns true if the server responded, and false
// if call() was not able to contact the server. in particular,
// reply's contents are valid if and only if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in master.go, mapreduce.go,
// and worker.go.  please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	var c *rpc.Client
	var errx error
	if IsHost(srv) {
		c, errx = rpc.Dial("tcp", srv)

	} else {
		c, errx = rpc.Dial("unix", srv)
	}
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type FileFetcher struct {
}

// 注册为一个RPC方法. 让map worker来获取分片文件
func (f *FileFetcher) FetchFile(args *FileArgs, reply *FileReply) error {
	content, err := ioutil.ReadFile(args.File)
	if err != nil {
		return err
	}
	reply.Content = content
	reply.File = args.File
	return nil
}

// 从Master或Worker拉取文件
// @param role: 角色(Master, Worker)
// @param address: 节点地址
func (f *FileFetcher) pullFile(role, address, file string) error {
	reply := &FileReply{}
	if call(address, role+".FetchFile", &FileArgs{File: file}, reply) {
		log.Printf("pull %s from %s success", file, address)
		err := ioutil.WriteFile(file, reply.Content, 0600)
		if err != nil {
			return errors.Wrap(err, "WriteFile")
		}
		return nil

	} else {
		log.Printf("pull %s from %s error", file, address)
		return fmt.Errorf("failed to pull file from %s", address)
	}
}
