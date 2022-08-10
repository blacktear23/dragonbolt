package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/server"
	"github.com/blacktear23/dragonbolt/tso"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/goutils/syncutil"
)

var (
	// initial nodes count is fixed to three, their addresses are also fixed
	addresses = []string{
		"localhost:63001",
		"localhost:63002",
		"localhost:63003",
	}
)

const (
	exampleShardID uint64 = 128
	tsoShardID     uint64 = 0xFFFFFFFFFFFFFF01
)

type RequestType uint64

const (
	PUT RequestType = iota
	GET
)

func init() {
	// config.Soft.NodeReloadMillisecond = 200
}

func parseCommand(msg string) (RequestType, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(msg), " ")
	if len(parts) == 0 || (parts[0] != "put" && parts[0] != "get") {
		return PUT, "", "", false
	}
	if parts[0] == "put" {
		if len(parts) != 3 {
			return PUT, "", "", false
		}
		return PUT, parts[1], parts[2], true
	}
	if len(parts) != 2 {
		return GET, "", "", false
	}
	return GET, parts[1], "", true
}

func printUsage() {
	fmt.Fprintf(os.Stdout, "Usage - \n")
	fmt.Fprintf(os.Stdout, "put key value\n")
	fmt.Fprintf(os.Stdout, "get key\n")
}

func main() {
	var (
		replicaID int
		addr      string
		join      bool
		dbFile    string
		redisAddr string
		walDir    string
		rtt       int
	)
	flag.IntVar(&replicaID, "replica-id", 1, "Replica ID to use")
	flag.IntVar(&rtt, "rtt", 10, "RTT")
	flag.StringVar(&addr, "addr", "", "Nodehost address")
	flag.StringVar(&walDir, "wal-dir", "helloworld", "WAL directory")
	flag.StringVar(&dbFile, "db-file", "", "Database file name")
	flag.BoolVar(&join, "join", false, "Joining a new node")
	flag.StringVar(&redisAddr, "redis-addr", "", "Redis Server listen address")
	flag.Parse()
	if dbFile == "" {
		fmt.Println("Require -db-file parameter")
		return
	}

	db, err := bolt.Open(dbFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	initMembers := make(map[uint64]string)
	if !join {
		for idx, v := range addresses {
			initMembers[uint64(idx+1)] = v
		}
	}
	var nodeAddr string
	if len(addr) != 0 {
		nodeAddr = addr
	} else {
		nodeAddr = initMembers[uint64(replicaID)]
	}
	log.Printf("Node address: %s", nodeAddr)
	logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	rc := config.Config{
		ReplicaID:          uint64(replicaID),
		ShardID:            exampleShardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    0,
		CompactionOverhead: 16,
	}
	datadir := filepath.Join(
		"/tmp",
		walDir,
		fmt.Sprintf("node-%d", replicaID),
	)
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: uint64(rtt),
		RaftAddress:    nodeAddr,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		log.Fatal(err)
	}
	/*
		builder := kv.MemKVBuilder{}
	*/
	builder := kv.DiskKVBuilder{
		DB: db,
	}
	if err := nh.StartOnDiskReplica(initMembers, join, builder.Build, rc); err != nil {
		log.Fatal(err)
	}
	raftStopper := syncutil.NewStopper()
	ch := make(chan string, 16)

	raftStopper.RunWorker(func() {
		cs := nh.GetNoOPSession(exampleShardID)
		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				msg := strings.Replace(v, "\n", "", 1)
				// input message must be in the following formats -
				// put key value
				// get key
				rt, key, val, ok := parseCommand(msg)
				if !ok {
					fmt.Fprintf(os.Stderr, "invalid input\n")
					printUsage()
					os.Stdout.Write([]byte("> "))
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				if rt == PUT {
					muts := []kv.Mutation{
						kv.Mutation{
							Op:    kv.PUT,
							Key:   []byte(key),
							Value: []byte(val),
						},
					}
					data, err := json.Marshal(muts)
					if err != nil {
						panic(err)
					}
					_, err = nh.SyncPropose(ctx, cs, data)
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
					} else {
						os.Stdout.Write([]byte("> "))
					}
				} else {
					result, err := nh.SyncRead(ctx, exampleShardID, []byte(key))
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
					} else {
						fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", key, result)
					}
					os.Stdout.Write([]byte("> "))
				}
				cancel()
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})

	tsoServer, err := tso.NewTSOServer(nh, tsoShardID, uint64(replicaID), dbFile, initMembers)
	if err != nil {
		log.Println("Start TSO Server error", err)
	}

	var rs *server.RedisServer = nil
	if redisAddr != "" {
		rs := server.NewRedisServer(redisAddr, nh, exampleShardID, tsoServer)
		rs.Run()
		log.Println("Start Redis Server for", redisAddr)
	}

	consoleStopper := syncutil.NewStopper()
	consoleStopper.RunWorker(func() {
		var (
			fp *os.File = nil
		)
		reader := bufio.NewReader(os.Stdin)
		os.Stdout.Write([]byte("> "))
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if strings.TrimSpace(s) == "" {
				os.Stdout.WriteString("> ")
			} else if s == "stat\n" {
				leaderID, term, valid, err := nh.GetLeaderID(exampleShardID)
				os.Stdout.WriteString(fmt.Sprintf("Node ID: %v\n", rc.ReplicaID))
				os.Stdout.WriteString(fmt.Sprintf("Leader ID: %v term %v valid %v err %v\n", leaderID, term, valid, err))
				os.Stdout.WriteString("> ")

			} else if s == "profile\n" {
				if fp == nil {
					fp, err = os.OpenFile("./dragonbolt.pprof", os.O_RDWR|os.O_CREATE, 0644)
					if err == nil {
						err = pprof.StartCPUProfile(fp)
						if err != nil {
							fp.Close()
							fp = nil
							os.Stdout.WriteString("Cannot start porfile\n> ")
						} else {
							os.Stdout.WriteString("Profile Started\n> ")
						}
					} else {
						os.Stdout.WriteString("Cannot open file\n> ")
					}
				} else {
					os.Stdout.WriteString("Already Profiling\n> ")
				}
			} else if s == "endprofile\n" {
				if fp == nil {
					os.Stdout.WriteString("Not start profile\n> ")
				} else {
					pprof.StopCPUProfile()
					fp.Close()
					fp = nil
					os.Stdout.WriteString("Profile Ended\n> ")
				}
			} else if s == "exit\n" {
				raftStopper.Stop()
				nh.Close()
				if rs != nil {
					rs.Close()
				}
				return
			} else {
				ch <- s
			}
		}
	})
	printUsage()
	raftStopper.Wait()
}
