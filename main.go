package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/blacktear23/dragonbolt/kv"
	"github.com/blacktear23/dragonbolt/server"
	"github.com/blacktear23/dragonbolt/store"
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
	UNLOCK
	GC
	ADDNODE
	DELNODE
	NODES
)

func parseCommand(msg string) (RequestType, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(msg), " ")
	if len(parts) == 0 {
		return PUT, "", "", false
	}
	switch parts[0] {
	case "unlock":
		if len(parts) != 2 {
			return UNLOCK, "", "", false
		}
		return UNLOCK, parts[1], "", true
	case "put":
		if len(parts) != 3 {
			return PUT, "", "", false
		}
		return PUT, parts[1], parts[2], true
	case "get":
		if len(parts) != 2 {
			return GET, "", "", false
		}
		return GET, parts[1], "", true
	case "gc":
		if len(parts) != 2 {
			return GC, "", "", false
		}
		return GC, parts[1], "", true
	case "addnode":
		if len(parts) != 3 {
			return ADDNODE, "", "", false
		}
		return ADDNODE, parts[1], parts[2], true
	case "delnode":
		if len(parts) != 3 {
			return DELNODE, "", "", false
		}
		return DELNODE, parts[1], parts[2], true
	case "nodes":
		if len(parts) != 2 {
			return NODES, "", "", false
		}
		return NODES, parts[1], "", true
	}
	return PUT, "", "", false
}

func printUsage() {
	fmt.Fprintf(os.Stdout, "Usage - \n")
	fmt.Fprintf(os.Stdout, "put key value\n")
	fmt.Fprintf(os.Stdout, "get key\n")
	fmt.Fprintf(os.Stdout, "gc ver\n")
	fmt.Fprintf(os.Stdout, "unlock key\n")
	fmt.Fprintf(os.Stdout, "addnode target replicaID\n")
	fmt.Fprintf(os.Stdout, "delnode target replicaID\n")
	fmt.Fprintf(os.Stdout, "nodes shardID\n")
}

func startRestStores(tsoSrv *tso.TSOServer, sm *store.StoreManager, nh *dragonboat.NodeHost, initMembers map[uint64]string, replicaID uint64) {
	dbs, err := tsoSrv.ListDB()
	if err != nil {
		log.Println("List DB config got error:", err)
		return
	}
	for _, db := range dbs {
		stor, err := sm.CreateStore(db.ShardID)
		if err == nil {
			serr := stor.StartReplica(nh, initMembers, replicaID)
			if serr != nil {
				log.Println("Start Replica DB", db.Name, "Shard ID", db.ShardID, "got error:", serr)
			} else {
				log.Println("Start Replica DB", db.Name, "Shard ID", db.ShardID, "OK")
			}
		} else {
			log.Println("Create DB", db.Name, "Shard ID", db.ShardID, "got error:", err)
		}
	}
}

type SignalCallback func()

func WaitSignal(onReload, onExit SignalCallback) {
	var sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)
	for sig := range sigChan {
		if sig == syscall.SIGHUP {
			// Reload resolve rule file
			if onReload != nil {
				onReload()
			}
		} else {
			if onExit != nil {
				onExit()
			}
			log.Fatal("Server Exit\n")
		}
	}
}

func listAllShards(tsoSrv *tso.TSOServer) []uint64 {
	ret := []uint64{
		tsoShardID,
		exampleShardID,
	}
	infos, err := tsoSrv.ListDB()
	if err == nil {
		for _, info := range infos {
			ret = append(ret, info.ShardID)
		}
	}
	return ret
}

func main() {
	var (
		replicaID int
		addr      string
		join      bool
		dbDir     string
		redisAddr string
		walDir    string
		rtt       int
	)
	flag.IntVar(&replicaID, "replica-id", 1, "Replica ID to use")
	flag.IntVar(&rtt, "rtt", 100, "RTT")
	flag.StringVar(&addr, "addr", "", "Nodehost address")
	flag.StringVar(&walDir, "wal-dir", "/tmp/sample/wal", "WAL directory")
	flag.StringVar(&dbDir, "db-dir", "/tmp/sample/db", "Database file path")
	flag.BoolVar(&join, "join", false, "Joining a new node")
	flag.StringVar(&redisAddr, "redis-addr", "", "Redis Server listen address")
	flag.Parse()
	if dbDir == "" {
		fmt.Println("Require -db-dir parameter")
		return
	} else {
		err := os.MkdirAll(dbDir, 0755)
		if err != nil {
			log.Fatal("Cannot create db dir", err)
		}
	}
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
	datadir := filepath.Join(
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

	sm := store.NewStoreManager(dbDir, join)
	defer sm.Close()

	// Start TSO
	tsoServer, err := tso.NewTSOServer(nh, tsoShardID, uint64(replicaID), dbDir, initMembers, exampleShardID+1, sm, join)
	if err != nil {
		log.Fatal("Start TSO Server error", err)
	}
	// Update for kv tso service for mvcc gc use
	kv.SetTsoService(tsoServer)

	// Start default DB
	stor, err := sm.CreateStore(exampleShardID)
	if err != nil {
		log.Fatal(err)
	}
	if err := stor.StartReplica(nh, initMembers, uint64(replicaID)); err != nil {
		log.Fatal(err)
	}

	// Start Others DB
	startRestStores(tsoServer, sm, nh, initMembers, uint64(replicaID))

	var rs *server.RedisServer = nil
	if redisAddr != "" {
		rs = server.NewRedisServer(redisAddr, nh, exampleShardID, tsoServer, sm)
		rs.Run()
		log.Println("Start Redis Server for", redisAddr)
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
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
				} else if rt == UNLOCK {
					muts := []kv.Mutation{
						kv.Mutation{
							Op:  kv.MVCC_UNLOCK_FORCE,
							Key: []byte(key),
						},
					}
					data, err := json.Marshal(muts)
					if err != nil {
						panic(err)
					}
					ret, err := nh.SyncPropose(ctx, cs, data)
					if err != nil {
						fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
					} else {
						os.Stdout.WriteString(fmt.Sprintf("%+v\n", ret))
						os.Stdout.Write([]byte("> "))
					}
				} else if rt == GC {
					ver, err := strconv.ParseUint(key, 10, 64)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Invalid version %v\n", err)
					} else {
						muts := []kv.Mutation{
							kv.Mutation{
								Op:      kv.MVCC_GC,
								Version: ver,
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
					}
				} else if rt == ADDNODE {
					var (
						target           = key
						replicaID uint64 = 0
						err       error
					)
					replicaID, err = strconv.ParseUint(val, 10, 64)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Invalid replica ID %v\n", err)
						replicaID = 0
					}
					if replicaID != 0 {
						for _, shardID := range listAllShards(tsoServer) {
							lctx, lcancel := context.WithTimeout(context.Background(), 10*time.Second)
							err = nh.SyncRequestAddReplica(lctx, shardID, replicaID, target, 0)
							lcancel()
							if err != nil {
								fmt.Fprintf(os.Stderr, "Add Shard %d for Replica %d at %s returned error %v\n", shardID, replicaID, target, err)
							} else {
								os.Stdout.WriteString(fmt.Sprintf("Add Shard %d for Replica %d at %s OK\n", shardID, replicaID, target))
							}
						}
						os.Stdout.Write([]byte("> "))
					} else {
						os.Stdout.Write([]byte("> "))
					}
				} else if rt == DELNODE {
					var (
						target           = key
						replicaID uint64 = 0
						err       error
					)
					replicaID, err = strconv.ParseUint(val, 10, 64)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Invalid replica ID %v\n", err)
						replicaID = 0
					}
					if replicaID != 0 {
						for _, shardID := range listAllShards(tsoServer) {
							err = nh.StopReplica(shardID, replicaID)
							if err != nil {
								fmt.Fprintf(os.Stderr, "Stop Shard %d for Replica %d at %s returned error %v\n", shardID, replicaID, target, err)
							}
							lctx, lcancel := context.WithTimeout(context.Background(), 10*time.Second)
							err = nh.SyncRemoveData(lctx, shardID, replicaID)
							lcancel()
							if err != nil {
								fmt.Fprintf(os.Stderr, "Remove Shard %d data for Replica %d at %s returned error %v\n", shardID, replicaID, target, err)
							}
							lctx, lcancel = context.WithTimeout(context.Background(), 10*time.Second)
							err = nh.SyncRequestDeleteReplica(lctx, shardID, replicaID, 0)
							lcancel()
							if err != nil {
								fmt.Fprintf(os.Stderr, "Delete Shard %d for Replica %d at %s returned error %v\n", shardID, replicaID, target, err)
							} else {
								os.Stdout.WriteString(fmt.Sprintf("Delete Shard %d for Replica %d at %s OK\n", shardID, replicaID, target))
							}
						}
						os.Stdout.Write([]byte("> "))
					} else {
						os.Stdout.Write([]byte("> "))
					}
				} else if rt == NODES {
					var (
						shardID uint64 = 0
						err     error
					)
					if key == "tso" {
						shardID = tsoShardID
					} else {
						shardID, err = strconv.ParseUint(key, 10, 64)
						if err != nil {
							fmt.Fprintf(os.Stderr, "Invalid shard ID %v\n", err)
							shardID = 0
						}
					}
					if shardID != 0 {
						membs, err := nh.SyncGetShardMembership(ctx, shardID)
						if err != nil {
							fmt.Fprintf(os.Stderr, "Get members returned error %v\n", err)
						} else {
							os.Stdout.WriteString(fmt.Sprintf("Members: %+v\n", membs.Nodes))
						}
						os.Stdout.Write([]byte("> "))
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
				os.Stdout.WriteString(fmt.Sprintf("Node ID: %v\n", replicaID))
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
				if rs != nil {
					rs.Close()
				}
				raftStopper.Stop()
				nh.Close()
				return
			} else {
				ch <- s
			}
		}
	})
	printUsage()
	// raftStopper.Wait()
	WaitSignal(nil, func() {
		if rs != nil {
			rs.Close()
		}
		raftStopper.Stop()
		nh.Close()
	})
}
