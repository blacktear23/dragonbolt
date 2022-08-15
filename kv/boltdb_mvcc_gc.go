package kv

import (
	"log"
	"time"

	"github.com/blacktear23/bolt"
	"github.com/blacktear23/dragonbolt/mvcc"
)

func (d *DiskKV) processMvccGC(ver uint64) error {
	if !d.gcRunning {
		d.gcRunning = true
		go d.runMvccGC(ver)
	}
	return nil
}

func (d *DiskKV) runMvccGC(ver uint64) {
	defer func() {
		d.gcRunning = false
	}()
	var gcInfos []*mvcc.GCInfo = nil
	err := d.db.View(func(txn *bolt.Tx) error {
		mvccTxn := mvcc.NewTwoCFMvcc(txn)
		gcInfos = mvccTxn.GcScan(ver)
		return nil
	})
	if err != nil {
		log.Println("[GC] Scan got error:", err)
		return
	}
	if len(gcInfos) == 0 {
		return
	}
	log.Printf("[DEBUG] GC Infos: %+v", gcInfos)
	err = d.db.Update(func(txn *bolt.Tx) error {
		mvccTxn := mvcc.NewTwoCFMvcc(txn)
		return mvccTxn.GcPurge(gcInfos)
	})
	if err != nil {
		log.Println("[GC] Purge got error:", err)
	}
}

func (d *DiskKV) StartGC() {
	for {
		tsoVer, err := getTso()
		if err != nil {
			log.Println("[GC] Got error at get tso", err)
			time.Sleep(1 * time.Second)
			continue
		}
		dur := getGCDuration()
		time.Sleep(dur)
		log.Printf("GC For %v", dur)
		d.runMvccGC(tsoVer)
	}
}
