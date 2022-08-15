package mvcc

import (
	"bytes"
	"errors"

	"github.com/blacktear23/bolt"
)

var (
	ErrBucketNotExists = errors.New("Bucket not exists")
)

type GCInfo struct {
	Key           []byte
	MvccKeys      [][]byte
	NeedDeleteKey bool
}

func (m *TwoCFMvcc) GcScan(ver uint64) []*GCInfo {
	kb := m.txn.Bucket(cfKeys)
	if kb == nil {
		return nil
	}
	vb := m.txn.Bucket(cfValues)
	if vb == nil {
		return nil
	}
	kc := kb.Cursor()
	vc := vb.Cursor()

	ret := []*GCInfo{}
	key, _ := kc.First()
	for {
		if key == nil {
			break
		}
		gcInfo := m.checkNeedGC(vc, key, ver)
		if gcInfo != nil {
			ret = append(ret, gcInfo)
		}
		key, _ = kc.Next()
	}
	return ret
}

func (m *TwoCFMvcc) GcPurge(infos []*GCInfo) error {
	if len(infos) == 0 {
		return nil
	}
	kb := m.txn.Bucket(cfKeys)
	if kb == nil {
		return ErrBucketNotExists
	}
	vb := m.txn.Bucket(cfValues)
	if vb == nil {
		return ErrBucketNotExists
	}
	vbc := vb.Cursor()

	for _, info := range infos {
		for _, vk := range info.MvccKeys {
			err := vb.Delete(vk)
			if err != nil {
				return err
			}
		}
		if info.NeedDeleteKey && !m.hasMvccValues(vbc, info.Key) {
			err := kb.Delete(info.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *TwoCFMvcc) hasMvccValues(c *bolt.Cursor, key []byte) bool {
	ek, _ := c.Seek(key)
	hasOne := false
	for {
		if !bytes.HasPrefix(ek, key) {
			break
		}
		dver := decodeMvccKeyVersion(ek)
		if dver > 0 {
			hasOne = true
			break
		}
		ek, _ = c.Next()
	}
	return hasOne
}

func (m *TwoCFMvcc) checkNeedGC(c *bolt.Cursor, key []byte, ver uint64) *GCInfo {
	var (
		mvccKeys         = [][]byte{}
		latestDel        = false
		latestKey []byte = nil
		numKeys          = 0
		needDel          = false
	)
	ek, ev := c.Seek(key)
	for {
		if !bytes.HasPrefix(ek, key) {
			break
		}
		dver := decodeMvccKeyVersion(ek)
		if dver > 0 {
			// This is valid key inc counter
			numKeys += 1
			if latestKey == nil {
				// Process first key just keep at least one item
				latestKey = ek
				if ev[0] == OP_DEL && dver < ver {
					latestDel = true
				}
				ek, ev = c.Next()
				continue
			}
			// If version less than GC version should be delete
			if dver < ver {
				mvccKeys = append(mvccKeys, ek)
			}
		}
		ek, ev = c.Next()
	}
	if latestDel && (numKeys-len(mvccKeys)) == 1 {
		// Only has one key and value is delete
		// Delete the mvcc bucket key and key bucket key
		mvccKeys = append(mvccKeys, latestKey)
		needDel = true
	}
	// Nothing to delete just return nil
	if len(mvccKeys) == 0 {
		return nil
	}
	return &GCInfo{
		Key:           key,
		MvccKeys:      mvccKeys,
		NeedDeleteKey: needDel,
	}
}
