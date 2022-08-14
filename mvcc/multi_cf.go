package mvcc

import (
	"bytes"

	"github.com/blacktear23/bolt"
)

const (
	MAX_UINT64 uint64 = 0xFFFFFFFFFFFFFFFF

	OP_SET byte = 1
	OP_DEL byte = 2
)

var (
	cfKeys      = []byte("cf:mvcc:keys")
	cfValues    = []byte("cf:mvcc:vals")
	cfKeysValue = []byte("Y")

	_ MvccDB     = (*TwoCFMvcc)(nil)
	_ MvccCursor = (*twoCFMvccCursor)(nil)
)

type TwoCFMvcc struct {
	txn *bolt.Tx
}

func NewTwoCFMvcc(txn *bolt.Tx) *TwoCFMvcc {
	return &TwoCFMvcc{
		txn: txn,
	}
}

func (m *TwoCFMvcc) Get(ver uint64, key []byte) ([]byte, error) {
	kb := m.txn.Bucket(cfKeys)
	if kb == nil {
		// No bucket means no key exists just return
		return nil, ErrKeyNotFound
	}
	kVal := kb.Get(key)
	if kVal == nil {
		// Key not exists means no key
		return nil, ErrKeyNotFound
	}
	return m.readVersion(ver, key)
}

func (m *TwoCFMvcc) Set(ver uint64, key []byte, value []byte) error {
	kb, vb, err := m.ensureBuckets()
	if err != nil {
		return err
	}
	err = kb.Put(key, cfKeysValue)
	if err != nil {
		return err
	}
	ekey := encodeMvccKey(ver, key)
	eval := encodeMvccValue(OP_SET, value)
	return vb.Put(ekey, eval)
}

func (m *TwoCFMvcc) Delete(ver uint64, key []byte) error {
	kb, vb, err := m.ensureBuckets()
	if err != nil {
		return err
	}
	cv := kb.Get(key)
	if cv == nil {
		// Not found no need to update value cf
		return nil
	}
	ekey := encodeMvccKey(ver, key)
	eval := encodeMvccValue(OP_DEL, nil)
	return vb.Put(ekey, eval)
}

func (m *TwoCFMvcc) Cursor(ver uint64) MvccCursor {
	return &twoCFMvccCursor{
		ver:    ver,
		txn:    m.txn,
		finish: false,
	}
}

func (m *TwoCFMvcc) readVersion(ver uint64, key []byte) ([]byte, error) {
	vb := m.txn.Bucket(cfValues)
	if vb == nil {
		// No bucket means no values exists just return nil
		return nil, ErrKeyNotFound
	}
	c := vb.Cursor()
	verKey := encodeMvccKey(ver, key)
	ek, ev := c.Seek(verKey)
	if !bytes.HasPrefix(ek, key) {
		// Not found
		return nil, ErrKeyNotFound
	}
	dver := decodeMvccKeyVersion(ek)
	if dver > 0 && ver >= dver {
		dop, dv := decodeMvccValue(ev)
		if dop == OP_DEL {
			return nil, ErrKeyNotFound
		} else {
			return dv, nil
		}
	}
	// Seek next
	for {
		ek, ev = c.Next()
		if !bytes.HasPrefix(ek, key) {
			// Not Found
			break
		}
		dver := decodeMvccKeyVersion(ek)
		if dver > 0 && ver >= dver {
			dop, dv := decodeMvccValue(ev)
			if dop == OP_DEL {
				return nil, ErrKeyNotFound
			} else {
				return dv, nil
			}
		}
	}
	return nil, ErrKeyNotFound
}

func (m *TwoCFMvcc) ensureBuckets() (kb *bolt.Bucket, vb *bolt.Bucket, err error) {
	kb, err = m.txn.CreateBucketIfNotExists(cfKeys)
	if err != nil {
		return nil, nil, err
	}
	vb, err = m.txn.CreateBucketIfNotExists(cfValues)
	if err != nil {
		return nil, nil, err
	}
	return kb, vb, err
}

type twoCFMvccCursor struct {
	ver    uint64
	txn    *bolt.Tx
	kbc    *bolt.Cursor
	vbc    *bolt.Cursor
	finish bool
}

func (c *twoCFMvccCursor) Seek(start []byte) ([]byte, []byte) {
	c.finish = false
	kb := c.txn.Bucket(cfKeys)
	if kb == nil {
		// No keys cf found set finish and return
		c.finish = true
		return nil, nil
	}
	vb := c.txn.Bucket(cfValues)
	if vb == nil {
		// No values cf found set finish annd return
		c.finish = true
		return nil, nil
	}

	c.kbc = kb.Cursor()
	c.vbc = vb.Cursor()

	kkey, _ := c.kbc.Seek(start)
	if kkey == nil {
		// No keys found set finish and return
		c.finish = true
		return nil, nil
	}

	mval, err := c.readValue(kkey)
	if err == nil {
		// Found one return
		// err not nil means not found values or current value is deleted
		return kkey, mval
	}
	for {
		kkey, _ = c.kbc.Next()
		if kkey == nil {
			c.finish = true
			return nil, nil
		}
		mval, err := c.readValue(kkey)
		if err == nil {
			// Means found value, just return
			return kkey, mval
		}
	}
}

func (c *twoCFMvccCursor) Next() ([]byte, []byte) {
	if c.finish {
		return nil, nil
	}
	if c.kbc == nil || c.vbc == nil {
		// No cursors means not seek just return nils
		return nil, nil
	}
	kkey, _ := c.kbc.Next()
	if kkey == nil {
		// No keys found set finish and return
		c.finish = true
		return nil, nil
	}
	mval, err := c.readValue(kkey)
	if err == nil {
		// Means found value, just return
		return kkey, mval
	}
	// Should iterate next
	for {
		kkey, _ = c.kbc.Next()
		if kkey == nil {
			// Not found keys return nils
			c.finish = true
			return nil, nil
		}
		mval, err := c.readValue(kkey)
		if err == nil {
			// Means found value, just return
			return kkey, mval
		}
	}
}

func (c *twoCFMvccCursor) readValue(key []byte) ([]byte, error) {
	verKey := encodeMvccKey(c.ver, key)
	ek, ev := c.vbc.Seek(verKey)
	if !bytes.HasPrefix(ek, key) {
		return nil, ErrKeyNotFound
	}
	dver := decodeMvccKeyVersion(ek)
	if dver > 0 && c.ver >= dver {
		dop, dv := decodeMvccValue(ev)
		if dop == OP_DEL {
			return nil, ErrKeyNotFound
		} else {
			return dv, nil
		}
	}
	// Seek next
	for {
		ek, ev = c.vbc.Next()
		if !bytes.HasPrefix(ek, key) {
			// Not found
			break
		}
		dver := decodeMvccKeyVersion(ek)
		if dver > 0 && c.ver >= dver {
			dop, dv := decodeMvccValue(ev)
			if dop == OP_DEL {
				return nil, ErrKeyNotFound
			} else {
				return dv, nil
			}
		}
	}
	return nil, ErrKeyNotFound
}
