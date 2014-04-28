package sdsmeta

import (
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"encoding/binary"
)

type leveldbstore struct {
	dbh *leveldb.DB
}

func openLevelDBStore(path string) (ldbs leveldbstore, err error) {
	err = nil

	fname := path + DF_SEP + DF_PDB

	ldbs.dbh, err = leveldb.Open(fname, nil)
	return
}

func (ldbs leveldbstore) close() (err error) {
	err = ldbs.dbh.Close()
	return
}

func (ldbs leveldbstore) put(key string, value int64) (err error) {
	b := make([]byte, 8)
	binary.PutVarint(b, int64(value))

	opts := db.WriteOptions{Sync: true}
	err = ldbs.dbh.Set([]byte(key), b, &opts)
	return
}
