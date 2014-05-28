package sdsmeta

import (
	//"code.google.com/p/leveldb-go/leveldb"
	//"code.google.com/p/leveldb-go/leveldb/db"
	//"encoding/binary"
	"fmt"
	//"io"
	"os"
	//"strconv"
)

// List of column buffers
type SDataFramePartitionCols struct {
	Sdf        *SDataFrame           // Pointer to Dataframe
	Rows       int32                 // Current count of rows
	Chunks     int32                 // Current chunk count
	Pkey       string                // Partition key
	path       string                // Partition path
	colBuffers []SDataFrameColBuffer // List of column buffers
}

// Inititalize new PartitionCols
func NewPartitionCols(sdf *SDataFrame) (buffers SDataFramePartitionCols) {
	buffers.Sdf = sdf
	buffers.Rows = 0
	buffers.Chunks = 0
	buffers.Pkey = "-nil-"
	return
}

// Create a new Partition
func CreatePartitionCols(sdf *SDataFrame, pkey string) (buffers SDataFramePartitionCols, err error) {
	err = nil
	buffers.Sdf = sdf
	buffers.Rows = 0
	buffers.Chunks = 0
	buffers.Pkey = pkey
	buffers.path = sdf.PartitionPath(pkey)

	_, err = os.Stat(buffers.path)
	if err == nil {
		var serr SError
		serr.msg = fmt.Sprintf("error: partition %s already exists", pkey)
		err = &serr
		return
	}

	err = os.Mkdir(buffers.path, 0774)
	if err != nil {
		return buffers, err
	}
	buffers.colBuffers = make([]SDataFrameColBuffer, len(sdf.Schema.Columns))

	for index, element := range sdf.Schema.Columns {
		buffers.colBuffers[index] = NewColBuffer(sdf, element, pkey)
	}

	return buffers, nil

}

func (pCols *SDataFramePartitionCols) AppendRow(row int32, record []string) (err error) {
	err = nil

	//fmt.Printf("Set row ... %d\n", row)
	for i := 0; i < len(pCols.colBuffers); i++ {
		nrow := row % pCols.Sdf.Schema.Keyspace.Rows_per_chunk
		err = pCols.colBuffers[i].setCol(nrow, record[i])
		if err != nil {
			return err
		}
	}
	nrows := pCols.Rows + 1
	if nrows%pCols.Sdf.Schema.Keyspace.Rows_per_chunk == 0 {
		err = pCols.FlushToDisk()
		pCols.Chunks++
	}
	pCols.Rows = nrows
	return
}

// Write current in memory content to disk
func (pCols *SDataFramePartitionCols) FlushToDisk() (err error) {
	err = nil

	if pCols.Rows == 0 {
		return
	}

	for _, colBuffer := range pCols.colBuffers {
		err = colBuffer.FlushToDisk(pCols.Rows, pCols.Chunks)
		if err != nil {
			return err
		}
	}

	return pCols.createPartitionDB()
}

func (pCols *SDataFramePartitionCols) createPartitionDB() (err error) {
	err = nil

	var dbh partitionStorer

	//dbh, err = openLevelDBStore(pCols.path)
	dbh, err = openMsgPackStore(pCols.path)

	//fname := pCols.path + DF_SEP + DF_PDB

	//var dbh *leveldb.DB
	//dbh, err = leveldb.Open(fname, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err = dbh.close(); err != nil {
			panic(err)
		}
	}()

	//nrows := make([]byte, 8)
	//binary.PutVarint(nrows, int64(pCols.Rows))
	dbh.put(DB_NROW, int64(pCols.Rows))
	//opts := db.WriteOptions{Sync: true}
	//err = dbh.Set([]byte(DB_NROW), nrows, &opts)

	return err
}

func (pCols *SDataFramePartitionCols) String() (s string) {
	s = fmt.Sprintf("List of columns of length %d", len(pCols.colBuffers))
	return
}
