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
	Rows       int                   // Current count of rows
	Chunk      int                   // Current chunk count
	path       string                // Partition path
	colBuffers []SDataFrameColBuffer // List of column buffers
}

// Create a new Partition
func CreatePartitionCols(sdf *SDataFrame, pkey string) (buffers SDataFramePartitionCols, err error) {
	buffers.Sdf = sdf
	buffers.Rows = 0
	buffers.Chunk = 0
	buffers.path = sdf.PartitionPath(pkey)
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

func (pCols *SDataFramePartitionCols) setRow(row int, record []string) (err error) {
	//fmt.Printf("Set row ... %d\n", row)
	for i := 0; i < len(pCols.colBuffers); i++ {
		err = pCols.colBuffers[i].setCol(row, record[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Write current in memory content to disk
func (pCols *SDataFramePartitionCols) FlushToDisk() (err error) {
	err = nil

	if pCols.Rows == 0 {
		return
	}

	for _, colBuffer := range pCols.colBuffers {
		err = colBuffer.FlushToDisk(pCols.Rows)
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
