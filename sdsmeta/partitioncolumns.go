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

type RowAppender interface {
	FlushToDisk() (err error)
	AppendRow(record []string) (err error)
	Rows() (nrows int32)
}

var nullAppender nullRowAppender

// List of column buffers
type SDataFramePartitionCols struct {
	Sdf        *SDataFrame      // Pointer to Dataframe
	rows       int32            // Current count of rows
	splits     int32            // Current split count
	pkey       string           // Partition key
	path       string           // Partition path
	colBuffers []columnBufferer // List of column buffers
}

// Inititalize new PartitionCols
func NewPartitionCols(sdf *SDataFrame) (buffers SDataFramePartitionCols) {
	buffers.Sdf = sdf
	buffers.rows = 0
	buffers.splits = 0
	buffers.pkey = "-nil-"
	return
}

// Create a new Partition
func (pCols *SDataFramePartitionCols) CreatePartitionCols(sdf *SDataFrame, pkey string, noappend bool) (appender RowAppender, err error) {
	err = nil
	path := sdf.PartitionPath(pkey)
	_, err = os.Stat(path)
	if err == nil {
		if noappend {
			return RowAppender(&nullAppender), nil
		}
		var serr SError
		serr.msg = fmt.Sprintf("error: partition %s already exists", pkey)
		err = &serr
		return RowAppender(&nullAppender), err
	}

	pCols.Sdf = sdf
	pCols.rows = 0
	pCols.splits = 0
	pCols.pkey = pkey
	pCols.path = path

	err = os.Mkdir(path, 0774)
	if err != nil {
		return RowAppender(&nullAppender), err
	}
	pCols.colBuffers = make([]columnBufferer, len(sdf.Schema.Columns))

	for index, element := range sdf.Schema.Columns {
		if element.isPartOfKey() {
			pCols.colBuffers[index] = NewKeyColumnBuffer(sdf, element, pkey)
		} else {
			switch SDF_ColType_Keywords[element.Coltype] {
			case SDFK_Integer32:
				pCols.colBuffers[index] = NewVectorColumnBuffer(sdf, element, pkey)
			case SDFK_Factor:
				pCols.colBuffers[index] = NewFactorColumnBuffer(sdf, element, pkey, &sdf.factors[index])
			case SDFK_Float:
				pCols.colBuffers[index] = NewNullColumnBuffer(sdf, element)
			case SDFK_Double:
				pCols.colBuffers[index] = NewVectorColumnBuffer(sdf, element, pkey)
			case SDFK_Date:
				pCols.colBuffers[index] = NewDateColumnBuffer(sdf, element, pkey)
			case SDFK_DateTime:
				pCols.colBuffers[index] = NewDateColumnBuffer(sdf, element, pkey)
			case SDFK_Integer64:
				pCols.colBuffers[index] = NewNullColumnBuffer(sdf, element)
			case SDFK_Character:
				pCols.colBuffers[index] = NewNullColumnBuffer(sdf, element)
			case SDFK_Logical:
				pCols.colBuffers[index] = NewVectorColumnBuffer(sdf, element, pkey)
			default:
				panic(fmt.Sprintf("Unknown column type %s\n",
					element.Colname))
			}

		}
	}

	return RowAppender(pCols), nil

}

func (pCols *SDataFramePartitionCols) AppendRow(record []string) (err error) {
	err = nil

	//fmt.Printf("Set row ... %d\n", row)
	for i := 0; i < len(pCols.colBuffers); i++ {
		nrow := pCols.rows % pCols.Sdf.Schema.Keyspace.Rows_per_split
		err = pCols.colBuffers[i].setCol(nrow, record[i])
		if err != nil {
			return err
		}
	}
	nrows := pCols.rows + 1
	if nrows%pCols.Sdf.Schema.Keyspace.Rows_per_split == 0 {
		err = pCols.FlushToDisk()
		pCols.splits++
	}
	pCols.rows = nrows
	return
}

// Write current in memory content to disk
func (pCols *SDataFramePartitionCols) FlushToDisk() (err error) {
	err = nil

	if pCols.rows == 0 {
		return
	}

	for _, colBuffer := range pCols.colBuffers {
		err = colBuffer.flushToDisk(pCols.rows, pCols.splits)
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
	dbh.put(DB_NROW, int64(pCols.rows))
	//opts := db.WriteOptions{Sync: true}
	//err = dbh.Set([]byte(DB_NROW), nrows, &opts)

	return err
}

func (pCols *SDataFramePartitionCols) Rows() (nrows int32) {
	return pCols.rows
}

func (pCols *SDataFramePartitionCols) String() (s string) {
	s = fmt.Sprintf("List of columns of length %d", len(pCols.colBuffers))
	return
}
