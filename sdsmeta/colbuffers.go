package sdsmeta

import (
	//"code.google.com/p/leveldb-go/leveldb"
	//"code.google.com/p/leveldb-go/leveldb/db"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

const HEADER_PAD_BYTES = 128 // Used for R SEXPREC_ALIGN pad

// $$$ Since now using inheritance can avoid the ugly union below

// Single Buffer for a Partition of a Column
type SDataFrameColBuffer struct {
	Column           Sdscolumndef
	Path             string
	PartitionKey     string
	IsNA             string
	rowsPerSplit     int32
	DataBufferInt32  []int32
	DataBufferFloat  []float32
	DataBufferDouble []float64
	DataBufferInt64  []int64
	DataBufferByte   []byte
	DataBufferBool   []bool
}

// Create new Column Partition Buffer
func NewColBuffer(sdf *SDataFrame, col Sdscolumndef, pKey string) (colBuffer *SDataFrameColBuffer) {
	colBuffer = new(SDataFrameColBuffer)
	colBuffer.Column = col
	colBuffer.PartitionKey = pKey
	colBuffer.Path = sdf.PartitionPath(pKey)
	colBuffer.IsNA = sdf.Schema.Keyspace.IsNA
	nrows := sdf.Schema.Keyspace.Rows_per_split
	colBuffer.rowsPerSplit = nrows
	colBuffer.allocateBuffer(nrows)
	//fmt.Printf("Create SDataFrameColBuffer: %s\n", colBuffer.String())
	return
}

// String representation of column buffer
func (colBuffer *SDataFrameColBuffer) String() (s string) {
	s = fmt.Sprintf("Column: %s (%s) Attributes: %s. Partition %s:%s rowsPerSplit:%d",
		colBuffer.Column.Colname,
		colBuffer.Column.Coltype,
		colBuffer.Column.Attributes,
		colBuffer.PartitionKey,
		colBuffer.Path,
		colBuffer.rowsPerSplit)
	return
}

func (colBuffer *SDataFrameColBuffer) allocateBuffer(nrows int32) {
	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Integer32:
		fallthrough
	case SDFK_Factor:
		colBuffer.DataBufferInt32 = make([]int32, nrows)
	case SDFK_Float:
		colBuffer.DataBufferFloat = make([]float32, nrows)
	case SDFK_Double:
		colBuffer.DataBufferDouble = make([]float64, nrows)
	case SDFK_Date:
		fallthrough
	case SDFK_Integer64:
		colBuffer.DataBufferInt64 = make([]int64, nrows)
	case SDFK_Character:
		colBuffer.DataBufferByte = make([]byte, nrows)
	case SDFK_Boolean:
		colBuffer.DataBufferBool = make([]bool, nrows)
	default:
		panic(fmt.Sprintf("Unknown column type %s\n",
			colBuffer.String()))
	}
	return
}

// Set Value in buffer. row is offset in split
func (colBuffer *SDataFrameColBuffer) setCol(row int32, value string) (err error) {

	//fmt.Printf("setCol: row=%d value=%s Col=%s\n", row, value, colBuffer.String())

	err = nil

	if value == colBuffer.IsNA {
		// For now just ignore
		return nil
	}

	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Integer32:
		var i int64
		i, err = strconv.ParseInt(value, 10, 32)
		colBuffer.DataBufferInt32[row] = int32(i)
	case SDFK_Factor:
		// Not implemented
	case SDFK_Float:
		var f float64
		f, err = strconv.ParseFloat(value, 32)
		colBuffer.DataBufferFloat[row] = float32(f)
	case SDFK_Double:
		colBuffer.DataBufferDouble[row], err = strconv.ParseFloat(value, 64)
	case SDFK_Date:
		//
	case SDFK_Integer64:
		colBuffer.DataBufferInt64[row], err = strconv.ParseInt(value, 10, 64)
	case SDFK_Character:
		//
	case SDFK_Boolean:
		//
	default:
		panic(fmt.Sprintf("Unknown column type %s for row %d value %s of %s\n",
			colBuffer.Column.Coltype,
			row,
			value,
			colBuffer))
	}

	if err != nil {
		var serr SError
		serr.msg = fmt.Sprintf("%s: error: %s",
			colBuffer.String(),
			err)
		err = &serr
	}

	return
}

// Flush current column to disk. Pass in number of rows to write and split count
func (colBuffer *SDataFrameColBuffer) flushToDisk(rows int32, split int32) (err error) {
	err = nil

	var fo *os.File
	fname := fmt.Sprintf("%s-%08x.dat", colBuffer.Column.Colname, split)
	fpath := colBuffer.Path + DF_SEP + fname
	//fmt.Printf("write ... %s\n", fname)
	fo, err = os.Create(fpath)
	if err != nil {
		return err
	}
	defer func() {
		if err = fo.Close(); err != nil {
			panic(err)
		}
	}()
	var out io.Writer
	out = io.Writer(fo)

	// Write out header for R
	var hdrpad [HEADER_PAD_BYTES]byte
	var n int
	n, err = out.Write(hdrpad[:])
	if n != HEADER_PAD_BYTES {
		var serr SError
		serr.msg = "Error while writing out Pad bytes"
		err = &serr
		return err
	}
	if err != nil {
		return err
	}

	// +1 because we increment after the Append
	nrows := rows%colBuffer.rowsPerSplit + 1

	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Integer32:
		buff := colBuffer.DataBufferInt32[0:nrows]
		binary.Write(out, binary.LittleEndian, buff)
	case SDFK_Factor:
		//
	case SDFK_Float:
		//colBuffer.DataBufferFloat = make([]float32, nrows)
	case SDFK_Double:
		//colBuffer.DataBufferDouble = make([]float64, nrows)
		buff := colBuffer.DataBufferDouble[0:nrows]
		binary.Write(out, binary.LittleEndian, buff)
	case SDFK_Date:
		fallthrough
	case SDFK_Integer64:
		//colBuffer.DataBufferInt64 = make([]int64, nrows)
	case SDFK_Character:
		//colBuffer.DataBufferByte = make([]byte, nrows)
	case SDFK_Boolean:
		//colBuffer.DataBufferBool = make([]bool, nrows)
	default:
		panic(fmt.Sprintf("Unknown column type %s\n",
			colBuffer.String()))
	}

	return err
}
