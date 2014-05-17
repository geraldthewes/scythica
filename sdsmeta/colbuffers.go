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

// Single Buffer for a Partition of a Column
type SDataFrameColBuffer struct {
	Column           Sdscolumndef
	Path             string
	PartitionKey     string
	IsNA             string
	DataBufferInt32  []int32
	DataBufferFloat  []float32
	DataBufferDouble []float64
	DataBufferInt64  []int64
	DataBufferByte   []byte
	DataBufferFactor []int32
	DataBufferBool   []bool
}

// Create new Column Partition Buffer
func NewColPartitionBuffer(sdf *SDataFrame, col Sdscolumndef, pKey string) (colBuffer SDataFrameColBuffer) {
	colBuffer.Column = col
	colBuffer.PartitionKey = pKey
	colBuffer.Path = sdf.PartitionPath(pKey)
	colBuffer.IsNA = sdf.Schema.Keyspace.IsNA
	nrows := sdf.Schema.Keyspace.Rows
	colBuffer.allocateBuffer(nrows)
	return
}

func (pCols *SDataFrameColBuffer) String() (s string) {
	s = fmt.Sprintf("Column: %s (%s) Attributes: %s. Partition %s:%s",
		pCols.Column.Colname,
		pCols.Column.Coltype,
		pCols.Column.Attributes,
		pCols.PartitionKey,
		pCols.Path)
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

func (col *SDataFrameColBuffer) setCol(row int, value string) (err error) {

	err = nil

	if value == col.IsNA {
		// For now just ignore
		return nil
	}

	switch SDF_ColType_Keywords[col.Column.Coltype] {
	case SDFK_Integer32:
		var i int64
		i, err = strconv.ParseInt(value, 10, 32)
		col.DataBufferInt32[row] = int32(i)
	case SDFK_Factor:
		// Not implemented
	case SDFK_Float:
		var f float64
		f, err = strconv.ParseFloat(value, 32)
		col.DataBufferFloat[row] = float32(f)
	case SDFK_Double:
		col.DataBufferDouble[row], err = strconv.ParseFloat(value, 64)
	case SDFK_Date:
		//
	case SDFK_Integer64:
		col.DataBufferInt64[row], err = strconv.ParseInt(value, 10, 64)
	case SDFK_Character:
		//
	case SDFK_Boolean:
		//
	default:
		panic(fmt.Sprintf("Unknown column type %s for row %d value %s of %s\n",
			col.Column.Coltype,
			row,
			value,
			col))
	}

	if err != nil {
		var serr SError
		serr.msg = fmt.Sprintf("%s: error: %s",
			col.String(),
			err)
		err = &serr
	}

	return
}

func (colBuffer *SDataFrameColBuffer) FlushToDisk(rows int) (err error) {
	err = nil

	var fo *os.File
	fname := colBuffer.Path + DF_SEP + colBuffer.Column.Colname
	//fmt.Printf("write ... %s\n", fname)
	fo, err = os.Create(fname)
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

	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Integer32:
		buff := colBuffer.DataBufferInt32[0:rows]
		binary.Write(out, binary.LittleEndian, buff)
	case SDFK_Factor:
		//
	case SDFK_Float:
		//colBuffer.DataBufferFloat = make([]float32, nrows)
	case SDFK_Double:
		//colBuffer.DataBufferDouble = make([]float64, nrows)
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
