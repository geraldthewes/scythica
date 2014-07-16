/*
This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.
*/

package sdsmeta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

const HEADER_PAD_BYTES = 128 // Used for R SEXPREC_ALIGN pad

// $$$ Since now using inheritance can avoid the ugly union below

// Single Buffer for a Partition of a Column
type VectorColumnBuffer struct {
	Column          Sdscolumndef
	path            string
	partitionKey    string
	isNA            string
	rowsPerSplit    int32
	dataBufferInt32 []int32
	//	dataBufferFloat  []float32
	dataBufferDouble []float64
	//	dataBufferInt64  []int64
	//	dataBufferByte   []byte
	//	dataBufferBool   []bool
}

// Create new Vector Column Partition Buffer
func NewVectorColumnBuffer(sdf *SDataFrame, col Sdscolumndef, pKey string) (colBuffer *VectorColumnBuffer) {
	colBuffer = new(VectorColumnBuffer)
	colBuffer.Column = col
	colBuffer.partitionKey = pKey
	colBuffer.path = sdf.PartitionPath(pKey)
	colBuffer.isNA = sdf.Schema.Keyspace.IsNA
	nrows := sdf.Schema.Keyspace.Rows_per_split
	colBuffer.rowsPerSplit = nrows
	colBuffer.allocateBufferSplit(nrows)
	//fmt.Printf("Create VectorColumnBuffer: %s\n", colBuffer.String())
	return
}

// String representation of column buffer
func (colBuffer *VectorColumnBuffer) String() (s string) {
	s = fmt.Sprintf("Column: %s (%s) Attributes: %s. Partition %s:%s rowsPerSplit:%d",
		colBuffer.Column.Colname,
		colBuffer.Column.Coltype,
		colBuffer.Column.Attributes,
		colBuffer.partitionKey,
		colBuffer.path,
		colBuffer.rowsPerSplit)
	return
}

// Allocate storage for the buffer, based on max split size
func (colBuffer *VectorColumnBuffer) allocateBufferSplit(nrows int32) {
	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Integer32:
		fallthrough
	case SDFK_Factor:
		colBuffer.dataBufferInt32 = make([]int32, nrows)
	case SDFK_Float:
		//colBuffer.dataBufferFloat = make([]float32, nrows)
	case SDFK_Double:
		colBuffer.dataBufferDouble = make([]float64, nrows)
	case SDFK_Integer64:
		//colBuffer.dataBufferInt64 = make([]int64, nrows)
	case SDFK_Character:
		//colBuffer.dataBufferByte = make([]byte, nrows)
	case SDFK_Logical:
		colBuffer.dataBufferInt32 = make([]int32, nrows)
	default:
		panic(fmt.Sprintf("Unknown column type %s\n",
			colBuffer.String()))
	}
	return
}

// Set Value for row in column. row is offset in split
func (colBuffer *VectorColumnBuffer) setCol(row int32, value string) (err error) {

	//fmt.Printf("setCol: row=%d value=%s Col=%s\n", row, value, colBuffer.String())

	err = nil

	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Integer32:
		if value == colBuffer.isNA {
			colBuffer.dataBufferInt32[row] = math.MinInt32
		} else {
			var i int64
			i, err = strconv.ParseInt(value, 10, 32)
			colBuffer.dataBufferInt32[row] = int32(i)
		}
	case SDFK_Factor:
		// Not implemented
	case SDFK_Float:
		//var f float64
		//f, err = strconv.ParseFloat(value, 32)
		//colBuffer.dataBufferFloat[row] = float32(f)
	case SDFK_Double:
		if value == colBuffer.isNA {
			// R's double NA - See arithmetic.c
			//var w int32[2]
			//w[0] = 0x7ff00000
			//w[1] = 1954

			var rnan []byte = []byte{0x00, 0x00, 0x07, 0xa2, 0x00, 0x00, 0xf0, 0x7f}
			r := bytes.NewReader(rnan)
			var dnan float64
			binary.Read(r, binary.LittleEndian, &dnan)
			colBuffer.dataBufferDouble[row] = dnan
		} else {
			colBuffer.dataBufferDouble[row], err = strconv.ParseFloat(value, 64)
		}
	case SDFK_Integer64:
		//colBuffer.dataBufferInt64[row], err = strconv.ParseInt(value, 10, 64)
	case SDFK_Character:
		//
	case SDFK_Logical:
		// False is f, False, 0
		var v int32
		v = 1
		if value == colBuffer.isNA {
			v = math.MinInt32
		} else if strings.EqualFold(value, "f") {
			v = 0
		} else if strings.EqualFold(value, "false") {
			v = 0
		} else if value == "0" {
			v = 0
		}
		colBuffer.dataBufferInt32[row] = v
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
func (colBuffer *VectorColumnBuffer) flushToDisk(rows int32, split int32) (err error) {
	err = nil

	var fo *os.File
	fname := fmt.Sprintf("%s-%08x.dat", colBuffer.Column.Colname, split)
	fpath := colBuffer.path + DF_SEP + fname
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
		buff := colBuffer.dataBufferInt32[0:nrows]
		binary.Write(out, binary.LittleEndian, buff)
	case SDFK_Factor:
		//
	case SDFK_Float:
		//colBuffer.DataBufferFloat = make([]float32, nrows)
	case SDFK_Double:
		//colBuffer.DataBufferDouble = make([]float64, nrows)
		buff := colBuffer.dataBufferDouble[0:nrows]
		binary.Write(out, binary.LittleEndian, buff)
	case SDFK_Date:
		fallthrough
	case SDFK_Integer64:
		//colBuffer.DataBufferInt64 = make([]int64, nrows)
	case SDFK_Character:
		//colBuffer.DataBufferByte = make([]byte, nrows)
	case SDFK_Logical:
		buff := colBuffer.dataBufferInt32[0:nrows]
		binary.Write(out, binary.LittleEndian, buff)
	default:
		panic(fmt.Sprintf("Unknown column type %s\n",
			colBuffer.String()))
	}

	return err
}
