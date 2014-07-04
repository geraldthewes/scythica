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
	"encoding/binary"
	"fmt"
	"io"
	"os"
"time"
)


// Single Buffer for a Partition of a Column
// Dates are represented as an interger representing the number of days 
// since 1970-01-01
type DateColumnBuffer struct {
	Column           Sdscolumndef
	path             string
	partitionKey     string
	isNA             string
	rowsPerSplit     int32
	dataBufferInt32  []int32
}

// Create new Date Column Partition Buffer
func NewDateColumnBuffer(sdf *SDataFrame, col Sdscolumndef, pKey string) (colBuffer *DateColumnBuffer) {
	colBuffer = new(DateColumnBuffer)
	colBuffer.Column = col
	colBuffer.partitionKey = pKey
	colBuffer.path = sdf.PartitionPath(pKey)
	colBuffer.isNA = sdf.Schema.Keyspace.IsNA
	nrows := sdf.Schema.Keyspace.Rows_per_split
	colBuffer.rowsPerSplit = nrows
	colBuffer.allocateBufferSplit(nrows)
	//fmt.Printf("Create DateColumnBuffer: %s\n", colBuffer.String())
	return
}

// String representation of column buffer
func (colBuffer *DateColumnBuffer) String() (s string) {
	s = fmt.Sprintf("Date Column: %s (%s) Attributes: %s. Partition %s:%s rowsPerSplit:%d",
		colBuffer.Column.Colname,
		colBuffer.Column.Coltype,
		colBuffer.Column.Attributes,
		colBuffer.partitionKey,
		colBuffer.path,
		colBuffer.rowsPerSplit)
	return
}

// Allocate storage for the buffer, based on max split size
func (colBuffer *DateColumnBuffer) allocateBufferSplit(nrows int32) {
	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Date:
		colBuffer.dataBufferInt32 = make([]int32, nrows)
	default:
		panic(fmt.Sprintf("Unknown column type %s\n",
			colBuffer.String()))
	}
	return
}

// Set Value for row in column. row is offset in split
func (colBuffer *DateColumnBuffer) setCol(row int32, value string) (err error) {

	//fmt.Printf("setCol: row=%d value=%s Col=%s\n", row, value, colBuffer.String())

	err = nil

	if value == colBuffer.isNA {
		// For now just ignore
		return nil
	}

	switch SDF_ColType_Keywords[colBuffer.Column.Coltype] {
	case SDFK_Date:
		var days int32
		dt, err := time.Parse("2006-01-02", value)
		if err != nil {
			return err
		}
		secs := dt.Unix()
		days = int32(secs/86400)

		colBuffer.dataBufferInt32[row] = days
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
func (colBuffer *DateColumnBuffer) flushToDisk(rows int32, split int32) (err error) {
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
	case SDFK_Date:
		buff := colBuffer.dataBufferInt32[0:nrows]
		binary.Write(out, binary.LittleEndian, buff)
	default:
		panic(fmt.Sprintf("Unknown column type %s\n",
			colBuffer.String()))
	}

	return err
}
