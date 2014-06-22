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
)

// Single Buffer for a Partition of a Column
type factorColumnBuffer struct {
	Column           Sdscolumndef
	factors          *factor
	path             string
	partitionKey     string
	isNA             string
	rowsPerSplit     int32
	dataBufferInt32  []int32
}

// Create new Vector Column Partition Buffer
func NewFactorColumnBuffer(sdf *SDataFrame, col Sdscolumndef, pKey string, factors *factor) (colBuffer *factorColumnBuffer) {
	colBuffer = new(factorColumnBuffer)
	colBuffer.Column = col
	colBuffer.factors = factors
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
func (colBuffer *factorColumnBuffer) String() (s string) {
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
func (colBuffer *factorColumnBuffer) allocateBufferSplit(nrows int32) {


	if SDF_ColType_Keywords[colBuffer.Column.Coltype] != SDFK_Factor {
		panic(fmt.Sprintf("Allocating a FactorColumnBuffer but column is not a factor (%d) %s", SDF_ColType_Keywords[colBuffer.Column.Coltype], colBuffer.String()))
	}

	colBuffer.dataBufferInt32 = make([]int32, nrows)
	return
}

// Set Value for row in column. row is offset in split
func (colBuffer *factorColumnBuffer) setCol(row int32, value string) (err error) {

	//fmt.Printf("setCol: row=%d value=%s Col=%s\n", row, value, colBuffer.String())

	err = nil

	if SDF_ColType_Keywords[colBuffer.Column.Coltype] != SDFK_Factor {
		panic(fmt.Sprintf("Unknown column type %s for row %d value %s of %s\n",
			colBuffer.Column.Coltype,
			row,
			value,
			colBuffer))
	}

	if value == colBuffer.isNA {
		// For now just ignore
		return nil
	}

	//fmt.Printf(colBuffer.factors.String())
	i := colBuffer.factors.encode(value)
	colBuffer.dataBufferInt32[row] = int32(i)


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
func (colBuffer *factorColumnBuffer) flushToDisk(rows int32, split int32) (err error) {
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

	buff := colBuffer.dataBufferInt32[0:nrows]
	binary.Write(out, binary.LittleEndian, buff)

	return err
}
