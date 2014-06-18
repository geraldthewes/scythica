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
	"fmt"
)

// Represents a key column. By definition value is constant throughtout a partition
// so no need to store it on disk.
type keyColumnBuffer struct {
	Column       Sdscolumndef
	path         string
	partitionKey string
	key          string
}

// Create new Column Partition Buffer
func NewKeyColumnBuffer(sdf *SDataFrame, col Sdscolumndef, pKey string) (colBuffer *keyColumnBuffer) {
	colBuffer = new(keyColumnBuffer)
	colBuffer.Column = col
	colBuffer.partitionKey = pKey
	colBuffer.path = sdf.PartitionPath(pKey)
	colBuffer.key = ""
	//fmt.Printf("Create keyColumnBuffer: %s\n", colBuffer.String())
	return
}

// String representation of column buffer
func (colBuffer *keyColumnBuffer) String() (s string) {
	s = fmt.Sprintf("Partition Column: %s (%s) Attributes: %s. Partition %s:%s",
		colBuffer.Column.Colname,
		colBuffer.Column.Coltype,
		colBuffer.Column.Attributes,
		colBuffer.partitionKey,
		colBuffer.path)
	return
}

// Set Value in buffer. row is offset in split
func (colBuffer *keyColumnBuffer) setCol(row int32, value string) (err error) {
	if len(colBuffer.key) == 0 {
		colBuffer.key = value
	} else if colBuffer.key != value {
		panic(fmt.Sprintf("Key column value %s does not match partition value %s for key %s",
			value, colBuffer.key, colBuffer.partitionKey))
	}
	return nil
}

// Flush current column to disk. Pass in number of rows to write and split count
func (colBuffer *keyColumnBuffer) flushToDisk(rows int32, split int32) (err error) {
	// Nothing to write to disk
	return nil
}
