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

// Represents a null column. Used either for unimplemented types of columns
// to be ignored
type nullColumnBuffer struct {
	Column       Sdscolumndef
}

// Create new Column Partition Buffer
func NewNullColumnBuffer(sdf *SDataFrame, col Sdscolumndef) (colBuffer *nullColumnBuffer) {
	colBuffer = new(nullColumnBuffer)
	colBuffer.Column = col
	//fmt.Printf("Create keyColumnBuffer: %s\n", colBuffer.String())
	return
}

// String representation of column buffer
func (colBuffer *nullColumnBuffer) String() (s string) {
	s = fmt.Sprintf("Partition Column: %s (%s) Attributes: %s. Partition %s:%s",
		colBuffer.Column.Colname,
		colBuffer.Column.Coltype,
		colBuffer.Column.Attributes)

	return
}

// Set Value in buffer. row is offset in split
func (colBuffer *nullColumnBuffer) setCol(row int32, value string) (err error) {
	return nil
}

// Flush current column to disk. Pass in number of rows to write and split count
func (colBuffer *nullColumnBuffer) flushToDisk(rows int32, split int32) (err error) {
	return nil
}
