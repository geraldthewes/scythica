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
	"github.com/ugorji/go/codec"
	"io"
	"os"
	"fmt"
)

// Factor are one based for R

type factor struct {
	factors      []string
	inverseIndex map[string]int
}

// Initialize factor
func (f *factor) init() *factor {
	f.factors = make([]string, 0)
	f.inverseIndex = make(map[string]int)
	return f
}

// String representation
func (f *factor) String() string {
	return fmt.Sprintf("Factor with %d levels\n",len(f.factors))
}

// Get factor of level i
func (f *factor) get(i int) (val string) {
	return f.factors[i-1]
}

// Lookup level value of factor
func (f *factor) lookup(s string) (index int) {
	return f.inverseIndex[s]
}

// Encode a factor value, return index
func (f *factor) encode(s string) (index int) {
	i, ok := f.inverseIndex[s]
	if !ok {
		f.factors = append(f.factors, s)
		i = len(f.factors)
		f.inverseIndex[s] = i
	}
	return i
}

// Number of levels in factor
func (f *factor) length() int {
	return len(f.factors)
}

// Store factors to file
func (f *factor) save(path string) (err error) {
	err = nil
	var fo *os.File
	fo, err = os.Create(path)
	if err != nil {
		return
	}

	defer func() {
		if err = fo.Close(); err != nil {
			panic(err)
		}
	}()

	var w io.Writer
	w = io.Writer(fo)
	var mh codec.MsgpackHandle
	enc := codec.NewEncoder(w, &mh)

	err = enc.Encode(f.factors)

	return
}

// Load factors from file
func (f *factor) load(path string) (err error) {
	err = nil
	var fo *os.File
	fo, err = os.Open(path)
	if err != nil {
		return
	}

	defer func() {
		if err = fo.Close(); err != nil {
			panic(err)
		}
	}()

	var r io.Reader
	r = io.Reader(fo)

	var mh codec.MsgpackHandle
	dec := codec.NewDecoder(r, &mh)
	err = dec.Decode(&f.factors)

	for i, val := range f.factors {
		f.inverseIndex[val] = i + 1
	}

	return
}
