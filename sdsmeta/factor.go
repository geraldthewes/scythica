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

// Factor are one based for R

type factor struct {
	factors      []string
	inverseIndex map[string]int
}

func (f *factor) init() *factor {
	f.factors = make([]string, 0)
	f.inverseIndex = make(map[string]int)
	return f
}

func (f *factor) get(i int) (val string) {
	return f.factors[i-1]
}

func (f *factor) lookup(s string) (index int) {
	return f.inverseIndex[s]
}

func (f *factor) encode(s string) (index int) {
	i, ok := f.inverseIndex[s]
	if !ok {
		f.factors = append(f.factors, s)
		i = len(f.factors)
		f.inverseIndex[s] = i
	}
	return i
}
