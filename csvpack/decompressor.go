/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package csvpack

import "encoding/csv"
import "github.com/maxymania/stunning-guacamole/generic"

type Reader struct{
	reader generic.SeqReader
	data   [][]string
	chain  []generic.Transform
}
func NewReader(r generic.SeqReader,chain  []generic.Transform) *Reader {
	return &Reader{reader:r,chain:chain}
}
func (r *Reader) fill() error {
	if len(r.data)>0 { return nil }
	buf,err := r.reader.ReadBuffer()
	if err!=nil { return err }
	for _,t := range r.chain {
		nb := t(buf)
		generic.Free(buf)
		buf = nb
	}
	r.data = deserialize(buf)
	generic.Free(buf)
	return nil
}
func (r *Reader) ReadChunk() ([][]string,error) {
	if len(r.data)==0 { if err := r.fill() ; err!=nil { return nil,err } }
	
	d := r.data
	r.data = nil
	return d,nil
}
func (r *Reader) Read() ([]string,error) {
	if len(r.data)==0 { if err := r.fill() ; err!=nil { return nil,err } }
	
	ret := r.data[0]
	r.data = r.data[1:]
	return ret,nil
}
func (r *Reader) ReadTo(w *csv.Writer) (err error) {
	for {
		if len(r.data)==0 { if err = r.fill() ; err!=nil { return } }
		
		for _,rec := range r.data {
			err = w.Write(rec)
			if err!=nil { return }
		}
		r.data = nil
	}
}

// deserialize

