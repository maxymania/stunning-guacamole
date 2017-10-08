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

// This package is still experimental. Don't use.
package csvindex

import "github.com/maxymania/stunning-guacamole/generic"
import "github.com/maxymania/stunning-guacamole/csvpack"
import "github.com/vmihailenco/msgpack"
import "io"

type object struct{
	_msgpack struct{} `msgpack:",asArray"`
	Content string
	Column  int
	ChunkNo int64
	InChunk int
}



type key struct{
	Col  int
	Data string
}
type pos struct{
	ChunkNo int64
	InChunk int
}

func expand(p []pos) []pos {
	if (len(p)-cap(p))==0 {
		np := make([]pos,len(p),len(p)+128)
		copy(np,p)
		return np
	}
	return p
}

// This is a Non-Working Index. Don't use it.
type Index struct{
	elem map[key][]pos
}
func NewIndex() *Index { return &Index{make(map[key][]pos)} }
func (i *Index) Import(r io.Reader) (err error) {
	dec := msgpack.NewDecoder(r)
	obj := new(object)
	for {
		err = dec.Decode(obj)
		K := key{obj.Column,obj.Content}
		V := pos{obj.ChunkNo,obj.InChunk}
		i.elem[K] = append(expand(i.elem[K]),V)
	}
	if err==io.EOF { err = nil }
	return
	
}
func (i *Index) LookUp(col int,s string,r generic.RandomAccessReader, chain []generic.Transform) (str [][]string,err error) {
	var recs [][]string
	offset := int64(-1)
	for _,p := range i.elem[key{col,s}] {
		if offset!=p.ChunkNo {
			offset = p.ChunkNo
			buf,e := r.ReadBufferAt(offset)
			err = e
			if err!=nil { return }
			for _,t := range chain {
				nb := t(buf)
				generic.Free(buf)
				buf = nb
			}
			recs = csvpack.DecodeBuffer(buf)
			generic.Free(buf)
		}
		if len(recs) <= p.InChunk { continue }
		str = append(str,recs[p.InChunk])
	}
	return
}


/*
A Index-Writer. In fact, this is not really an Index. Instead, it is an extract from a Dataset (aka. CSV-Table).

The Index-Writer creates a MSGPACK-stream, that consist of arrays (that are used like tuples).

The format is [ColumnValue,ColumnNumber,ChunkOffset,RecordIndexInChunk].

	type object struct{
		_msgpack struct{} `msgpack:",asArray"`
		Content string
		Column  int
		ChunkNo int64
		InChunk int
	}
*/
type IndexWriter struct{
	Enc     *msgpack.Encoder
	Indeces []int
}
func NewIndexWriter(w io.Writer, i... int) *IndexWriter{
	return &IndexWriter{msgpack.NewEncoder(w),i}
}

func (iw *IndexWriter) AddRecord(rec []string,chunkNo int64,inChunk int) error {
	obj := &object{ChunkNo:chunkNo,InChunk:inChunk}
	for _,i := range iw.Indeces {
		if len(rec) <= i { continue }
		obj.Content = rec[i]
		obj.Column  = i
		err := iw.Enc.Encode(obj)
		if err!=nil { return err }
	}
	return nil
}

func (iw *IndexWriter) AddRecords(recs [][]string,chunkNo int64) error {
	obj := &object{ChunkNo:chunkNo}
	for inChunk,rec := range recs {
		obj.InChunk = inChunk
		for _,i := range iw.Indeces {
			if len(rec) <= i { continue }
			obj.Content = rec[i]
			obj.Column  = i
			err := iw.Enc.Encode(obj)
			if err!=nil { return err }
		}
	}
	return nil
}
func (iw *IndexWriter) AddFrom(r generic.ScanReader, chain []generic.Transform) error {
	obj := new(object)
	
	for {
		chunkNo,buf,err := r.ReadBufferWithPos()
		if err!=nil { return err }
		for _,t := range chain {
			nb := t(buf)
			generic.Free(buf)
			buf = nb
		}
		recs := csvpack.DecodeBuffer(buf)
		generic.Free(buf)
		obj.ChunkNo = chunkNo
		for inChunk,rec := range recs {
			obj.InChunk = inChunk
			for _,i := range iw.Indeces {
				if len(rec) <= i { continue }
				obj.Content = rec[i]
				obj.Column  = i
				err := iw.Enc.Encode(obj)
				if err!=nil { return err }
			}
		}
	}
	return nil
}
