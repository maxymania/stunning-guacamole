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

// Bolt-Based Index over a compressed CSV-File.
package csvbolt

import "github.com/maxymania/stunning-guacamole/generic"
import "github.com/maxymania/stunning-guacamole/csvpack"
import "github.com/vmihailenco/msgpack"
import "sort"
import "github.com/boltdb/bolt"
import "encoding/binary"

func i2b(i int) []byte {
	b := make([]byte,12)
	return b[:binary.PutUvarint(b,uint64(i))]
}

type pos struct{
	_msgpack struct{} `msgpack:",asArray"`
	ChunkNo int64
	InChunk int
}
func expandP(p []pos) []pos {
	if (len(p)-cap(p))==0 {
		np := make([]pos,len(p),len(p)+128)
		copy(np,p)
		return np
	}
	return p
}

type object struct{
	Column  int
	Data    string
	pos
}
func expand(p []object) []object {
	if (len(p)-cap(p))==0 {
		np := make([]object,len(p),len(p)+128)
		copy(np,p)
		return np
	}
	return p
}
type array []object
func (a array) Len() int { return len(a) }
func (a array) Less(i, j int) bool {
	if a[i].Column>a[j].Column { return false }
	return (a[i].Column<a[j].Column) || (a[i].Data<a[j].Data)
}
func (a array) Swap(i, j int) { a[i],a[j] = a[j],a[i] }

type aPos []pos
func (a aPos) Len() int { return len(a) }
func (a aPos) Less(i, j int) bool {
	if a[i].ChunkNo>a[j].ChunkNo { return false }
	return (a[i].ChunkNo<a[j].ChunkNo) || (a[i].InChunk<a[j].InChunk)
}
func (a aPos) Swap(i, j int) { a[i],a[j] = a[j],a[i] }


type IndexReader struct{
	DB *bolt.DB
}
func (ir *IndexReader) Lookup(col int,dat string,r generic.RandomAccessReader, chain []generic.Transform) (reco [][]string,e error) {
	var target []pos
	e = ir.DB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(i2b(col))
		if bkt==nil { return nil }
		msgpack.Unmarshal(bkt.Get([]byte(dat)),&target)
		return nil
	})
	if e!=nil { return }
	sort.Sort(aPos(target))
	chunk := int64(-1)
	var block [][]string
	lb := 0
	for _,elem := range target {
		if chunk != elem.ChunkNo {
			chunk = elem.ChunkNo
			buf,err := r.ReadBufferAt(chunk)
			if err!=nil { return nil,err }
			for _,t := range chain {
				nb := t(buf)
				generic.Free(buf)
				buf = nb
			}
			block = csvpack.DecodeBuffer(buf)
			lb = len(block)
			generic.Free(buf)
		}
		if elem.InChunk<lb { reco = append(reco,block[elem.InChunk]) }
	}
	return
}

type IndexWriter struct{
	DB *bolt.DB
	Indeces []int
}

func (iw *IndexWriter) AddFrom(r generic.ScanReader, chain []generic.Transform) error {
	var obj object
	lines := array{}
	
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
		lines = lines[:0]
		for inChunk,rec := range recs {
			obj.InChunk = inChunk
			for _,i := range iw.Indeces {
				if len(rec) <= i { continue }
				obj.Data = rec[i]
				obj.Column  = i
				lines = append(expand(lines),obj)
			}
		}
		sort.Sort(lines)
		iw.DB.Update(func(tx *bolt.Tx) error {
			var err error
			var bkt *bolt.Bucket
			col := -1
			name := ""
			value := []pos{}
			iLoad := false
			for _,line := range lines {
				if line.Data == "" { continue }
				iCol := line.Column!=col
				iVal := line.Data!=name
				if (iCol || iVal) && iLoad {
					bits,_ := msgpack.Marshal(value)
					err = bkt.Put([]byte(name),bits)
					if err!=nil { return err }
				}
				if iCol {
					col = line.Column
					bkt,err = tx.CreateBucketIfNotExists(i2b(col))
					if err!=nil { return err }
					name = line.Data
					value = nil
					msgpack.Unmarshal(bkt.Get([]byte(name)), &value)
					iLoad = true
				}
				if iVal {
					name = line.Data
					value = nil
					msgpack.Unmarshal(bkt.Get([]byte(name)), &value)
					iLoad = true
				}
				value = append(expandP(value),line.pos)
			}
			return nil
		})
	}
	return nil
}

