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

import "github.com/valyala/bytebufferpool"
import "github.com/maxymania/stunning-guacamole/generic"
import "github.com/vmihailenco/msgpack"
import "bytes"

type matrixBuffer struct{
	buffer   [][]string
	maxwidth int
	bincount int
	
	maxsize  int
}

func (m *matrixBuffer) Reset() {
	m.buffer   = m.buffer[:0]
	m.maxwidth = 0
	m.bincount = 0
}
func (m *matrixBuffer) Size() int { return (len(m.buffer)*m.maxwidth) + m.bincount }
func (m *matrixBuffer) AddRecord(rec []string) {
	rw := len(rec)
	if m.maxwidth<rw { m.maxwidth = rw }
	for _,cell := range rec { m.bincount += len(cell)+3 }
	m.buffer = append(m.buffer,rec)
}
func (m *matrixBuffer) Serialize() *bytebufferpool.ByteBuffer {
	n := generic.Alloc()
	pk := msgpack.NewEncoder(n)
	colspan := m.maxwidth
	pk.Encode(m.maxwidth,len(m.buffer))
	for i:=0 ; i<colspan ; i++ {
		for _,rec := range m.buffer {
			if len(rec)>i {
				pk.Encode(rec[i])
			}else{
				pk.Encode("")
			}
		}
	}
	return n
}

func DecodeBuffer(b *bytebufferpool.ByteBuffer) [][]string {
	return deserialize(b)
}

func deserialize(b *bytebufferpool.ByteBuffer) [][]string {
	var width,rows int
	pk := msgpack.NewDecoder(bytes.NewReader(b.B))
	pk.Decode(&width,&rows)
	data := make([][]string,rows)
	for i:=0 ; i<rows ; i++ {
		data[i] = make([]string,width)
	}
	for j:=0 ; j<width ; j++ {
		for i:=0 ; i<rows ; i++ {
			pk.Decode(&data[i][j])
		}
	}
	return data
}

