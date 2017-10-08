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


package generic

import "github.com/valyala/bytebufferpool"
import "io"
import "encoding/binary"

func readFullAt(r io.ReaderAt,buf []byte, offset int64) (n int,err error){
	min := len(buf)
	for n<min && err==nil {
		var nn int
		nn, err = r.ReadAt(buf[n:],offset)
		n+=nn
		offset+=int64(nn)
	}
	if n>=min {
		err = nil
	} else if n > 0 && err==io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return
}


type SeqWriter interface{
	WriteBuffer(buf *bytebufferpool.ByteBuffer) error
}
type SeqReader interface{
	ReadBuffer() (*bytebufferpool.ByteBuffer,error)
}
type RandomAccessReader interface{
	ReadBufferAt(offset int64) (*bytebufferpool.ByteBuffer,error)
}
type ScanReader interface{
	ReadBufferWithPos() (int64,*bytebufferpool.ByteBuffer,error)
}


func NewWriter(w io.Writer) SeqWriter { return &writer{wri:w} }

type writer struct{
	buf [4]byte
	wri io.Writer
}
func (w *writer) WriteBuffer(buf *bytebufferpool.ByteBuffer) error {
	binary.BigEndian.PutUint32(w.buf[:],uint32(len(buf.B)))
	_,e := w.wri.Write(w.buf[:])
	if e!=nil { return e }
	_,e = w.wri.Write(buf.B)
	return e
}


type Reader interface{
	SeqReader
	ScanReader
}

func NewReader(r io.Reader) Reader { return &seqReader{rea:r,pos:0} }

type seqReader struct{
	buf [4]byte
	rea io.Reader
	pos int64
}
func (r *seqReader) ReadBufferWithPos() (pos int64,buf *bytebufferpool.ByteBuffer,err error) {
	pos = r.pos
	_,err = io.ReadFull(r.rea,r.buf[:])
	if err!=nil { return }
	l := int(binary.BigEndian.Uint32(r.buf[:]))
	buf = Alloc()
	buf.B = resize(buf.B,l)
	_,err = io.ReadFull(r.rea,buf.B)
	r.pos = pos+int64(l+4)
	return
}
func (r *seqReader) ReadBuffer() (*bytebufferpool.ByteBuffer,error) {
	_,b,e := r.ReadBufferWithPos()
	return b,e
}

type ReaderAt interface{
	Reader
	RandomAccessReader
}

func NewReaderAt(r io.ReaderAt) ReaderAt { return &parReader{rea:r,pos:0} }

type parReader struct{
	buf [4]byte
	rea io.ReaderAt
	pos int64
}
func (r *parReader) ReadBufferAt(offset int64) (buf *bytebufferpool.ByteBuffer,err error) {
	pos := offset
	_,err = readFullAt(r.rea,r.buf[:],pos)
	if err!=nil { return }
	l := int(binary.BigEndian.Uint32(r.buf[:]))
	buf = Alloc()
	buf.B = resize(buf.B,l)
	_,err = readFullAt(r.rea,buf.B,pos+4)
	return
}
func (r *parReader) ReadBufferWithPos() (pos int64,buf *bytebufferpool.ByteBuffer,err error) {
	pos = r.pos
	buf,err = r.ReadBufferAt(pos)
	if buf!=nil { r.pos = pos+4+int64(len(buf.B)) }
	return
}
func (r *parReader) ReadBuffer() (*bytebufferpool.ByteBuffer,error) {
	_,b,e := r.ReadBufferWithPos()
	return b,e
}
