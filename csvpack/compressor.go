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
import "io"
import "github.com/maxymania/stunning-guacamole/generic"

type Writer struct{
	buf *matrixBuffer
	swr generic.SeqWriter
	chain []generic.Transform
}
func NewWriter(swr generic.SeqWriter,chain []generic.Transform) *Writer {
	w := new(Writer)
	w.buf   = new(matrixBuffer)
	w.buf.maxsize = 1<<24
	w.swr   = swr
	w.chain = chain
	return w
}
func (w *Writer) Write(record []string) (err error) {
	w.buf.AddRecord(record)
	if w.buf.Size() >= w.buf.maxsize {
		err = w.swr.WriteBuffer( w.buf.Serialize() )
		w.buf.Reset()
	}
	return
}
func (w *Writer) WriteFrom(r *csv.Reader) error {
	for {
		rec,err := r.Read()
		if err==io.EOF { return nil }
		if err!=nil { return err }
		w.buf.AddRecord(rec)
		if w.buf.Size() >= w.buf.maxsize {
			err = w.Flush()
			if err!=nil { return err }
		}
	}
	panic("unreachable")
}

func (w *Writer) Flush() (err error) {
	if len(w.buf.buffer)>0 {
		chunk := w.buf.Serialize()
		
		for _,t := range w.chain {
			nc := t(chunk)
			generic.Free(chunk)
			chunk = nc
		}
		
		err = w.swr.WriteBuffer(chunk)
		generic.Free(chunk)
		w.buf.Reset()
	}
	return err
}

