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
import "github.com/klauspost/compress/flate"
import "io"
import "github.com/pierrec/lz4"
import "encoding/binary"
import "github.com/mad-day/lioness"
import "golang.org/x/crypto/sha3"
import "time"

var prng = sha3.NewShake256()

func init(){
	u := uint64(time.Now().UTC().Unix())
	b := []byte{
		byte(u>>56),
		byte(u>>48),
		byte(u>>40),
		byte(u>>32),
		byte(u>>24),
		byte(u>>16),
		byte(u>> 8),
		byte(u    ),
	}
	prng.Write(b)
}

var genpool bytebufferpool.Pool

type binRead struct{
	arr []byte
}
func (b *binRead) Read(p []byte) (n int, err error) {
	n = copy(p,b.arr)
	if n<len(p) { err = io.EOF }
	b.arr = b.arr[n:]
	return
}

func resize(b []byte,n int) []byte {
	if cap(b)<n { return make([]byte,n) }
	return b[:n]
}

func Free(buf *bytebufferpool.ByteBuffer) {
	genpool.Put(buf)
}

func Alloc() *bytebufferpool.ByteBuffer {
	return genpool.Get()
}

type Transform func(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer

// RFC 1951 compliant Huffman-Only DEFLATE Encoder.
func Huffman(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	n := Alloc()
	w,_ := flate.NewWriter(n,flate.HuffmanOnly)
	b.WriteTo(w)
	w.Close()
	return n
}

// RFC 1951 compliant High Speed DEFLATE Encoder.
func DeflateFast(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	n := Alloc()
	w,_ := flate.NewWriter(n,2)
	b.WriteTo(w)
	w.Close()
	return n
}

// RFC 1951 compliant High Compression DEFLATE Encoder.
func DeflateSlow(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	n := Alloc()
	w,_ := flate.NewWriter(n,7)
	b.WriteTo(w)
	w.Close()
	return n
}

// RFC 1951 compliant High Compression DEFLATE Encoder.
func DeflateUltraSlow(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	n := Alloc()
	w,_ := flate.NewWriter(n,9)
	b.WriteTo(w)
	w.Close()
	return n
}

// RFC 1951 DEFLATE Decoder.
func UnHuffman(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	n := Alloc()
	r := flate.NewReader(&binRead{b.B})
	io.Copy(n,r)
	r.Close()
	return n
}

func Lz4(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	lb := len(b.B)
	n := Alloc()
	n.B = resize(n.B,lz4.CompressBlockBound(lb)+4)
	cs,e := lz4.CompressBlock(b.B,n.B[4:],0)
	if e!=nil || cs==0 {
		binary.BigEndian.PutUint32(n.B,0)
		n.B = append(n.B[:4],b.B...)
		return n
	}
	binary.BigEndian.PutUint32(n.B,uint32(lb))
	n.B = n.B[:cs+4]
	return n
}
func Lz4HC(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	lb := len(b.B)
	n := Alloc()
	n.B = resize(n.B,lz4.CompressBlockBound(lb)+4)
	cs,e := lz4.CompressBlockHC(b.B,n.B[4:],0)
	if e!=nil || cs==0 {
		binary.BigEndian.PutUint32(n.B,0)
		n.B = append(n.B[:4],b.B...)
		return n
	}
	binary.BigEndian.PutUint32(n.B,uint32(lb))
	n.B = n.B[:cs+4]
	return n
}
func FakeLz4(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	lb := len(b.B)
	n := Alloc()
	n.B = resize(n.B,lb+4)
	binary.BigEndian.PutUint32(n.B,0)
	n.B = append(n.B[:4],b.B...)
	return n
}
func UnLz4(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
	n := Alloc()
	if len(b.B)<4 { return n } // Error!
	tl := int(binary.BigEndian.Uint32(b.B))
	if tl==0 {
		n.Set(b.B[4:])
		return n
	}
	n.B = resize(n.B,tl)
	us,e := lz4.UncompressBlock(b.B[4:],n.B,0)
	if e!=nil || us!=tl { n.B = n.B[:0] } // Error!
	return n
}

func Encrypt(key []byte) (t Transform) {
	var sk [lioness.KeySize]byte
	sha3.ShakeSum256(key,sk[:])
	t = func(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
		var iv [lioness.IVSize]byte
		prng.Read(iv[:])
		n := Alloc()
		if len(b.B)<lioness.MinBlockSize || lioness.MaxBlockSize<len(b.B) {
			n.B = append(resize(n.B,lioness.IVSize),b.B...)
			return n
		}
		n.B = resize(n.B,len(b.B)+lioness.IVSize)
		copy(n.B,iv[:])
		lioness.Encrypt(sk,iv,n.B[lioness.IVSize:],b.B)
		return n
	}
	return
}

func Decrypt(key []byte) (t Transform) {
	var sk [lioness.KeySize]byte
	sha3.ShakeSum256(key,sk[:])
	t = func(b *bytebufferpool.ByteBuffer) *bytebufferpool.ByteBuffer {
		var iv [lioness.IVSize]byte
		n := Alloc()
		if len(b.B)<(lioness.MinBlockSize+lioness.IVSize) || (lioness.MaxBlockSize+lioness.IVSize)<len(b.B) {
			n.B = append(resize(n.B,lioness.IVSize),b.B...)
			return n
		}
		n.B = resize(n.B,len(b.B)-lioness.IVSize)
		copy(iv[:],b.B)
		lioness.Decrypt(sk,iv,n.B,b.B[lioness.IVSize:])
		return n
	}
	return
}

