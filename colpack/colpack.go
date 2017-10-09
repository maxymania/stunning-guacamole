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

// Column-Oriented Structured Packing algorithm.
package colpack

import "encoding/binary"

func prefixSuffix(a, b string) (pre,suf int) {
	la := len(a)
	lb := len(b)
	lx := la
	if lx>lb { lx=lb }
	for pre = 0 ; pre<lx ; pre++ {
		if a[pre]!=b[pre] { break }
	}
	lx-=pre
	la--
	lb--
	for suf = 0 ; suf<lx ; suf++ {
		if a[la-suf]!=b[lb-suf] { break }
	}
	return
}


func encodeHint(idx,prefix,suffix int,mid string) string {
	idxs := make([]byte,32)
	idxs[0] = '1'
	n := 1
	n += binary.PutUvarint(idxs[n:],uint64(idx))
	n += binary.PutUvarint(idxs[n:],uint64(prefix))
	n += binary.PutUvarint(idxs[n:],uint64(suffix))
	return string(idxs[:n])+mid
}
func encodeSame(idx int) string {
	idxs := make([]byte,12)
	idxs[0] = '2'
	n := 1
	n += binary.PutUvarint(idxs[n:],uint64(idx))
	return string(idxs[:n])
}

func Compress(dst, src []string, lookBehind int) {
	for I,str := range src {
		if len(str)==0 { dst[I]="" ; continue } // Safe space!
		
		dst[I] = "0"+str
		
		for i,j := I,0; i>0 && j<lookBehind ; j++ {
			i--
			if src[i]==str {
				enc := encodeSame(i)
				if len(dst[I])>len(enc) {
					dst[I] = enc
					break
				}
			}else {
				pre,suf := prefixSuffix(src[i],str)
				if pre>0 || suf>0 {
					enc := encodeHint(i,pre,suf,str[pre:len(str)-suf])
					if len(dst[I])>len(enc) {
						dst[I] = enc
					}
				}
			}
		}
	}
}
func Decompress(dst, src []string) {
	for i,str := range src {
		if len(str)==0 { dst[i]="" ; continue } // Skip empty strings
		switch str[0] {
		case '0':
			dst[i] = str[1:]
		case '1':
			{
				data := []byte(str[1:])
				m := 0
				idx,n := binary.Uvarint(data[m:]) ; m+=n
				pre,n := binary.Uvarint(data[m:]) ; m+=n
				suf,n := binary.Uvarint(data[m:]) ; m+=n
				it := dst[idx]
				mid := str[m+1:]
				dst[i] = it[:int(pre)]+mid+it[len(it)-int(suf):]
			}
		case '2':
			{
				j,_ := binary.Uvarint([]byte(str[1:]))
				dst[i] = dst[j]
			}
		}
	}
}

