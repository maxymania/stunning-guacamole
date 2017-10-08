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


package util

import "flag"
import "encoding/hex"
import "github.com/maxymania/stunning-guacamole/generic"

var un = flag.Bool("un",false,"Unpack")

var mc = flag.Bool("mc",false,"Medium Compression")
var hc = flag.Bool("hc",false,"High Compression")
var uhc = flag.Bool("uhc",false,"Ultra High Compression")
var uuhc = flag.Bool("uuhc",false,"Ultra Ultra High Compression")

var key = flag.String("key","","Encryption key")

func Unpack() bool { return *un }

func getKey() []byte {
	if *key=="" { return nil }
	arr,err := hex.DecodeString(*key)
	if err!=nil { return arr }
	return []byte(*key)
}

func Encoder() []generic.Transform {
	k := getKey()
	if len(k)>0 {
		switch {
		case *mc: return generic.EncoderV2MC(k)
		case *hc: return generic.EncoderV2HC(k)
		case *uhc: return generic.EncoderV2UHC(k)
		case *uuhc: return generic.EncoderV2UUHC(k)
		}
		return generic.EncoderV2(k)
	}
	switch {
	case *mc: return generic.EncoderV1MC()
	case *hc: return generic.EncoderV1HC()
	case *uhc: return generic.EncoderV1UHC()
	case *uuhc: return generic.EncoderV1UHC()
	}
	return generic.EncoderV1()
}
func Decoder() []generic.Transform {
	k := getKey()
	if len(k)>0 {
		return generic.DecoderV2(k)
	}
	return generic.DecoderV1()
}

