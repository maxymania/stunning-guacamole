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

// V1 Encoder (standard)
func EncoderV1() []Transform {
	return []Transform{
		Lz4,
		Huffman,
	}
}
// V1 Decoder
func DecoderV1() []Transform {
	return []Transform{
		UnHuffman,
		UnLz4,
	}
}
// V1 Encoder (Medium Compression)
func EncoderV1MC() []Transform {
	return []Transform{
		FakeLz4,
		DeflateFast,
	}
}
// V1 Encoder (High Compression)
func EncoderV1HC() []Transform {
	return []Transform{
		Lz4HC,
		Huffman,
	}
}
// V1 Encoder (Ultra High Compression)
func EncoderV1UHC() []Transform {
	return []Transform{
		FakeLz4,
		DeflateSlow,
	}
}


// V2 Encoder (standard)
func EncoderV2(key []byte) []Transform {
	return []Transform{
		Lz4,
		Huffman,
		Encrypt(key),
	}
}
// V2 Decoder
func DecoderV2(key []byte) []Transform {
	return []Transform{
		Decrypt(key),
		UnHuffman,
		UnLz4,
	}
}
// V2 Encoder (Medium Compression)
func EncoderV2MC(key []byte) []Transform {
	return []Transform{
		FakeLz4,
		DeflateFast,
		Encrypt(key),
	}
}
// V2 Encoder (High Compression)
func EncoderV2HC(key []byte) []Transform {
	return []Transform{
		Lz4HC,
		Huffman,
		Encrypt(key),
	}
}
// V2 Encoder (Ultra High Compression)
func EncoderV2UHC(key []byte) []Transform {
	return []Transform{
		FakeLz4,
		DeflateSlow,
		Encrypt(key),
	}
}
// V2 Encoder (Ultra Ultra High Compression)
func EncoderV2UUHC(key []byte) []Transform {
	return []Transform{
		FakeLz4,
		DeflateUltraSlow,
		Encrypt(key),
	}
}



