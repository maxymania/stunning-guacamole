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


package main

import "flag"
import "github.com/maxymania/stunning-guacamole/cmd/cmd-util"
import "github.com/maxymania/stunning-guacamole/csvpack"
import "github.com/maxymania/stunning-guacamole/generic"
import "encoding/csv"
import "os"
import "log"

var inpt = flag.String("input","","Input file")
var outpt = flag.String("output","","Output file")
var appnd = flag.Bool("append",false,"Appends to a file")

func main() {
	flag.Parse()
	var I,O *os.File
	var err error
	I,err = os.Open(*inpt)
	if err!=nil { flag.PrintDefaults(); log.Fatalln("Error (input)",err) }
	if *appnd {
		O,err = os.OpenFile(*outpt,os.O_APPEND|os.O_CREATE|os.O_WRONLY,0640)
	} else {
		O,err = os.OpenFile(*outpt,os.O_TRUNC|os.O_CREATE|os.O_WRONLY,0640)
	}
	if err!=nil { flag.PrintDefaults(); log.Fatalln("Error (ouput)",err) }
	defer I.Close()
	defer O.Close()
	
	if util.Unpack() {
		r := csvpack.NewReader(generic.NewReader(I),util.Decoder())
		w := csv.NewWriter(O)
		r.ReadTo(w)
		w.Flush()
	} else {
		r := csv.NewReader(I)
		w := csvpack.NewWriter(generic.NewWriter(O),util.Encoder())
		w.WriteFrom(r)
		w.Flush()
	}
}
