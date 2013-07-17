package main

import (
  "fmt"
	"os"
	//"strconv"
	"strings"
)

type DbfHead struct {
	Version    []byte
	Updatedate string
	Records    int64
	Headerlen  int64
	Recordlen  int64
}
type Field struct {
	Name             string
	Fieldtype        string
	FieldDataaddress []byte
	FieldLen         int64
	DecimalCount     []byte
	Workareaid       []byte
}
type Record struct {
	Delete bool
	//Data   string
	Data map[string]string
}

func GetDbfHead(dbfhead *DbfHead, reader *os.File) {
	//fileinfo, _ := reader.Stat()
	buf := make([]byte, 16)
	_, err := reader.Read(buf)
	if err != nil {
		panic(err)
	}
	dbfhead.Version = buf[0:1]
	dbfhead.Updatedate = fmt.Sprintf("%d", buf[1:4])
	dbfhead.Headerlen = Changebytetoint(buf[8:10])
	dbfhead.Recordlen = Changebytetoint(buf[10:12])
	dbfhead.Records = Changebytetoint(buf[4:8])
}
func RemoveNullfrombyte(b []byte) (s string) {
	for _, val := range b {
		if val == 0 {
			continue
		}
		s = s + string(val)
		//fmt.Println(s)
	}
	return
}
func GetFields(field *Field, reader *os.File, start, end int64) []Field {
	off := end - start - 264
	fieldlist := make([]Field, off/32)
	buf := make([]byte, off)
	//fmt.Println(off)
	_, err := reader.ReadAt(buf, start)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%d\n", len(buf))
	//fmt.Println(fieldlist)
	curbuf := make([]byte, 32)
	for i, val := range fieldlist {
		a := i * 32
		//fmt.Println(i, a)
		curbuf = buf[a:]
		//val.Name = fmt.Sprintf("%s", curbuf[0:10])
		val.Name = RemoveNullfrombyte(curbuf[0:11])
		//val.Name = string(curbuf[0:11])
		val.Fieldtype = fmt.Sprintf("%s", curbuf[11:12])
		val.FieldDataaddress = curbuf[12:16]
		val.FieldLen = Changebytetoint(curbuf[16:17])
		val.DecimalCount = curbuf[17:18]
		val.Workareaid = curbuf[20:21]
		//fmt.Printf("%s\n", val.Name)
		fieldlist[i] = val

	}
	//for _, val := range fieldlist {
	//fmt.Printf("%s\n", val.Name)
	//}
	return fieldlist
}
func Changebytetoint(b []byte) (x int64) {
	for i, val := range b {
		if i == 0 {
			x = x + int64(val)
		} else {
			x = x + int64(2<<7*int64(i)*int64(val))
		}
		//fmt.Println(x)
	}
	//fmt.Println(fieldlist)

	return
}
func GetRecords(dbfhead DbfHead, fields []Field, reader *os.File) (records map[int]Record) {
	recordlen := dbfhead.Recordlen
	start := dbfhead.Headerlen
	buf := make([]byte, recordlen)
	i := 1
	temp := map[int]Record{}
	for {
		_, err := reader.ReadAt(buf, start)
		if err != nil {
			return temp
			panic(err)
		}
		//fmt.Printf("%s\n", buf)
		record := Record{}
		//fmt.Println(string(buf[0:1]))
		if string(buf[0:1]) == " " {
			record.Delete = true
		} else if string(buf[0:1]) == "*" {
			record.Delete = false
		}
		//record.Data = fmt.Sprintf("%s", buf[1:])
		//temp[i] = record
		//fmt.Println(i, len(temp), temp[i])
		tempdata := map[string]string{}
		a := int64(1)
		for _, val := range fields {
			fieldlen := val.FieldLen
			//fmt.Println(fieldlen)
			//fmt.Println(len(val.Name))
			tempdata[val.Name] = strings.Trim(fmt.Sprintf("%s", buf[a:a+fieldlen]), " ")
			//fmt.Println(len(tempdata[val.Name]))
			//fmt.Println(record)
			a = a + fieldlen
		}
		record.Data = tempdata
		temp[i] = record
		//fmt.Println(i)
		//fmt.Println(record)
		start = start + recordlen
		i = i + 1
	}
}
func GetRecordbyField(fieldname string, fieldval string, fp *os.File) (record map[int]Record) {
	var start int64 = 32
	dbfhead := DbfHead{}
	field := Field{}
	records := map[int]Record{}
	GetDbfHead(&dbfhead, fp)
	headend := dbfhead.Headerlen
	fields := GetFields(&field, fp, start, headend)
	records = GetRecords(dbfhead, fields, fp)
	temp := map[int]Record{}
	i := 1
	for _, val := range records {
		for _, val1 := range fields {
			if val1.Name == fieldname && val.Delete {
				if val.Data[val1.Name] == fieldval {
					//fmt.Println(val.Data)
					temp[i] = val
				}
			}
		}
		i = i + 1
	}
	return temp
}
func main() {

	fp, err := os.OpenFile("*.dbf", os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer fp.Close()
	records := GetRecordbyField("******", "*****", fp)
	for _, val := range records {
		fmt.Println(val.Data["DIRECTORY"], val.Data["DIRSEQUENC"])
	}

}
