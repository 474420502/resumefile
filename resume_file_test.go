package main

import (
	"bytes"
	"crypto/md5"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/474420502/random"
)

func TestCase1(t *testing.T) {
	var testfile = "testfile1"

	rand := random.New()
	os.Remove(testfile)
	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}
	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}

	tfsize := len(tfdata)
	file := NewSubdividedFile(testfile, uint64(tfsize))

	min := tfsize
	max := 0

	for {
		start := rand.Intn(tfsize)
		end := rand.Intn(tfsize)
		if start > end {
			start, end = end, start
		}

		if min > start {
			min = start
		}

		if max < end {
			max = end
		}

		if max-min >= 90 {

			end = tfsize
			start = 0

		}
		log.Println(start, end)

		s := file.Put(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		log.Println(s, file.Data.Values())
		if s == StateCompleted {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	file.File.Seek(0, 0)
	writefile, err := ioutil.ReadAll(file.File)
	if err != nil {
		panic(err)
	}
	countmd5 := md5.New()

	if !bytes.Equal(countmd5.Sum(writefile), md5.New().Sum(tfdata)) {
		t.Error("md5 is not equal")
	}

}

func TestCase2(t *testing.T) {
	var testfile = "testfile2"

	rand := random.New()
	os.Remove(testfile)
	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}
	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}

	tfsize := len(tfdata)
	file := NewSubdividedFile(testfile, uint64(tfsize))

	var end = 0
	for start := 0; ; start = end {

		end = start + rand.Intn(tfsize/3)
		if end > tfsize {
			end = tfsize
		}

		log.Println(start, end)

		s := file.Put(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		log.Println(s, file.Data.Values())
		if s == StateCompleted {
			break
		}

	}

	file.File.Seek(0, 0)
	writefile, err := ioutil.ReadAll(file.File)
	if err != nil {
		panic(err)
	}
	countmd5 := md5.New()

	if !bytes.Equal(countmd5.Sum(writefile), md5.New().Sum(tfdata)) {
		t.Error("md5 is not equal")
	}

}
