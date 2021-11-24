package resumefile

import (
	"bytes"
	"crypto/md5"
	"io/ioutil"
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
	file := NewResumeFile(testfile, uint64(tfsize))
	defer file.Close()

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
		// log.Println(start, end)

		s, err := file.Put(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		if err != nil {
			t.Error(err)
		}
		// log.Println(s, file.Data.Values())
		if s == StateCompleted {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if !bytes.Equal(file.GetCurrentMD5(), md5.New().Sum(tfdata)) {
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
	file := NewResumeFile(testfile, uint64(tfsize))
	defer file.Close()

	var end = 0
	for start := 0; ; start = end {

		end = start + rand.Intn(tfsize/3)
		if end > tfsize {
			end = tfsize
		}

		// log.Println(start, end)
		s, err := file.Put(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		if err != nil {
			t.Error(err)
		}
		// log.Println(s, file.Data.Values())
		if s == StateCompleted {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if !bytes.Equal(file.GetCurrentMD5(), md5.New().Sum(tfdata)) {
		t.Error("md5 is not equal")
	}

}

func TestCase3(t *testing.T) {
	var testfile = "testfile3"

	_, err := os.Stat(testfile)
	if os.IsNotExist(err) {
		copyFileTo("go.mod", testfile)
	}

	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}
	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}

	rfile := NewResumeFile(testfile, uint64(len(tfdata)))

	if !bytes.Equal(rfile.GetCurrentMD5(), md5.New().Sum(tfdata)) {
		t.Error("md5 is not equal")
	}
}

func TestCase4(t *testing.T) {
	var testfile = "testfile4"

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
	md5hash := md5.New().Sum(tfdata)

	tfsize := len(tfdata)
	file := NewResumeFile(testfile, uint64(tfsize))
	defer file.Close()
	file.SetVaildMD5(md5hash)

	var start, end int
	for {
		start = rand.Intn(tfsize/4) + start
		if start > tfsize {
			start = tfsize - 1
		}
		end = start + rand.Intn(20)

		if end > tfsize {
			end = tfsize
		}
		// log.Println(start, end)

		_, err := file.Put(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		if err != nil {
			t.Error(err)
		}

		if end >= tfsize {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	// log.Println(file.Data.Values())
	for _, pr := range file.Lacking() {
		_, err := file.Put(pr, tfdata[pr.Start:pr.End])
		if err != nil {
			t.Error(err)
		}
	}

	if file.Data.Size() != 1 {
		panic("")
	}

	if !file.VaildMD5() {
		t.Error()
	}
}
