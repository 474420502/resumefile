package resumefile

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/474420502/random"
)

func init() {
	info, err := os.Stat("./tests")
	if os.IsNotExist(err) {
		err = os.Mkdir("./tests", 0774)
		if err != nil {
			panic(err)
		}
	} else {
		if !info.IsDir() {
			panic(fmt.Errorf("tests is exists. and not dir"))
		}
	}
}

func getTestFileData() []byte {
	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}
	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}
	return tfdata
}

func copyFileTo(src string, dst string) error {
	// Read all content of src to data
	data, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}
	// Write data to dst
	err = ioutil.WriteFile(dst, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func TestCase1(t *testing.T) {
	tfilemd5 := GetFileMD5Sum("go.mod")
	var testfile = "./tests/testfile1"

	rand := random.New()
	RemoveResumeFile(testfile)
	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}

	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}

	tfsize := len(tfdata)
	file := NewResumeFile(testfile)

	file.Create(tfilemd5, uint64(tfsize))
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

		s, err := file.Write(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		if err != nil {
			t.Error(err)
		}
		// log.Println(s, file.Data.Values())
		if s == StateCompleted {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if !bytes.Equal(file.GetCurrentMD5(), GetFileMD5Sum("./go.mod")) {
		t.Error("md5 is not equal")
		t.Errorf("%x\n%x", file.GetCurrentMD5(), GetFileMD5Sum("./go.mod"))
	}

	defer func() {

		file := NewResumeFile(testfile)
		// defer file.Close()
		if !file.WalExists() {
			t.Error("wal file is not exist")
		}
		err = file.Resume()
		if err != nil {
			t.Error(err)
		}
		defer file.Close()

		if file.Size != uint64(tfsize) {
			t.Error(file.Size)
		}

		if file.Data.Size() != 1 {
			t.Error("file.Data.Size != 1")
		}

		file.Data.Traverse(func(k, v interface{}) bool {
			if k.(*PartRange).End != file.Size {
				t.Error("PartRange.End != file.Size")
			}
			return true
		})

		log.Println(file.Size, file.Data.Size(), file.Data.Values(), file.GetModTime())
	}()

}

func TestCase2(t *testing.T) {
	tfilemd5 := GetFileMD5Sum("go.mod")
	var testfile = "./tests/testfile2"

	rand := random.New()
	RemoveResumeFile(testfile)
	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}
	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}

	tfsize := len(tfdata)
	var file *ResumeFile

	var end = 0
	for start := 0; ; start = end {

		file = NewResumeFile(testfile)
		if file.WalExists() {
			err = file.Resume()
			if err != nil {
				t.Error(err)
			}
		} else {
			file.Create(tfilemd5, uint64(tfsize))
		}

		end = start + rand.Intn(tfsize/3)
		if end > tfsize {
			end = tfsize
		}

		// log.Println(start, end)
		s, err := file.Write(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		if err != nil {
			t.Error(err)
		}
		// log.Println(s, file.Data.Values())
		if s == StateCompleted {
			break
		}
		err = file.Close()
		if err != nil {
			t.Error(err)
		}
		time.Sleep(time.Millisecond * 10)
	}

	if !bytes.Equal(file.GetCurrentMD5(), GetFileMD5Sum("./go.mod")) {
		t.Error("md5 is not equal")
	}

	err = file.Remove()
	if err != nil {
		t.Error(err)
	}

	file = NewResumeFile(testfile)
	if file.WalExists() {
		t.Error("Remove error")
	}
}

func TestCase3(t *testing.T) {

}

func TestCaseLacking(t *testing.T) {
	tfilemd5 := GetFileMD5Sum("go.mod")
	var testfile = "./tests/testfile_lacking"
	RemoveResumeFile(testfile)

	tfdata := getTestFileData()
	tfsize := len(tfdata)

	// file := NewResumeFile(testfile)
	// file.Create(tfilemd5, uint64(tfsize))

	file := MustNewResumeFile(testfile, tfilemd5, uint64(tfsize))

	defer file.Close()

	if len(file.GetLacking()) != 1 {
		panic("")
	}
	if pr := file.GetLacking()[0]; pr.Start != 0 && pr.End != uint64(tfsize) {
		panic("")
	}

	s, err := file.Write(PartRange{Start: 0, End: uint64(tfsize)}, tfdata)
	if err != nil {
		panic("")
	}
	if s != StateCompleted {
		panic("")
	}

	if len(file.GetLacking()) != 0 {
		log.Panic(file.GetLacking())
	}

}

func TestCase4(t *testing.T) {
	tfilemd5 := GetFileMD5Sum("go.mod")
	var testfile = "./tests/testfile4"

	rand := random.New()
	RemoveResumeFile(testfile)

	tf, err := os.Open("./go.mod")
	if err != nil {
		panic(err)
	}
	tfdata, err := ioutil.ReadAll(tf)
	if err != nil {
		panic(err)
	}

	tfsize := len(tfdata)
	file := NewResumeFile(testfile)

	file.Create(tfilemd5, uint64(tfsize))
	defer file.Close()

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

		_, err := file.Write(PartRange{Start: uint64(start), End: uint64(end)}, tfdata[start:end])
		if err != nil {
			t.Error(err)
		}

		if end >= tfsize {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	// log.Println(file.Data.Values())
	for _, pr := range file.GetLacking() {
		_, err := file.Write(pr, tfdata[pr.Start:pr.End])
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
