package resumefile

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"syscall"
	"time"

	"github.com/474420502/structure/tree/avl"
)

type ResumeFileState int

const (
	StateErrorMD5       ResumeFileState = -5 // MD5校验错误
	StateErrorFileSync  ResumeFileState = -4 // 文件同步写入错误
	StateErrorFileWrite ResumeFileState = -3 // 文件写入错误
	StateErrorSeek      ResumeFileState = -2 // Seek错误
	StateErrorOutOfSize ResumeFileState = -1 // 插入错误,超出文件最大范围
	StateCompleted      ResumeFileState = 0  // 完成
	StateMegre          ResumeFileState = 1  // 插入数据与前数据合并
	StateInsert         ResumeFileState = 2  // 插入数据并没有与其他块数据交接
)

func (s ResumeFileState) String() string {
	switch s {
	case StateErrorMD5:
		return "StateErrorMD5"
	case StateErrorFileSync:
		return "StateErrorFileSync"
	case StateErrorFileWrite:
		return "StateErrorFileWrite"
	case StateErrorSeek:
		return "StateErrorSeek"
	case StateErrorOutOfSize:
		return "StateErrorOutOfSize"
	case StateCompleted:
		return "StateCompleted"
	case StateMegre:
		return "StateMegre"
	case StateInsert:
		return "StateInsert"
	default:
		return "UnknownState"
	}
}

// ResumeFile 细分的文件数据结构
type ResumeFile struct {
	File     *os.File
	Size     uint64 // 文件总Size
	Data     *avl.Tree
	CreateAt time.Time
	MD5      []byte
}

func (rfile *ResumeFile) Put(pr PartRange, data []byte) (ResumeFileState, error) {
	ppr := &pr
	var state ResumeFileState = StateInsert
	if ppr.End > rfile.Size { // 超过最大值, 返回错误
		return StateErrorOutOfSize, fmt.Errorf("PartRange End > Size of resumefile")
	}

	// 文件定位然后写入数据
	_, err := rfile.File.Seek(int64(ppr.Start), io.SeekStart)
	if err != nil {
		return StateErrorSeek, err
	}
	_, err = rfile.File.Write(data)
	if err != nil {
		return StateErrorFileWrite, err
	}
	// 同步文件数据
	err = rfile.File.Sync()
	if err != nil {
		return StateErrorFileSync, err
	}

	// 循环合并 数据块, 使块可以通过tree快速检索.
	for {
		if p, ok := rfile.Data.Remove(ppr); ok {
			pdOld := p.(*PartRange)
			ppr.Merge(pdOld)
			if state != StateMegre {
				state = StateMegre
			}
		} else {
			rfile.Data.Set(ppr, ppr)
			break
		}
	}

	if rfile.Data.Size() == 1 {
		var ppr *PartRange // ppr必然不会nil
		rfile.Data.Traverse(func(k, v interface{}) bool {
			ppr = v.(*PartRange)
			return false
		})
		if ppr.End-ppr.Start == rfile.Size {
			if len(rfile.MD5) != 0 {
				if !rfile.VaildMD5() {
					return StateErrorMD5, fmt.Errorf("want md5 %x != ResumeFile md5 %x", rfile.MD5, rfile.GetCurrentMD5())
				}
			}
			state = StateCompleted
		}
	}
	return state, nil
}

func (rfile *ResumeFile) Close() error {
	return rfile.File.Close()
}

func (rfile *ResumeFile) SetVaildMD5(md5data []byte) {
	rfile.MD5 = md5data
}

func (rfile *ResumeFile) VaildMD5() bool {
	return bytes.Equal(rfile.GetCurrentMD5(), rfile.MD5)
}

func (rfile *ResumeFile) Lacking() []PartRange {
	var result []PartRange

	var lackStart uint64 = 0
	iter := rfile.Data.Iterator()

	for iter.SeekToFirst(); iter.Vaild(); iter.Next() {
		pr := iter.Value().(*PartRange)
		start := pr.Start - lackStart
		if start <= 0 {
			lackStart = pr.End
			continue
		}
		result = append(result, PartRange{Start: lackStart, End: pr.Start})
		lackStart = pr.End

	}

	if lackStart < rfile.Size {
		result = append(result, PartRange{Start: lackStart, End: rfile.Size})
	}

	return result
}

func (rfile *ResumeFile) GetCurrentMD5() []byte {
	rfile.File.Seek(0, 0)
	data, err := ioutil.ReadAll(rfile.File)
	if err != nil {
		panic(err)
	}

	return md5.New().Sum(data)
}

func NewResumeFile(filepath string, size uint64) *ResumeFile {
	var f *os.File
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		f, err = os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0664)
		if err != nil {
			panic(err)
		}
		err = syscall.Fallocate(int(f.Fd()), 0, 0, int64(size))
		if err != nil {
			panic(err)
		}
	} else {
		f, err = os.OpenFile(filepath, os.O_RDWR, 0664)
		if err != nil {
			panic(err)
		}
	}

	//

	return &ResumeFile{File: f, Size: size, Data: avl.New(partRangeCompare)}
}
