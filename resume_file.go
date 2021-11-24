package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"

	indextree "github.com/474420502/structure/tree/itree"
)

type ResumeFileState int

const (
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
	Data     *indextree.Tree
	CreateAt time.Time
	MD5      []byte
}

func (rfile *ResumeFile) Put(pr PartRange, data []byte) ResumeFileState {
	ppr := &pr
	var state ResumeFileState = StateInsert
	if ppr.End > rfile.Size { // 超过最大值, 返回错误
		return StateErrorOutOfSize
	}

	_, err := rfile.File.Seek(int64(ppr.Start), io.SeekStart)
	if err != nil {
		log.Println(err)
		return StateErrorSeek
	}
	_, err = rfile.File.Write(data)
	if err != nil {
		log.Println(err)
		return StateErrorFileWrite
	}
	err = rfile.File.Sync()
	if err != nil {
		log.Println(err)
		return StateErrorFileSync
	}

	for {

		if p := rfile.Data.Remove(ppr); p != nil {
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
		_, v := rfile.Data.Index(0)
		ppr = v.(*PartRange)
		if ppr.End-ppr.Start == rfile.Size {
			state = StateCompleted
		}
	}
	return state
}

// PartRange SubdividedFile的Data里的块
type PartRange struct {
	Start uint64
	End   uint64
}

func (pd *PartRange) String() string {
	return fmt.Sprintf("[%d-%d]", pd.Start, pd.End)
}

// Merge 合并其他范围
func (pd *PartRange) Merge(other *PartRange) {
	if pd.Start > other.Start {
		pd.Start = other.Start
	}

	if pd.End < other.End {
		pd.End = other.End
	}
}

func compare(k1, k2 interface{}) int {
	d1 := k1.(*PartRange)
	d2 := k2.(*PartRange)
	if d1.End < d2.Start {
		return -1
	} else if d1.Start > d2.End {
		return 1
	} else {
		return 0
	}
}

func NewSubdividedFile(filepath string, size uint64) *ResumeFile {
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

	return &ResumeFile{File: f, Size: size, Data: indextree.New(compare)}
}

type UploadPart struct {
}
