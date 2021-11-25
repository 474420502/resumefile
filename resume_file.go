package resumefile

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/474420502/structure/tree/avl"
)

type State int

const (
	StateErrorMD5       State = -5 // MD5校验错误
	StateErrorFileSync  State = -4 // 文件同步写入错误
	StateErrorFileWrite State = -3 // 文件写入错误
	StateErrorSeek      State = -2 // Seek错误
	StateErrorOutOfSize State = -1 // 插入错误,超出文件最大范围
	StateCompleted      State = 0  // 完成
	StateMegre          State = 1  // 插入数据与前数据合并
	StateInsert         State = 2  // 插入数据并没有与其他块数据交接
)

func (s State) String() string {
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
	FilePath     string
	File         *os.File
	Size         uint64 // 文件总Size
	Data         *avl.Tree
	MD5          []byte
	LackingLimit int // 限制GetLacking的数量
}

// NewResumeFile 创建一个可填充, 断点续传的文件
func NewResumeFile(filepath string, size uint64) *ResumeFile {

	if size == 0 {
		panic(fmt.Errorf("ResumeFile Size is Zero"))
	}

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

	return &ResumeFile{File: f, Size: size, Data: avl.New(partRangeCompare), FilePath: filepath}
}

// Put 把分块数据填充文件
func (rfile *ResumeFile) Put(pr PartRange, data []byte) (State, error) {
	ppr := &pr
	var state State = StateInsert
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

// Close 关闭 rfile相关文件
func (rfile *ResumeFile) Close() error {
	rfile.Data = nil
	return rfile.File.Close()
}

// Remove 关闭rfile相关文件, 移除文件
func (rfile *ResumeFile) Remove() error {
	var err error
	err = rfile.Close()
	if err != nil {
		return err
	}
	err = os.Remove(rfile.FilePath)
	if err != nil {
		return err
	}
	return nil
}

// SetVaildMD5 设置需要校验的md5
func (rfile *ResumeFile) SetVaildMD5(md5data []byte) {
	rfile.MD5 = md5data
}

// GetVaildMD5 获取设置需要校验的md5
func (rfile *ResumeFile) GetVaildMD5() []byte {
	return rfile.MD5
}

// VaildMD5 校验md5
func (rfile *ResumeFile) VaildMD5() bool {
	return bytes.Equal(rfile.GetCurrentMD5(), rfile.MD5)
}

// GetLacking 获取缺少的范围
func (rfile *ResumeFile) GetLacking() []PartRange {
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
		if rfile.LackingLimit > 0 && len(result) >= rfile.LackingLimit {
			return result
		}
		lackStart = pr.End
	}

	if lackStart < rfile.Size {
		result = append(result, PartRange{Start: lackStart, End: rfile.Size})
	}

	return result
}

// GetCurrentMD5 获取当前已经填充的文件file
func (rfile *ResumeFile) GetCurrentMD5() []byte {
	rfile.File.Seek(0, 0)
	md5hash := md5.New()
	_, err := io.Copy(md5hash, rfile.File)
	// data, err := ioutil.ReadAll(rfile.File)
	if err != nil {
		panic(err)
	}

	return md5hash.Sum(nil)
}

// GetModTime 获取文件的修改时间
func (rfile *ResumeFile) GetModTime() time.Time {
	info, err := os.Stat(rfile.FilePath)
	if os.IsNotExist(err) {
		panic(err)
	}
	return info.ModTime()
}
