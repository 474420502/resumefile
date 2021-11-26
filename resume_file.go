package resumefile

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/474420502/structure/tree/avl"
)

type State int

const (
	StateErrorWal       State = -6 // Wal写入错误
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
	case StateErrorWal:
		return "StateErrorWal"
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
	Wal          *os.File
	MD5          []byte
	LackingLimit int // 限制GetLacking的数量
}

func NewResumeFile(filepath string) *ResumeFile {
	return &ResumeFile{
		FilePath: filepath,
		Data:     avl.New(partRangeCompare),
	}
}

// Create 创建ResumeFile文件
func (rfile *ResumeFile) Create(md5bytes []byte, size uint64) error {
	if size == 0 {
		panic(fmt.Errorf("ResumeFile Size is Zero"))
	}

	var (
		file, walfile *os.File
		// waldata       []byte
		err error
	)

	_, err = os.Stat(rfile.FilePath)
	if !os.IsNotExist(err) {
		return err
	}

	file, err = os.OpenFile(rfile.FilePath, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return err
	}
	err = syscall.Fallocate(int(file.Fd()), 0, 0, int64(size))
	if err != nil {
		return err
	}

	walfile, err = os.OpenFile(rfile.FilePath+walSuffix, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		return err
	}

	basebytes := encodeBaseInfo(md5bytes, size)
	_, err = walfile.Write(basebytes)
	if err != nil {
		panic(err)
	}

	rfile.Data = avl.New(partRangeCompare)
	rfile.File = file
	rfile.MD5 = md5bytes
	rfile.Size = size
	rfile.Wal = walfile

	//
	return nil
}

// WalExists 判断 Wal(*.rf) 是否存在
func (rfile *ResumeFile) WalExists() bool {
	_, err := os.Stat(rfile.FilePath + walSuffix)
	return !os.IsNotExist(err)
}

// Resume 从Wal日志里恢复(*.rf)
func (rfile *ResumeFile) Resume() error {

	var (
		err     error
		waldata []byte
		file    *os.File
		walfile *os.File
	)

	// resume data from wals
	waldata, err = ioutil.ReadFile(rfile.FilePath + walSuffix)
	if err != nil {
		return err
	}
	rfile.MD5, rfile.Size = decodeBaseInfo(waldata)
	wals, err := NewWalsDecode(waldata[24:])
	if err != nil {
		return err
	}
	rfile.Data.Clear()
	for _, wal := range wals {
		key := &wal.Key
		switch wal.Op {
		case WAL_PUT:
			rfile.Data.Put(key, key)
		case WAL_REMOVE:
			rfile.Data.Remove(key)
		}
	}

	// 恢复后打开 wal文件
	walfile, err = os.OpenFile(rfile.FilePath+walSuffix, os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		return err
	}

	// 恢复后打开 file文件
	file, err = os.OpenFile(rfile.FilePath, os.O_RDWR, 0664)
	if err != nil {
		return err
	}

	rfile.Wal = walfile
	rfile.File = file

	return nil
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
			_, err = rfile.Wal.Write(WalEncode(WAL_REMOVE, ppr))
			if err != nil {
				return StateErrorWal, err
			}
			err = rfile.Wal.Sync()
			if err != nil {
				return StateErrorWal, err
			}

			pdOld := p.(*PartRange)
			ppr.Merge(pdOld)
			if state != StateMegre {
				state = StateMegre
			}
		} else {
			if !rfile.Data.Put(ppr, ppr) {
				log.Panic("Put error")
			}
			_, err = rfile.Wal.Write(WalEncode(WAL_PUT, ppr))
			if err != nil {
				return StateErrorWal, err
			}
			err = rfile.Wal.Sync()
			if err != nil {
				return StateErrorWal, err
			}
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
	defer RemoveResumeFile(rfile.FilePath)
	var err = rfile.Close()
	if err != nil {
		return err
	}
	return nil
}

// SetVaildMD5 设置需要校验的md5
// func (rfile *ResumeFile) SetVaildMD5(md5data []byte) {
// 	rfile.MD5 = md5data
// }

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
