package resumefile

import (
	"crypto/md5"
	"encoding/binary"
	"io"
	"os"
)

// GetFileMD5Sum 获取 文件md5
func GetFileMD5Sum(file string) []byte {
	tf, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	md5hash := md5.New()
	io.Copy(md5hash, tf)
	return md5hash.Sum(nil)
}

func RemoveResumeFile(filepath string) {
	os.Remove(filepath + ".rf")
	os.Remove(filepath)
}

func encodeBaseInfo(md5bytes []byte, size uint64) []byte {
	var buf []byte = make([]byte, 24) // 16 byte
	copy(buf, md5bytes)
	binary.BigEndian.PutUint64(buf[16:24], size) // 8 byte
	return buf
}

func decodeBaseInfo(basebytes []byte) (md5bytes []byte, size uint64) {
	copy(md5bytes, basebytes[0:16])                  // 16 byte
	size = binary.BigEndian.Uint64(basebytes[16:24]) // 8 byte
	return
}
