package resumefile

import (
	"crypto/md5"
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
