package runnel

import (
	"os"

	"github.com/asp2insp/go-misc/utils"
	"github.com/edsrzf/mmap-go"
)

func mmapFile(path string, fileFlags int, mmapFlags int) (mmap.MMap, *os.File) {
	file, err := os.OpenFile(path, fileFlags, 0666)
	utils.Check(err)
	if utils.Filesize(file) == 0 {
		err = file.Truncate(int64(os.Getpagesize()))
		utils.Check(err)
	}
	mapData, err := mmap.Map(file, mmapFlags, 0)
	utils.Check(err)
	return mapData, file
}
