package bulk_load

import "io"

type Scanner interface {
	RunScanner(r io.Reader, syncChanDone chan int)
	IsScanFinished() bool
	GetReadStatistics() (itemsRead, bytesRead, valuesRead int64)
}
