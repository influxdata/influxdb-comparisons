package bulk_query

import "io"

type Scanner interface {
	RunScan(r io.Reader, closeChan chan int)
	IsScanFinished() bool
}
