package bulk_load

type Scanner interface {
	RunScanner(syncChanDone chan int)
	IsScanFinished() bool
	GetReadStatistics() (itemsRead, bytesRead, valuesRead int64)
}
