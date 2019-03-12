package bulk_load

type Scanner interface {
	RunScanner(syncChanDone chan int) (int64, int64, int64)
	IsScanFinished() bool
}
