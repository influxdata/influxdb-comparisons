package bulk_query

import "sync"

type Processor interface {
	PrepareProcess(i int)
	RunProcess(i int, qorkersGroup *sync.WaitGroup, statPool sync.Pool, statChan chan *Stat)
}
