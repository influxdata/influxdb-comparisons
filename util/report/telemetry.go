package report

import (
	"log"
	"os"
)

// TelemetryRunAsync runs a collection loop with many defaults already set. It will
// abort the program if an error occurs. Assumes points are owned by the
// GlobalPointPool.
func TelemetryRunAsync(c *Collector, batchSize uint64, writeToStderr bool, skipN uint64) (src chan *Point, done chan struct{}) {
	src = make(chan *Point, 100)
	done = make(chan struct{})

	send := func() {
		c.PrepBatch()
		if writeToStderr {
			_, err := os.Stderr.Write(c.buf.Bytes())
			if err != nil {
				log.Fatalf("collector error (stderr): %v", err.Error())
			}
		}

		err := c.SendBatch()
		if err != nil {
			log.Fatalf("collector error (http): %v", err.Error())
		}

		for _, p := range c.Points {
			PutPointIntoGlobalPool(p)
		}
	}

	go func() {
		var i uint64
		for p := range src {
			i++

			if i <= skipN {
				continue
			}

			c.Put(p)

			if i%batchSize == 0 {
				send()
				c.Reset()
			}
		}
		if len(c.Points) > 0 {
			send()
			c.Reset()
		}
		done <- struct{}{}
	}()

	return
}
