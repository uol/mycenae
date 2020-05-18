package timeline

import (
	"sync"
	"time"

	"github.com/uol/logh"
)

/**
* Data processor interface and datd.
* @author rnojiri
**/

// DataProcessor - a interface for data processors
type DataProcessor interface {

	// Start - starts the data processor
	Start()

	// Stop - stops the data processor
	Stop()

	// GetName - returns the processor's name
	GetName() string

	// SetTransport - sets the transport
	SetTransport(transport Transport)

	// ProcessMapEntry - process a map entry and return true to delete the entry
	ProcessMapEntry(entry interface{}) (deleteAfter bool)
}

// dataProcessorCore - contains the common data
type dataProcessorCore struct {
	pointMap      sync.Map
	transport     Transport
	configuration *DataTransformerConf
	terminateChan chan struct{}
	loggers       *logh.ContextualLogger
	parent        DataProcessor
}

// SetTransport - sets the transport
func (d *dataProcessorCore) SetTransport(transport Transport) {
	d.transport = transport
}

// Stop - terminates the processing cycle
func (d *dataProcessorCore) Stop() {

	if logh.InfoEnabled {
		d.loggers.Info().Msg("closing...")
	}

	d.terminateChan <- struct{}{}
}

// Start - starts the processor cycle
func (d *dataProcessorCore) Start() {

	go func() {
		if logh.InfoEnabled {
			d.loggers.Info().Msgf("starting %s cycle", d.parent.GetName())
		}

		for {
			<-time.After(d.configuration.CycleDuration)

			select {
			case <-d.terminateChan:
				if logh.InfoEnabled {
					d.loggers.Info().Msgf("breaking %s cycle", d.parent.GetName())
				}
				return
			default:
			}

			count := 0

			d.pointMap.Range(func(k, v interface{}) bool {

				if delete := d.parent.ProcessMapEntry(v); delete {
					d.pointMap.Delete(k)
				}

				count++

				return true
			})

			if logh.DebugEnabled {
				d.loggers.Debug().Msgf("%d points were processed", count)
			}
		}
	}()
}
