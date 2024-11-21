package nozzle

import (
	"time"

	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventrouter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventsource"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/monitoring"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
	"github.com/gorilla/websocket"
)

type Config struct {
	Logger                lager.Logger
	StatusMonitorInterval time.Duration
}

// Nozzle reads events from eventsource.Source and routes events
// to targets by using eventrouter.Router
type Nozzle struct {
	eventSource eventsource.Source
	eventRouter eventrouter.Router
	config      *Config

	closing chan struct{}
	closed  chan struct{}
}

func New(eventSource eventsource.Source, eventRouter eventrouter.Router, config *Config) *Nozzle {
	return &Nozzle{
		eventRouter: eventRouter,
		eventSource: eventSource,
		config:      config,
		closing:     make(chan struct{}, 1),
		closed:      make(chan struct{}, 1),
	}
}

func (f *Nozzle) Start() error {
	err := f.eventSource.Open()
	if err != nil {
		return err
	}

	defer close(f.closed)

	receivedCount := monitoring.RegisterCounter("firehose.events.received.count", utils.UintType)

	var lastErr error
	events, errs := f.eventSource.Read()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				f.config.Logger.Info("Give up after retries. Firehose consumer is going to exit")
				return lastErr
			}
			receivedCount.Add(uint64(1))
			if err := f.eventRouter.Route(event); err != nil {
				f.config.Logger.Error("Failed to route event", err)
			}

		case lastErr = <-errs:
			f.handleError(lastErr)

		case <-f.closing:
			return lastErr
		}
	}
}

func (f *Nozzle) Close() error {
	err := f.eventSource.Close()
	if err != nil {
		return err
	}

	close(f.closing)
	<-f.closed
	return nil
}

func (f *Nozzle) handleError(err error) {
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		f.config.Logger.Error("Error while reading from the firehose", err)
		return
	}

	msg := ""
	switch closeErr.Code {
	case websocket.CloseNormalClosure:
		msg = "Connection was disconnected by Firehose server. This usually means Nozzle can't keep up " +
			"with server. Please try to scaling out Nozzzle with mulitple instances by using the " +
			"same subscription ID."

	case websocket.ClosePolicyViolation:
		msg = "Nozzle lost the keep-alive heartbeat with Firehose server. Connection was disconnected " +
			"by Firehose server. This usually means either Nozzle was busy with processing events or there " +
			"was some temporary network issue causing the heartbeat to get lost."

	default:
		msg = "Encountered close error while reading from Firehose"
	}

	f.config.Logger.Error(msg, err)
}
