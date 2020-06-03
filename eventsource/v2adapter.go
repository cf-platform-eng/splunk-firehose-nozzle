package eventsource

import (
	"code.cloudfoundry.org/go-loggregator/v8"
	"code.cloudfoundry.org/go-loggregator/v8/conversion"
	"code.cloudfoundry.org/go-loggregator/v8/rpc/loggregator_v2"
	"context"
	"github.com/cloudfoundry/sonde-go/events"
)

// Streamer implements Stream which returns a new EnvelopeStream for the given context and request.
type Streamer interface {
	// EnvelopeStream returns batches of envelopes.
	Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream
}

// V2Adapter struct with field of type streamer
type V2Adapter struct {
	streamer Streamer
}

// NewV2Adapter returns v2Adapter
func NewV2Adapter(s Streamer) V2Adapter {
	return V2Adapter{
		streamer: s,
	}
}

// Firehose returns only selected event stream
func (a V2Adapter) Firehose(config *FirehoseConfig) chan *events.Envelope {
	ctx := context.Background()
	var v1msgs = make(chan *events.Envelope, 10000)
	var v2msgs = make(chan *loggregator_v2.Envelope, 10000)
	es := a.streamer.Stream(ctx, &loggregator_v2.EgressBatchRequest{
		ShardId: config.SubscriptionID,
		Selectors: []*loggregator_v2.Selector{
			{
				Message: &loggregator_v2.Selector_Log{
					Log: &loggregator_v2.LogSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Counter{
					Counter: &loggregator_v2.CounterSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Event{
					Event: &loggregator_v2.EventSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Gauge{
					Gauge: &loggregator_v2.GaugeSelector{},
				},
			},
			{
				Message: &loggregator_v2.Selector_Timer{
					Timer: &loggregator_v2.TimerSelector{},
				},
			},
		},
	})

	go func() {
		for ctx.Err() == nil {
			for _, e := range es() {
				v2msgs <- e
			}
		}
	}()

	go func() {
		for ctx.Err() == nil {
			e := <-v2msgs
			//// ToV1 converts v2 envelopes down to v1 envelopes.
			for _, v1e := range conversion.ToV1(e) {
				v1msgs <- v1e
			}
		}
	}()

	return v1msgs
}
