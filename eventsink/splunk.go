package eventsink

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"sync/atomic"

	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/cache"
	fevents "github.com/cloudfoundry-community/splunk-firehose-nozzle/events"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventwriter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/monitoring"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
	"github.com/cloudfoundry/sonde-go/events"
)

const SPLUNK_HEC_FIELDS_SUPPORT_VERSION = "6.4"

type SplunkConfig struct {
	FlushInterval           time.Duration
	QueueSize               int // consumer queue buffer size
	BatchSize               int
	Retries                 int // No of retries to post events to HEC before dropping events
	Hostname                string
	SubscriptionID          string
	ExtraFields             map[string]string
	TraceLogging            bool
	UUID                    string
	Logger                  lager.Logger
	StatusMonitorInterval   time.Duration
	LoggingIndex            string
	RefreshSplunkConnection bool
	KeepAliveTimer          time.Duration
}

type ParseConfig = fevents.Config

type Splunk struct {
	writers               []eventwriter.Writer
	config                *SplunkConfig
	parseConfig           *ParseConfig
	appCache              cache.Cache
	events                chan *events.Envelope
	wg                    sync.WaitGroup
	eventCount            uint64
	sentCountChan         chan uint64
	FirehoseDroppedEvents utils.Counter
	SplunkDroppedEvents   utils.Counter

	// cached IP
	ip string
}

func NewSplunk(writers []eventwriter.Writer, config *SplunkConfig, parseConfig *ParseConfig, appCache cache.Cache) *Splunk {
	hostname, ip, _ := utils.GetHostIPInfo(config.Hostname)
	config.Hostname = hostname
	splunk := &Splunk{
		writers:               writers,
		config:                config,
		parseConfig:           parseConfig,
		appCache:              appCache,
		events:                make(chan *events.Envelope, config.QueueSize),
		ip:                    ip,
		eventCount:            0,
		sentCountChan:         make(chan uint64, 100),
		FirehoseDroppedEvents: monitoring.RegisterCounter("firehose.events.dropped.count", utils.UintType),
		SplunkDroppedEvents:   monitoring.RegisterCounter("splunk.events.dropped.count", utils.UintType),
	}
	monitoring.RegisterFunc("nozzle.queue.percentage", func() interface{} {
		return (float64(len(splunk.events)) / float64(splunk.config.QueueSize) * 100.0)
	})

	return splunk
}

func (s *Splunk) Open() error {
	for _, client := range s.writers[:len(s.writers)-1] {
		s.wg.Add(1)
		go s.consume(client)
	}
	return nil
}

func (s *Splunk) Close() error {
	// Notify the consume loop to drain events and exit
	close(s.events)
	s.wg.Wait()
	return nil
}

// parseEvent parses the event received from the doppler
func (s *Splunk) parseEvent(msg *events.Envelope) map[string]interface{} {
	eventType := msg.GetEventType()

	var event *fevents.Event
	switch eventType {
	case events.Envelope_HttpStartStop:
		event = fevents.HttpStartStop(msg)
	case events.Envelope_LogMessage:
		event = fevents.LogMessage(msg)
	case events.Envelope_ValueMetric:
		event = fevents.ValueMetric(msg)
	case events.Envelope_CounterEvent:
		event = fevents.CounterEvent(msg)
	case events.Envelope_Error:
		event = fevents.ErrorEvent(msg)
	case events.Envelope_ContainerMetric:
		event = fevents.ContainerMetric(msg)
	case events.Envelope_HttpStart:
		event = fevents.HttpStart(msg)
	case events.Envelope_HttpStop:
		event = fevents.HttpStop(msg)

	default:
		return nil
	}

	event.AnnotateWithEnvelopeData(msg, s.parseConfig)
	event.AnnotateWithCFMetaData()

	if _, hasAppId := event.Fields["cf_app_id"]; hasAppId {
		event.AnnotateWithAppData(s.appCache, s.parseConfig)
	}

	if ignored, ok := event.Fields["cf_ignored_app"]; ok {
		if ignoreApp, ok := ignored.(bool); ok && ignoreApp {
			// Ignore events from this app since end user tag to ignore this app
			return nil
		}
	}

	parsedEvent := event.Fields

	if len(event.Msg) > 0 {
		parsedEvent["msg"] = event.Msg
	}

	return parsedEvent
}

func (s *Splunk) Write(fields *events.Envelope) error {
	select {
	case s.events <- fields:
	default:
		s.FirehoseDroppedEvents.Add(1)
	}
	return nil
}

func (s *Splunk) consume(writer eventwriter.Writer) {
	defer s.wg.Done()

	var batch []map[string]interface{}
	timer := time.NewTimer(s.config.FlushInterval)

	// Flush takes place when 1) batch limit is reached. 2) flush window expires
LOOP:
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				// events chan has closed and we have drained all events in it
				break LOOP
			}

			parsedEvent := s.parseEvent(event)
			if parsedEvent != nil {
				finalEvent := s.buildEvent(parsedEvent)
				batch = append(batch, finalEvent)
				if len(batch) >= s.config.BatchSize {
					batch = s.indexEvents(writer, batch)
					timer.Reset(s.config.FlushInterval) // reset channel timer
				}
			}

		case <-timer.C:
			batch = s.indexEvents(writer, batch)
			timer.Reset(s.config.FlushInterval)
		}

	}
	// Last batch
	s.indexEvents(writer, batch)
}

// indexEvents indexes events to Splunk
// return nil when successful which clears all outstanding events
// return what the batch has if there is an error for next retry cycle
func (s *Splunk) indexEvents(writer eventwriter.Writer, batch []map[string]interface{}) []map[string]interface{} {
	if len(batch) == 0 {
		return batch
	}
	var err error
	for i := 0; i < s.config.Retries; i++ {
		err, sentCount := writer.Write(batch)
		if err == nil {
			if s.config.StatusMonitorInterval > time.Second*0 {
				s.sentCountChan <- sentCount
			}
			return nil
		}
		s.config.Logger.Error("Unable to talk to Splunk", err, lager.Data{"Retry attempt": i + 1})
		time.Sleep(getRetryInterval(i))
	}
	s.SplunkDroppedEvents.Add(len(batch))
	s.config.Logger.Error("Finish retrying and dropping events", err, lager.Data{"events": len(batch)})
	return nil
}

func (s *Splunk) buildEvent(fields map[string]interface{}) map[string]interface{} {
	if msg, ok := fields["msg"]; ok {
		if msgStr, ok := msg.(string); ok && len(msgStr) > 0 {
			fields["msg"] = utils.ToJson(msgStr)
		}
	}

	event := map[string]interface{}{}

	var timestamp string
	if val, ok := fields["timestamp"]; ok {
		if v, ok := val.(int64); ok {
			timestamp = utils.NanoSecondsToSeconds(v)
		}
	}

	// Timestamp
	if timestamp == "" {
		timestamp = utils.NanoSecondsToSeconds(time.Now().UnixNano())
	}
	event["time"] = timestamp

	event["host"] = fields["ip"]
	event["source"] = fields["job"]

	if eventType, ok := fields["event_type"].(string); ok {
		event["sourcetype"] = fmt.Sprintf("cf:%s", strings.ToLower(eventType))
	}

	extraFields := make(map[string]interface{})

	if s.config.TraceLogging {
		extraFields["nozzle-event-counter"] = strconv.FormatUint(atomic.AddUint64(&s.eventCount, 1), 10)
		extraFields["firehose-subscription-id"] = s.config.SubscriptionID
		extraFields["uuid"] = s.config.UUID
	}
	for k, v := range s.config.ExtraFields {
		extraFields[k] = v
	}
	event["fields"] = extraFields
	event["event"] = fields
	return event
}

// Log implements lager.Sink required interface
func (s *Splunk) Log(message lager.LogFormat) {
	e := map[string]interface{}{
		"logger_source": message.Source,
		"message":       message.Message,
		"ip":            s.ip,
		"origin":        "splunk_nozzle",
		"log_level":     int(message.LogLevel),
	}

	event := map[string]interface{}{
		"host":       s.config.Hostname,
		"sourcetype": "cf:splunknozzle",
		"event":      e,
	}

	if s.config.LoggingIndex != "" {
		event["index"] = s.config.LoggingIndex
	}

	if message.Timestamp != "" {
		event["time"] = message.Timestamp
	}

	if len(message.Data) > 0 {
		data := map[string]interface{}{}
		for key, value := range message.Data {
			data[key] = value
		}
		e["data"] = data
	}

	events := []map[string]interface{}{event}
	s.writers[len(s.writers)-1].Write(events)
}

func (s *Splunk) LogStatus() {
	timer := time.NewTimer(s.config.StatusMonitorInterval)
	var sent uint64 = 0
	for {
		select {
		case <-timer.C:
			percent := float64(len(s.events)) / float64(s.config.QueueSize) * 100.0
			status := "low"
			switch {
			case percent > 99.9:
				status = "too high"
			case percent > 90:
				status = "high"
			}
			if status != "low" {
				s.config.Logger.Info("Memory_Queue_Pressure", lager.Data{"events_in_consumer_queue": len(s.events), "percentage": int(percent), "status": status})
			}
			sent = 0
			timer.Reset(s.config.StatusMonitorInterval)
		default:
		}
		select {
		case sentCount := <-s.sentCountChan:
			atomic.AddUint64(&sent, sentCount)
		default:
		}
	}
}

func getRetryInterval(attempt int) time.Duration {
	// algorithm taken from https://en.wikipedia.org/wiki/Exponential_backoff
	timeInSec := 5 + (0.5 * (math.Exp2(float64(attempt)) - 1.0))
	return time.Millisecond * time.Duration(1000*timeInSec)
}
