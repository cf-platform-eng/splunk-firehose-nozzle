package eventsink_test

import (
	"os"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/cache"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventrouter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
	"github.com/cloudfoundry/sonde-go/events"

	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventsink"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventwriter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/testing"
)

var _ = Describe("Splunk", func() {

	var (
		err           error
		origin        string
		deployment    string
		job           string
		jobIndex      string
		ip            string
		timestampNano int64
		envelope      *events.Envelope
		eventType     events.Envelope_EventType

		memSink            *testing.MemorySinkMock
		sink               *eventsink.Splunk
		sinkLogging        *eventsink.Splunk
		config             *eventsink.SplunkConfig
		configLoggingIndex *eventsink.SplunkConfig

		event      map[string]interface{}
		logger     lager.Logger
		mockClient *testing.EventWriterMock
		// Used for internal logging
		mockClient2 *testing.EventWriterMock
		eventRouter eventrouter.Router

		rconfig *eventrouter.Config
	)

	BeforeEach(func() {
		timestampNano = 1467040874046121775
		deployment = "cf-warden"
		jobIndex = "85c9ff80-e99b-470b-a194-b397a6e73913"
		ip = "10.244.0.22"
		envelope = &events.Envelope{
			Origin:     &origin,
			EventType:  &eventType,
			Timestamp:  &timestampNano,
			Deployment: &deployment,
			Job:        &job,
			Index:      &jobIndex,
			Ip:         &ip,
		}

		//using routing to serialize envelope
		memSink = testing.NewMemorySinkMock()
		rconfig = &eventrouter.Config{
			SelectedEvents: "ContainerMetric, CounterEvent, Error, HttpStart, HttpStartStop, HttpStop, LogMessage, ValueMetric",
		}
		eventRouter, err = eventrouter.New(cache.NewNoCache(), memSink, rconfig)
		Ω(err).ShouldNot(HaveOccurred())

		mockClient = &testing.EventWriterMock{}
		mockClient2 = &testing.EventWriterMock{}

		logger = lager.NewLogger("test")
		config = &eventsink.SplunkConfig{
			FlushInterval: time.Millisecond,
			QueueSize:     1000,
			BatchSize:     1,
			Retries:       1,
			Hostname:      "localhost",
			ExtraFields:   map[string]string{"env": "dev", "test": "field"},
			UUID:          "0a956421-f2e1-4215-9d88-d15633bb3023",
			Logger:        logger,
		}
		configLoggingIndex = &eventsink.SplunkConfig{
			LoggingIndex: "pcf_logs",
		}
		sink = eventsink.NewSplunk([]eventwriter.Writer{mockClient, mockClient2}, config, rconfig, cache.NewNoCache())
		sinkLogging = eventsink.NewSplunk([]eventwriter.Writer{mockClient, mockClient2}, configLoggingIndex, rconfig, cache.NewNoCache())
	})
	Context("When LogStatus is executed", func() {
		BeforeEach(func() {
			config.StatusMonitorInterval = time.Second * 1
			flushInterval := time.Second * 2
			config.FlushInterval = flushInterval
			file, _ := os.OpenFile("lager.log", os.O_CREATE|os.O_RDWR, 0600)
			loggerSink := lager.NewReconfigurableSink(lager.NewWriterSink(file, lager.DEBUG), lager.DEBUG)
			myLogger := lager.NewLogger("LogStatus")
			myLogger.RegisterSink(loggerSink)
			config.Logger = myLogger
			defer file.Close()
			go sink.LogStatus()
			// low pressure
			for i := 0; i < int(float64(config.QueueSize)*0.12); i++ {
				sink.Write(envelope)
			}
			// medium pressure
			time.Sleep(flushInterval)
			for i := 0; i < int(float64(config.QueueSize)*0.40); i++ {
				sink.Write(envelope)
			}
			time.Sleep(flushInterval)
			// high pressure
			for i := 0; i < int(float64(config.QueueSize)*0.40); i++ {
				sink.Write(envelope)
			}
			time.Sleep(flushInterval)
			// too high pressure
			for i := 0; i < int(float64(config.QueueSize)*0.08); i++ {
				sink.Write(envelope)
			}
			time.Sleep(flushInterval)
		})

		It("tests pressure status", func() {
			data, _ := os.ReadFile("lager.log")
			log := string(data)
			Expect(log).Should(ContainSubstring("status\":\"too high"))
			Expect(log).Should(ContainSubstring("status\":\"high"))
			os.Remove("lager.log")
		})
	})

	It("sends events to client", func() {
		eventType = events.Envelope_Error
		eventRouter.Route(envelope)

		sink.Open()
		sink.Write(memSink.Events[0])

		Eventually(func() []map[string]interface{} {
			return mockClient.CapturedEvents()
		}).Should(HaveLen(1))
	})

	It("does not block when downstream is blocked", func() {

		config := &eventsink.SplunkConfig{
			FlushInterval: time.Millisecond,
			QueueSize:     1,
			BatchSize:     1,
			Retries:       1,
			Hostname:      "localhost",
			ExtraFields:   map[string]string{"env": "dev", "test": "field"},
			UUID:          "0a956421-f2e1-4215-9d88-d15633bb3023",
			Logger:        logger,
		}
		sink = eventsink.NewSplunk([]eventwriter.Writer{mockClient, mockClient2}, config, rconfig, cache.NewNoCache())
		sink.FirehoseDroppedEvents = new(utils.IntCounter)
		eventType = events.Envelope_Error
		eventRouter.Route(envelope)
		eventRouter.Route(envelope)
		mockClient.Block = true
		mockClient2.Block = true

		sink.Open()
		sink.Write(memSink.Events[0])
		sink.Write(memSink.Events[1])

		Eventually(func() []map[string]interface{} {
			return mockClient.CapturedEvents()
		}).Should(HaveLen(1))
		Expect(sink.FirehoseDroppedEvents.Value()).To(Equal(uint64(1)))
	})

	It("job_index is present, index is not", func() {
		eventType = events.Envelope_Error
		eventRouter.Route(envelope)

		sink.Open()
		sink.Write(memSink.Events[0])

		Eventually(func() []map[string]interface{} {
			return mockClient.CapturedEvents()
		}).Should(HaveLen(1))

		event = mockClient.CapturedEvents()[0]

		data := event["event"].(map[string]interface{})
		Expect(data).NotTo(HaveKey("index"))

		index := data["job_index"]
		Expect(index).To(Equal(jobIndex))
	})

	Context("envelope HttpStartStop", func() {
		var envelopeHttpStartStop *events.HttpStartStop
		var startTimestamp, stopTimestamp int64
		var requestId events.UUID
		var peerType events.PeerType
		var method events.Method
		var uri, remoteAddress, userAgent string
		var statusCode int32
		var contentLength int64
		var applicationId events.UUID
		var instanceIndex int32
		var instanceId string
		var forwarded []string

		BeforeEach(func() {
			startTimestamp = 1467143062034348090
			stopTimestamp = 1467143062042890400
			peerType = events.PeerType_Server
			method = events.Method_GET
			uri = "http://app-node-express.bosh-lite.com/"
			remoteAddress = "10.244.0.34:45334"
			userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
			statusCode = 200
			contentLength = 23
			instanceIndex = 1
			instanceId = "055a847afbb146f78fdcebf6b2a0067bef2394a07fd34d06a3c3e0811aa966ee"
			forwarded = []string{"hello"}

			requestIdLow := uint64(17459518436806699697)
			requestIdHigh := uint64(17377260946761993045)
			requestId = events.UUID{
				Low:  &requestIdLow,
				High: &requestIdHigh,
			}

			applicationIdLow := uint64(10539615360601842564)
			applicationIdHigh := uint64(3160954123591206558)
			applicationId = events.UUID{
				Low:  &applicationIdLow,
				High: &applicationIdHigh,
			}

			envelopeHttpStartStop = &events.HttpStartStop{
				StartTimestamp: &startTimestamp,
				StopTimestamp:  &stopTimestamp,
				RequestId:      &requestId,
				PeerType:       &peerType,
				Method:         &method,
				Uri:            &uri,
				RemoteAddress:  &remoteAddress,
				UserAgent:      &userAgent,
				StatusCode:     &statusCode,
				ContentLength:  &contentLength,
				ApplicationId:  &applicationId,
				InstanceIndex:  &instanceIndex,
				InstanceId:     &instanceId,
				Forwarded:      forwarded,
			}

			job = "runner_z1"
			origin = "gorouter"
			eventType = events.Envelope_HttpStartStop
			envelope.HttpStartStop = envelopeHttpStartStop

		})

		BeforeEach(func() {
			eventRouter.Route(envelope)

			sink.Open()
			sink.Write(memSink.Events[0])

			Eventually(func() []map[string]interface{} {
				return mockClient.CapturedEvents()
			}).Should(HaveLen(1))

			event = mockClient.CapturedEvents()[0]
		})

		It("metadata", func() {
			Expect(event["host"]).To(Equal(ip))
			Expect(event["source"]).To(Equal(job))
			Expect(event["sourcetype"]).To(Equal("cf:httpstartstop"))
		})

		It("uses current time without event timestamp", func() {
			eventTime, err := strconv.ParseFloat(event["time"].(string), 64)

			Expect(err).To(BeNil())
			Expect(time.Now().Unix()).To(BeNumerically("~", eventTime, 2))
		})

		It("adds fields to payload.event", func() {
			eventContents := event["event"].(map[string]interface{})

			Expect(eventContents["method"]).To(Equal("GET"))
			Expect(eventContents["remote_addr"]).To(Equal(remoteAddress))
		})
	})

	Context("envelope LogMessage", func() {
		var message []byte
		var messageType events.LogMessage_MessageType
		var timestamp int64
		var appId, sourceType, sourceInstance string
		var envelopeLogMessage *events.LogMessage

		BeforeEach(func() {
			message = []byte("App debug log message")
			messageType = events.LogMessage_OUT
			timestamp = 1467128185055072010
			appId = "8463ec45-543c-4492-9ec6-f52707f7dd2b"
			sourceType = "App"
			sourceInstance = "0"
			envelopeLogMessage = &events.LogMessage{
				Message:        message,
				MessageType:    &messageType,
				Timestamp:      &timestamp,
				AppId:          &appId,
				SourceType:     &sourceType,
				SourceInstance: &sourceInstance,
			}

			job = "runner_z1"
			origin = "dea_logging_agent"
			eventType = events.Envelope_LogMessage
			envelope.LogMessage = envelopeLogMessage
		})

		BeforeEach(func() {
			eventRouter.Route(envelope)

			sink.Open()
			sink.Write(memSink.Events[0])

			Eventually(func() []map[string]interface{} {
				return mockClient.CapturedEvents()
			}).Should(HaveLen(1))

			event = mockClient.CapturedEvents()[0]
		})

		It("metadata", func() {
			Expect(event["host"]).To(Equal(ip))
			Expect(event["source"]).To(Equal(job))
			Expect(event["sourcetype"]).To(Equal("cf:logmessage"))
		})

		It("uses event timestamp", func() {
			eventTimeSeconds := "1467128185.055072010"
			Expect(event["time"]).To(Equal(eventTimeSeconds))
		})

		It("adds fields to payload.event", func() {
			eventContents := event["event"].(map[string]interface{})

			Expect(eventContents["cf_app_id"]).To(Equal(appId))
			Expect(eventContents["message_type"]).To(Equal("OUT"))
		})
	})

	Context("envelope ValueMetric", func() {
		var name, unit string
		var value float64
		var envelopeValueMetric *events.ValueMetric

		BeforeEach(func() {
			name = "ms_since_last_registry_update"
			value = 1581.0
			unit = "ms"
			envelopeValueMetric = &events.ValueMetric{
				Name:  &name,
				Value: &value,
				Unit:  &unit,
			}

			job = "router_z1"
			origin = "MetronAgent"
			eventType = events.Envelope_ValueMetric
			envelope.ValueMetric = envelopeValueMetric
		})

		BeforeEach(func() {
			eventRouter.Route(envelope)

			sink.Open()
			sink.Write(memSink.Events[0])

			Eventually(func() []map[string]interface{} {
				return mockClient.CapturedEvents()
			}).Should(HaveLen(1))

			event = mockClient.CapturedEvents()[0]
		})

		It("metadata", func() {
			Expect(event["host"]).To(Equal(ip))
			Expect(event["source"]).To(Equal(job))
			Expect(event["sourcetype"]).To(Equal("cf:valuemetric"))
		})

		It("uses current time without event timestamp", func() {
			eventTime, err := strconv.ParseFloat(event["time"].(string), 64)

			Expect(err).To(BeNil())
			Expect(time.Now().Unix()).To(BeNumerically("~", eventTime, 2))
		})

		It("adds fields to payload.event", func() {
			eventContents := event["event"].(map[string]interface{})

			Expect(eventContents["unit"]).To(Equal("ms"))
			Expect(eventContents["value"]).To(Equal(1581.0))
		})
	})

	Context("envelope CounterEvent", func() {
		var name string
		var delta, total uint64
		var counterEvent *events.CounterEvent

		BeforeEach(func() {
			name = "registry_message.uaa"
			delta = 1
			total = 8196
			counterEvent = &events.CounterEvent{
				Name:  &name,
				Delta: &delta,
				Total: &total,
			}

			job = "router_z1"
			origin = "gorouter"
			eventType = events.Envelope_CounterEvent
			envelope.CounterEvent = counterEvent
		})

		BeforeEach(func() {
			eventRouter.Route(envelope)

			sink.Open()
			sink.Write(memSink.Events[0])

			Eventually(func() []map[string]interface{} {
				return mockClient.CapturedEvents()
			}).Should(HaveLen(1))

			event = mockClient.CapturedEvents()[0]
		})

		It("metadata", func() {
			Expect(event["host"]).To(Equal(ip))
			Expect(event["source"]).To(Equal(job))
			Expect(event["sourcetype"]).To(Equal("cf:counterevent"))
		})

		It("uses current time without event timestamp", func() {
			eventTime, err := strconv.ParseFloat(event["time"].(string), 64)

			Expect(err).To(BeNil())
			Expect(time.Now().Unix()).To(BeNumerically("~", eventTime, 2))
		})

		It("adds fields to payload.event", func() {
			eventContents := event["event"].(map[string]interface{})

			Expect(eventContents["origin"]).To(Equal("gorouter"))
			Expect(eventContents["name"]).To(Equal("registry_message.uaa"))
		})
	})

	Context("envelope Error", func() {
		var source, message string
		var code int32
		var envelopeError *events.Error

		BeforeEach(func() {
			source = "some_source"
			message = "Something failed"
			code = 42
			envelopeError = &events.Error{
				Source:  &source,
				Code:    &code,
				Message: &message,
			}

			job = "router_z1"
			origin = "Unknown"
			eventType = events.Envelope_Error
			envelope.Error = envelopeError
		})

		BeforeEach(func() {
			eventRouter.Route(envelope)

			sink.Open()
			sink.Write(memSink.Events[0])

			Eventually(func() []map[string]interface{} {
				return mockClient.CapturedEvents()
			}).Should(HaveLen(1))

			event = mockClient.CapturedEvents()[0]
		})

		It("metadata", func() {
			Expect(event["host"]).To(Equal(ip))
			Expect(event["source"]).To(Equal(job))
			Expect(event["sourcetype"]).To(Equal("cf:error"))
		})

		It("uses current time without event timestamp", func() {
			eventTime, err := strconv.ParseFloat(event["time"].(string), 64)

			Expect(err).To(BeNil())
			Expect(time.Now().Unix()).To(BeNumerically("~", eventTime, 2))
		})

		It("adds fields to payload.event", func() {
			eventContents := event["event"].(map[string]interface{})

			Expect(eventContents["code"]).To(BeNumerically("==", 42))
		})
	})

	Context("envelope ContainerMetric", func() {
		var applicationId string
		var instanceIndex int32
		var cpuPercentage float64
		var memoryBytes, diskBytes uint64
		var containerMetric *events.ContainerMetric

		BeforeEach(func() {
			applicationId = "8463ec45-543c-4492-9ec6-f52707f7dd2b"
			instanceIndex = 1
			cpuPercentage = 1.0916583859477904
			memoryBytes = 30011392
			diskBytes = 15005696
			containerMetric = &events.ContainerMetric{
				ApplicationId: &applicationId,
				InstanceIndex: &instanceIndex,
				CpuPercentage: &cpuPercentage,
				MemoryBytes:   &memoryBytes,
				DiskBytes:     &diskBytes,
			}

			job = "runner_z1"
			origin = "DEA"
			eventType = events.Envelope_ContainerMetric
			envelope.ContainerMetric = containerMetric
		})

		BeforeEach(func() {
			eventRouter.Route(envelope)

			sink.Open()
			sink.Write(memSink.Events[0])

			Eventually(func() []map[string]interface{} {
				return mockClient.CapturedEvents()
			}).Should(HaveLen(1))

			event = mockClient.CapturedEvents()[0]
		})

		It("metadata", func() {
			Expect(event["host"]).To(Equal(ip))
			Expect(event["source"]).To(Equal(job))
			Expect(event["sourcetype"]).To(Equal("cf:containermetric"))
		})

		It("uses current time without event timestamp", func() {
			eventTime, err := strconv.ParseFloat(event["time"].(string), 64)

			Expect(err).To(BeNil())
			Expect(time.Now().Unix()).To(BeNumerically("~", eventTime, 2))
		})

		It("adds fields to payload.event", func() {
			eventContents := event["event"].(map[string]interface{})

			Expect(eventContents["cpu_percentage"]).To(Equal(cpuPercentage))
			Expect(eventContents["memory_bytes"]).To(Equal(memoryBytes))
		})
	})

	It("Writer error, retry", func() {
		eventType = events.Envelope_Error
		eventRouter.Route(envelope)

		mockClient.ReturnErr = true

		err := sink.Open()
		Ω(err).ShouldNot(HaveOccurred())
		err = sink.Write(memSink.Events[0])
		Ω(err).ShouldNot(HaveOccurred())
		time.Sleep(time.Second)

		sink.Close()
	})

	It("Close no error", func() {
		eventType = events.Envelope_Error
		eventRouter.Route(envelope)

		err := sink.Open()
		Ω(err).ShouldNot(HaveOccurred())
		err = sink.Write(memSink.Events[0])
		Ω(err).ShouldNot(HaveOccurred())
		time.Sleep(time.Second)

		err = sink.Close()
		Ω(err).ShouldNot(HaveOccurred())
	})

	// lager.Logger interface
	It("posts to splunk", func() {
		message := lager.LogFormat{}

		Expect(mockClient2.CapturedEvents()).To(BeNil())

		sink.Log(message)
		sink.Log(message)

		Expect(mockClient2.CapturedEvents()).To(HaveLen(2))
	})

	It("translates log message metadata to splunk format", func() {
		message := lager.LogFormat{
			Timestamp: "1473180363",
			Source:    "splunk-nozzle-logger",
			Message:   "Failure",
			LogLevel:  lager.ERROR,
		}

		sink.Log(message)

		Expect(mockClient2.CapturedEvents()).To(HaveLen(1))
		envelope := mockClient2.CapturedEvents()[0]

		Expect(envelope["time"]).To(Equal("1473180363"))

		event := envelope["event"].(map[string]interface{})
		Expect(event["logger_source"]).To(Equal("splunk-nozzle-logger"))
		Expect(event["log_level"]).To(Equal(2))

	})

	It("translates log message payload to splunk format", func() {
		message := lager.LogFormat{
			Timestamp: "1473180363",
			Source:    "splunk-nozzle-logger",
			Message:   "Failure",
			LogLevel:  lager.ERROR,
			Data: lager.Data{
				"foo": "bar",
				"baz": 42,
			},
		}

		sink.Log(message)

		Expect(mockClient2.CapturedEvents()).To(HaveLen(1))
		envelope := mockClient2.CapturedEvents()[0]

		Expect(envelope["time"]).To(Equal("1473180363"))

		event := envelope["event"].(map[string]interface{})
		Expect(event["message"]).To(Equal("Failure"))

		data := event["data"].(map[string]interface{})
		Expect(data["foo"]).To(Equal("bar"))
		Expect(data["baz"]).To(Equal(42))

	})

	It("emit log event without logging index", func() {
		message := lager.LogFormat{}

		sink.Log(message)

		Expect(mockClient2.CapturedEvents()).To(HaveLen(1))
		event := mockClient2.CapturedEvents()[0]
		Expect(event["index"]).To(BeNil())

	})

	It("emit log event with logging index", func() {
		message := lager.LogFormat{}

		sinkLogging.Log(message)
		Expect(mockClient2.CapturedEvents()).To(HaveLen(1))
		event := mockClient2.CapturedEvents()[0]
		Expect(event["index"]).To(Equal("pcf_logs"))

	})

	It("adds expected Splunk fields", func() {
		message := lager.LogFormat{}

		sink.Log(message)

		Expect(mockClient2.CapturedEvents()).To(HaveLen(1))
		envelope := mockClient2.CapturedEvents()[0]

		Expect(envelope["sourcetype"]).To(Equal("cf:splunknozzle"))
		Expect(envelope["host"]).ToNot(BeEmpty())

		event := envelope["event"].(map[string]interface{})
		Expect(event["ip"]).ToNot(BeEmpty())
		Expect(event["origin"]).To(Equal("splunk_nozzle"))
	})

	It("std no error", func() {
		s := &eventsink.Std{}
		err := s.Open()
		Ω(err).ShouldNot(HaveOccurred())

		err = s.Write(nil)
		Ω(err).ShouldNot(HaveOccurred())

		err = s.Close()
		Ω(err).ShouldNot(HaveOccurred())
	})

})
