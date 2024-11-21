package eventwriter_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"

	// "time"

	"code.cloudfoundry.org/lager/v3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-community/splunk-firehose-nozzle/eventwriter"
)

var _ = Describe("Splunk", func() {
	var (
		testServer      *httptest.Server
		capturedRequest *http.Request
		capturedBody    []byte
		splunkResponse  []byte
		logger          lager.Logger
		config          *SplunkConfig
	)

	BeforeEach(func() {
		logger = lager.NewLogger("test")
		config = &SplunkConfig{
			Token:   "token",
			Index:   "metric",
			Fields:  nil,
			SkipSSL: true,
			Logger:  logger,
		}
	})

	Context("success response", func() {
		BeforeEach(func() {
			capturedRequest = nil

			splunkResponse = []byte("{}")
			testServer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				capturedRequest = request
				body, err := ioutil.ReadAll(request.Body)
				if err != nil {
					panic(err)
				}
				capturedBody = body

				writer.Write(splunkResponse)
			}))

			config.Host = testServer.URL
		})

		AfterEach(func() {
			testServer.Close()
		})

		It("correctly authenticates requests", func() {
			tokenValue := "abc-some-random-token"
			config.Token = tokenValue

			client := NewSplunkMetric(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			authValue := capturedRequest.Header.Get("Authorization")
			expectedAuthValue := fmt.Sprintf("Splunk %s", tokenValue)

			Expect(authValue).To(Equal(expectedAuthValue))
		})

		It("sets content type to json", func() {
			client := NewSplunkMetric(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			contentType := capturedRequest.Header.Get("Content-Type")
			Expect(contentType).To(Equal("application/json"))
		})

		It("sets app name to appName", func() {
			appName := "Splunk Firehose Nozzle"

			client := NewSplunkMetric(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			applicationName := capturedRequest.Header.Get("__splunk_app_name")
			Expect(applicationName).To(Equal(appName))

		})

		It("sets app appVersion", func() {
			appVersion := "1.2.5"
			config.Version = "1.2.5"

			client := NewSplunkMetric(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			applicationVersion := capturedRequest.Header.Get("__splunk_app_version")
			Expect(applicationVersion).To(Equal(appVersion))

		})

		It("Writes batch event json", func() {
			client := NewSplunkMetric(config)
			event1 := map[string]interface{}{
				"metric_name:firehose.events.dropped.count":  0,
				"metric_name:firehose.events.received.count": 1108,
				"metric_name:nozzle.queue.percentage":        0,
				"metric_name:splunk.events.dropped.count":    0,
				"metric_name:splunk.events.sent.count":       44,
				"metric_name:splunk.events.throughput":       788110,
			}
			events := []map[string]interface{}{event1}
			os.Setenv("INSTANCE_INDEX", "2")
			err, sentCount := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())
			Expect(sentCount).To(Equal(uint64(1)))

			expectedPayload := strings.TrimSpace(`
			{"event":"metric","fields":{"index": "metric","metric_name:firehose.events.dropped.count":0,"metric_name:firehose.events.received.count":1108,"metric_name:nozzle.queue.percentage":0,"metric_name:splunk.events.dropped.count":0,"metric_name:splunk.events.sent.count":44,"metric_name:splunk.events.throughput":788110
			},"index":"metric","time":"1664279537.396609918"}`)
			var expedtedjsonMap map[string]interface{}
			expectedPayload = expectedPayload + "\n\n"
			var capturedjsonMap map[string]interface{}
			json.Unmarshal([]byte(expectedPayload), &expedtedjsonMap)
			json.Unmarshal(capturedBody, &capturedjsonMap)
			Expect(capturedjsonMap).To(Equal(expedtedjsonMap["fields"]))
		})

		It("Writes to correct endpoint", func() {
			client := NewSplunkMetric(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest.URL.Path).To(Equal("/services/collector"))
		})

	})

	It("returns error on bad splunk host", func() {
		config.Host = ":"
		client := NewSplunkMetric(config)
		events := []map[string]interface{}{}
		err, _ := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("protocol"))
	})

	It("Returns error on non-2xx response", func() {
		testServer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(500)
			writer.Write([]byte("Internal server error"))
		}))

		config.Host = testServer.URL
		client := NewSplunkMetric(config)
		events := []map[string]interface{}{}
		err, _ := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("500"))
	})

	It("Returns error from http client", func() {
		config.Host = "foo://example.com"
		client := NewSplunkMetric(config)
		events := []map[string]interface{}{}
		err, _ := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("foo"))
	})
})
