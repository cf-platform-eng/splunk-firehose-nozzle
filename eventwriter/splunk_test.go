package eventwriter_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

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
			Index:   "",
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
				body, err := io.ReadAll(request.Body)
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

			client := NewSplunkEvent(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			authValue := capturedRequest.Header.Get("Authorization")
			expectedAuthValue := fmt.Sprintf("Splunk %s", tokenValue)

			Expect(authValue).To(Equal(expectedAuthValue))
		})

		It("sets content type to json", func() {
			client := NewSplunkEvent(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			contentType := capturedRequest.Header.Get("Content-Type")
			Expect(contentType).To(Equal("application/json"))
		})

		It("sets app name to appName", func() {
			appName := "Splunk Firehose Nozzle"

			client := NewSplunkEvent(config)
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

			client := NewSplunkEvent(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			applicationVersion := capturedRequest.Header.Get("__splunk_app_version")
			Expect(applicationVersion).To(Equal(appVersion))

		})

		It("Writes batch event json", func() {
			client := NewSplunkEvent(config)
			event1 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello world",
			}}
			event2 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello mars",
			}}
			event3 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello pluto",
			}}

			events := []map[string]interface{}{event1, event2, event3}
			err, sentCount := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())
			Expect(sentCount).To(Equal(uint64(3)))

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"}}

{"event":{"greeting":"hello mars"}}

{"event":{"greeting":"hello pluto"}}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))
		})

		It("sets index in splunk payload", func() {
			config.Index = "index_cf"
			client := NewSplunkEvent(config)
			event1 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello world",
			}}
			event2 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello mars",
			}}

			events := []map[string]interface{}{event1, event2}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"},"index":"index_cf"}

{"event":{"greeting":"hello mars"},"index":"index_cf"}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))
		})

		It("doesn't change index as it's already set", func() {
			config.Index = "index_cf"
			client := NewSplunkEvent(config)
			event1 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello world",
			}}
			event1["index"] = "index_logs"
			event2 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello mars",
			}}

			events := []map[string]interface{}{event1, event2}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"},"index":"index_logs"}

{"event":{"greeting":"hello mars"},"index":"index_cf"}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))
		})

		It("adds fields to splunk payload", func() {
			fields := map[string]string{
				"foo":   "bar",
				"hello": "world",
			}
			config.Fields = fields

			client := NewSplunkEvent(config)
			event1 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello world",
			}}
			event2 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello mars",
			}}

			events := []map[string]interface{}{event1, event2}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"},"fields":{"foo":"bar","hello":"world"}}

{"event":{"greeting":"hello mars"},"fields":{"foo":"bar","hello":"world"}}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))

		})

		It("Writes to correct endpoint", func() {
			client := NewSplunkEvent(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest.URL.Path).To(Equal("/services/collector"))
		})

		It("Writes to stdout in debug without error", func() {
			config.Debug = true
			client := NewSplunkEvent(config)
			events := []map[string]interface{}{}
			err, _ := client.Write(events)

			Expect(err).To(BeNil())
		})
	})

	It("returns error on bad splunk host", func() {
		config.Host = ":"
		client := NewSplunkEvent(config)
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
		client := NewSplunkEvent(config)
		events := []map[string]interface{}{}
		err, _ := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("500"))
	})

	It("Returns error from http client", func() {
		config.Host = "foo://example.com"
		client := NewSplunkEvent(config)
		events := []map[string]interface{}{}
		err, _ := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("foo"))
	})
})
