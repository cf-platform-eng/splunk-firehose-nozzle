package splunk_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"

	"code.cloudfoundry.org/lager"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-community/splunk-firehose-nozzle/splunk"
)

var _ = Describe("SplunkClient", func() {
	var (
		testServer      *httptest.Server
		capturedRequest *http.Request
		capturedBody    []byte
		splunkResponse  []byte
		logger          lager.Logger
	)

	BeforeEach(func() {
		logger = lager.NewLogger("test")
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
		})

		AfterEach(func() {
			testServer.Close()
		})

		It("correctly authenticates requests", func() {
			tokenValue := "abc-some-random-token"
			client := New(tokenValue, testServer.URL, "", nil, true, logger)
			events := []map[string]interface{}{}
			err := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			authValue := capturedRequest.Header.Get("Authorization")
			expectedAuthValue := fmt.Sprintf("Splunk %s", tokenValue)

			Expect(authValue).To(Equal(expectedAuthValue))
		})

		It("sets content type to json", func() {
			client := New("token", testServer.URL, "", nil, true, logger)
			events := []map[string]interface{}{}
			err := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			contentType := capturedRequest.Header.Get("Content-Type")
			Expect(contentType).To(Equal("application/json"))
		})

		It("Writes batch event json", func() {
			client := New("token", testServer.URL, "", nil, true, logger)
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
			err := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"}}

{"event":{"greeting":"hello mars"}}

{"event":{"greeting":"hello pluto"}}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))
		})

		It("sets index in splunk payload", func() {
			client := New("token", testServer.URL, "index_cf", nil, true, logger)
			event1 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello world",
			}}
			event2 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello mars",
			}}

			events := []map[string]interface{}{event1, event2}
			err := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"},"index":"index_cf"}

{"event":{"greeting":"hello mars"},"index":"index_cf"}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))
		})

		It("adds fields to splunk palylaod", func() {
			fields := map[string]string{
				"foo":   "bar",
				"hello": "world",
			}

			client := New("token", testServer.URL, "", fields, true, logger)
			event1 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello world",
			}}
			event2 := map[string]interface{}{"event": map[string]interface{}{
				"greeting": "hello mars",
			}}

			events := []map[string]interface{}{event1, event2}
			err := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest).NotTo(BeNil())

			expectedPayload := strings.TrimSpace(`
{"event":{"greeting":"hello world"},"fields":{"foo":"bar","hello":"world"}}

{"event":{"greeting":"hello mars"},"fields":{"foo":"bar","hello":"world"}}
`)
			Expect(string(capturedBody)).To(Equal(expectedPayload))

		})

		It("Writes to correct endpoint", func() {
			client := New("token", testServer.URL, "", nil, true, logger)
			events := []map[string]interface{}{}
			err := client.Write(events)

			Expect(err).To(BeNil())
			Expect(capturedRequest.URL.Path).To(Equal("/services/collector"))
		})
	})

	It("returns error on bad splunk host", func() {
		client := New("token", ":", "", nil, true, logger)
		events := []map[string]interface{}{}
		err := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("protocol"))
	})

	It("Returns error on non-2xx response", func() {
		testServer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(500)
			writer.Write([]byte("Internal server error"))
		}))

		client := New("token", testServer.URL, "", nil, true, logger)
		events := []map[string]interface{}{}
		err := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("500"))
	})

	It("Returns error from http client", func() {
		client := New("token", "foo://example.com", "", nil, true, logger)
		events := []map[string]interface{}{}
		err := client.Write(events)

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("foo"))
	})
})
