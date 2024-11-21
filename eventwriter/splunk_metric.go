package eventwriter

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/lager/v3"
)

type splunkMetric struct {
	httpClient *http.Client
	config     *SplunkConfig
}

func NewSplunkMetric(config *SplunkConfig) Writer {
	httpClient := cfhttp.NewClient()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: config.SkipSSL},
	}
	httpClient.Transport = tr

	return &splunkMetric{
		httpClient: httpClient,
		config:     config,
	}
}

func (s *splunkMetric) Write(events []map[string]interface{}) (error, uint64) {

	bodyBuffer := new(bytes.Buffer)
	count := uint64(len(events))
	for _, event := range events {
		event["index"] = s.config.Index
		eventJson, err := json.Marshal(event)
		if err == nil {
			bodyBuffer.Write(eventJson)
			bodyBuffer.Write([]byte("\n\n"))
		} else {
			s.config.Logger.Error("Error marshalling event", err,
				lager.Data{
					"event": fmt.Sprintf("%+v", event),
				},
			)
		}
	}

	bodyBytes := bodyBuffer.Bytes()
	return s.send(&bodyBytes), count
}

func (s *splunkMetric) send(postBody *[]byte) error {
	endpoint := fmt.Sprintf("%s/services/collector", s.config.Host)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(*postBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Authorization", fmt.Sprintf("Splunk %s", s.config.Token))
	//Add app headers for HEC telemetry
	req.Header.Set("__splunk_app_name", "Splunk Firehose Nozzle")
	req.Header.Set("__splunk_app_version", s.config.Version)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		return errors.New(fmt.Sprintf("Non-ok response code [%d] from splunk: %s", resp.StatusCode, responseBody))
	} else {
		//Draining the response buffer, so that the same connection can be reused the next time
		_, err := io.Copy(ioutil.Discard, resp.Body)
		if err != nil {
			s.config.Logger.Error("Error discarding response body", err)
		}
	}

	return nil
}
