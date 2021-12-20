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
	"code.cloudfoundry.org/lager"
)

type SplunkConfig struct {
	Host    string
	Token   string
	Index   string
	Fields  map[string]string
	SkipSSL bool
	Debug   bool

	Logger lager.Logger
}

type splunkClient struct {
	httpClient *http.Client
	config     *SplunkConfig
}

func NewSplunk(config *SplunkConfig) Writer {
	httpClient := cfhttp.NewClient()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: config.SkipSSL},
	}
	httpClient.Transport = tr

	return &splunkClient{
		httpClient: httpClient,
		config:     config,
	}
}

func (s *splunkClient) Write(events []map[string]interface{}) (error, uint64) {
	bodyBuffer := new(bytes.Buffer)
	count := uint64(len(events))
	for i, event := range events {

		if event["event"].(map[string]interface{})["info_splunk_index"] != nil {
			event["index"] = event["event"].(map[string]interface{})["info_splunk_index"]
		} else if s.config.Index != "" {
			event["index"] = s.config.Index
		}

		if len(s.config.Fields) > 0 {
			event["fields"] = s.config.Fields
		}

		eventJson, err := json.Marshal(event)
		if err == nil {
			bodyBuffer.Write(eventJson)
			if i < len(events)-1 {
				bodyBuffer.Write([]byte("\n\n"))
			}
		} else {
			s.config.Logger.Error("Error marshalling event", err,
				lager.Data{
					"event": fmt.Sprintf("%+v", event),
				},
			)
		}
	}

	if s.config.Debug {
		bodyString := bodyBuffer.String()
		return s.dump(bodyString), count
	} else {
		bodyBytes := bodyBuffer.Bytes()
		return s.send(&bodyBytes), count
	}
}

func (s *splunkClient) send(postBody *[]byte) error {
	endpoint := fmt.Sprintf("%s/services/collector", s.config.Host)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(*postBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Authorization", fmt.Sprintf("Splunk %s", s.config.Token))
	//Add app headers for HEC telemetry
	//Todo: update static values with appName and appVersion variables
	req.Header.Set("__splunk_app_name", "Splunk Firehose Nozzle")
	req.Header.Set("__splunk_app_version", "1.2.4")

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
		io.Copy(ioutil.Discard, resp.Body)
	}

	return nil
}

//To dump the event on stdout instead of Splunk, in case of 'debug' mode
func (s *splunkClient) dump(eventString string) error {
	fmt.Println(string(eventString))

	return nil
}
