package eventwriter

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"code.cloudfoundry.org/cfhttp"
	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
)

var keepAliveTimer = time.Now()

type SplunkConfig struct {
	Host                    string
	Token                   string
	Index                   string
	Fields                  map[string]string
	SkipSSL                 bool
	Debug                   bool
	Version                 string
	RefreshSplunkConnection bool
	KeepAliveTimer          time.Duration

	Logger lager.Logger
}

type SplunkEvent struct {
	httpClient     *http.Client
	config         *SplunkConfig
	BodyBufferSize utils.Counter
	SentEventCount utils.Counter
}

func NewSplunkEvent(config *SplunkConfig) Writer {
	httpClient := cfhttp.NewClient()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: config.SkipSSL, MinVersion: tls.VersionTLS12},
	}
	httpClient.Transport = tr

	return &SplunkEvent{
		httpClient:     httpClient,
		config:         config,
		BodyBufferSize: &utils.NopCounter{},
		SentEventCount: &utils.NopCounter{},
	}
}

func (s *SplunkEvent) Write(events []map[string]interface{}) (error, uint64) {
	bodyBuffer := new(bytes.Buffer)
	count := uint64(len(events))
	for i, event := range events {
		s.parseEvent(&event)

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
		s.SentEventCount.Add(count)
		return s.send(&bodyBytes), count
	}
}

func (s *SplunkEvent) parseEvent(event *map[string]interface{}) error {
	if _, ok := (*event)["index"]; !ok {
		if (*event)["event"].(map[string]interface{})["info_splunk_index"] != nil {
			(*event)["index"] = (*event)["event"].(map[string]interface{})["info_splunk_index"]
		} else if s.config.Index != "" {
			(*event)["index"] = s.config.Index
		}
	}

	if len(s.config.Fields) > 0 {
		(*event)["fields"] = s.config.Fields
	}

	return nil
}

func (s *SplunkEvent) send(postBody *[]byte) error {
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
		responseBody, _ := io.ReadAll(resp.Body)
		return errors.New(fmt.Sprintf("Non-ok response code [%d] from splunk: %s", resp.StatusCode, responseBody))
	} else {
		if s.config.RefreshSplunkConnection && time.Now().After(keepAliveTimer) {
			if s.config.KeepAliveTimer > 0 {
				keepAliveTimer = time.Now().Add(s.config.KeepAliveTimer)
			}
		} else {
			//Draining the response buffer, so that the same connection can be reused the next time
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				s.config.Logger.Error("Error discarding response body", err)
			}
		}
	}
	s.BodyBufferSize.Add(uint64(len(*postBody)))

	return nil
}

// To dump the event on stdout instead of Splunk, in case of 'debug' mode
func (s *SplunkEvent) dump(eventString string) error {
	fmt.Println(string(eventString))

	return nil
}
