package splunknozzle

import (
	"context"
	"fmt"
	"github.com/cloudfoundry/go-cfclient/v3/resource"
	"os"
	"strings"
	"time"

	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/cache"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventrouter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/events"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventsink"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventsource"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventwriter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/monitoring"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
	"github.com/cloudfoundry/go-cfclient/v3/client"
	"github.com/cloudfoundry/go-cfclient/v3/config"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"

	"github.com/cloudfoundry-community/splunk-firehose-nozzle/nozzle"
	"github.com/google/uuid"
)

type SplunkFirehoseNozzle struct {
	config *Config
	logger lager.Logger
}

type NozzleCfClient client.Client // NozzleCfClient is a wrapper around cfclient.Client

var cfContext = context.Background()

func (ncc NozzleCfClient) GetToken() (string, error) {
	if tokenSource, err := ncc.Config.CreateOAuth2TokenSource(context.Background()); err != nil {
		return "", err
	} else {
		if token, err := tokenSource.Token(); err != nil {
			return "", err
		} else {
			return token.AccessToken, nil
		}
	}
}

func (ncc NozzleCfClient) AppByGuid(appGUID string) (*resource.App, error) {
	return ncc.Applications.Get(cfContext, appGUID)
}

func (ncc NozzleCfClient) ListApps() ([]*resource.App, error) {
	return ncc.Applications.ListAll(cfContext, &client.AppListOptions{ListOptions: &client.ListOptions{PerPage: 500}})
}

func (ncc NozzleCfClient) GetSpaceByGuid(spaceGUID string) (*resource.Space, error) {
	return ncc.Spaces.Get(cfContext, spaceGUID)
}

func (ncc NozzleCfClient) GetOrgByGuid(orgGUID string) (*resource.Organization, error) {
	return ncc.Organizations.Get(cfContext, orgGUID)
}

// create new function of type *SplunkFirehoseNozzle
func NewSplunkFirehoseNozzle(config *Config, logger lager.Logger) *SplunkFirehoseNozzle {
	return &SplunkFirehoseNozzle{
		config: config,
		logger: logger,
	}
}

// EventRouter creates EventRouter object and setup routes for interested events
func (s *SplunkFirehoseNozzle) EventRouter(cache cache.Cache, eventSink eventsink.Sink) (eventrouter.Router, error) {
	LowerAddAppInfo := strings.ToLower(s.config.AddAppInfo)
	config := &eventrouter.Config{
		SelectedEvents: s.config.WantedEvents,
		AddAppName:     strings.Contains(LowerAddAppInfo, "appname"),
		AddOrgName:     strings.Contains(LowerAddAppInfo, "orgname"),
		AddOrgGuid:     strings.Contains(LowerAddAppInfo, "orgguid"),
		AddSpaceName:   strings.Contains(LowerAddAppInfo, "spacename"),
		AddSpaceGuid:   strings.Contains(LowerAddAppInfo, "spaceguid"),
		AddTags:        s.config.AddTags,
	}
	return eventrouter.New(cache, eventSink, config)
}

// CFClient creates a client object which can talk to Cloud Foundry
func (s *SplunkFirehoseNozzle) PCFClient() (*NozzleCfClient, error) {
	var skipSSL config.Option
	if s.config.SkipSSLCF {
		skipSSL = config.SkipTLSValidation()
	}
	if cfConfig, err := config.New(s.config.ApiEndpoint, config.ClientCredentials(s.config.ClientID, s.config.ClientSecret), skipSSL, config.UserAgent(fmt.Sprintf("splunk-firehose-nozzle/%s", s.config.Version))); err != nil {
		return nil, err
	} else {
		if cfClient, err := client.New(cfConfig); err != nil {
			return nil, err
		} else {
			nozzleCfClient := NozzleCfClient(*cfClient)
			return &nozzleCfClient, nil
		}
	}
}

// AppCache creates in-memory cache or boltDB cache
func (s *SplunkFirehoseNozzle) AppCache(client cache.AppClient) (cache.Cache, error) {
	if s.config.AddAppInfo != "" {
		c := cache.BoltdbConfig{
			Path:               s.config.BoltDBPath,
			IgnoreMissingApps:  s.config.IgnoreMissingApps,
			MissingAppCacheTTL: s.config.MissingAppCacheTTL,
			AppCacheTTL:        s.config.AppCacheTTL,
			OrgSpaceCacheTTL:   s.config.OrgSpaceCacheTTL,
			Logger:             s.logger,
		}
		return cache.NewBoltdb(client, &c)
	}

	return cache.NewNoCache(), nil
}

// EventSink creates std sink or Splunk sink
func (s *SplunkFirehoseNozzle) EventSink(cache cache.Cache) (eventsink.Sink, error) {

	// EventWriter for writing events
	writerConfig := &eventwriter.SplunkConfig{
		Host:                    s.config.SplunkHost,
		Token:                   s.config.SplunkToken,
		Index:                   s.config.SplunkIndex,
		SkipSSL:                 s.config.SkipSSLSplunk,
		Debug:                   s.config.Debug,
		Logger:                  s.logger,
		Version:                 s.config.Version,
		RefreshSplunkConnection: s.config.RefreshSplunkConnection,
		KeepAliveTimer:          s.config.KeepAliveTimer,
	}

	var writers []eventwriter.Writer
	for i := 0; i < s.config.HecWorkers+1; i++ {
		splunkWriter := eventwriter.NewSplunkEvent(writerConfig).(*eventwriter.SplunkEvent)
		splunkWriter.SentEventCount = monitoring.RegisterCounter("splunk.events.sent.count", utils.UintType)
		splunkWriter.BodyBufferSize = monitoring.RegisterCounter("splunk.events.throughput", utils.UintType)
		writers = append(writers, splunkWriter)
	}

	parsedExtraFields, err := events.ParseExtraFields(s.config.ExtraFields)
	if err != nil {
		s.logger.Error("Error at parsing extra fields", nil)
		return nil, err
	}

	nozzleUUID := uuid.New().String()

	sinkConfig := &eventsink.SplunkConfig{
		FlushInterval:           s.config.FlushInterval,
		QueueSize:               s.config.QueueSize,
		BatchSize:               s.config.BatchSize,
		Retries:                 s.config.Retries,
		Hostname:                s.config.JobHost,
		SubscriptionID:          s.config.SubscriptionID,
		TraceLogging:            s.config.TraceLogging,
		ExtraFields:             parsedExtraFields,
		UUID:                    nozzleUUID,
		Logger:                  s.logger,
		LoggingIndex:            s.config.SplunkLoggingIndex,
		StatusMonitorInterval:   s.config.StatusMonitorInterval,
		RefreshSplunkConnection: s.config.RefreshSplunkConnection,
		KeepAliveTimer:          s.config.KeepAliveTimer,
	}

	LowerAddAppInfo := strings.ToLower(s.config.AddAppInfo)
	parseConfig := &eventsink.ParseConfig{
		SelectedEvents: s.config.WantedEvents,
		AddAppName:     strings.Contains(LowerAddAppInfo, "appname"),
		AddOrgName:     strings.Contains(LowerAddAppInfo, "orgname"),
		AddOrgGuid:     strings.Contains(LowerAddAppInfo, "orgguid"),
		AddSpaceName:   strings.Contains(LowerAddAppInfo, "spacename"),
		AddSpaceGuid:   strings.Contains(LowerAddAppInfo, "spaceguid"),
		AddTags:        s.config.AddTags,
	}

	splunkSink := eventsink.NewSplunk(writers, sinkConfig, parseConfig, cache)
	splunkSink.Open()

	s.logger.RegisterSink(splunkSink)
	if s.config.StatusMonitorInterval > time.Second*0 {
		go splunkSink.LogStatus()
	}
	return splunkSink, nil
}

func (s *SplunkFirehoseNozzle) Metric() monitoring.Monitor {

	writerConfig := &eventwriter.SplunkConfig{
		Host:    s.config.SplunkHost,
		Token:   s.config.SplunkToken,
		Index:   s.config.SplunkMetricIndex,
		SkipSSL: s.config.SkipSSLSplunk,
		Debug:   s.config.Debug,
		Logger:  s.logger,
		Version: s.config.Version,
	}
	if s.config.StatusMonitorInterval > 0*time.Second && s.config.SelectedMonitoringMetrics != "" {
		splunkWriter := eventwriter.NewSplunkMetric(writerConfig)
		return monitoring.NewMetricsMonitor(s.logger, s.config.StatusMonitorInterval, splunkWriter, s.config.SelectedMonitoringMetrics)
	} else {
		return monitoring.NewNoMonitor()
	}

}

// EventSource creates eventsource.Source object which can read events from
func (s *SplunkFirehoseNozzle) EventSource(pcfClient *NozzleCfClient) *eventsource.Firehose {
	root, err := pcfClient.Root.Get(context.Background())
	if err != nil {
		fmt.Printf("Root: %v, err: %s\n", root, err)
	}
	firehoseConfig := &eventsource.FirehoseConfig{
		KeepAlive:      s.config.KeepAlive,
		SkipSSL:        s.config.SkipSSLCF,
		Endpoint:       root.Links.Logging.Href,
		SubscriptionID: s.config.SubscriptionID,
	}

	return eventsource.NewFirehose(*pcfClient, firehoseConfig)
}

// Nozzle creates a Nozzle object which glues the event source and event router
func (s *SplunkFirehoseNozzle) Nozzle(eventSource eventsource.Source, eventRouter eventrouter.Router) *nozzle.Nozzle {
	firehoseConfig := &nozzle.Config{
		Logger:                s.logger,
		StatusMonitorInterval: s.config.StatusMonitorInterval,
	}

	return nozzle.New(eventSource, eventRouter, firehoseConfig)
}

// Run creates all necessary objects, reading events from CF firehose and sending to target Splunk index
// It runs forever until something goes wrong
func (s *SplunkFirehoseNozzle) Run(shutdownChan chan os.Signal) error {

	metric := s.Metric()

	monitoring.RegisterFunc("nozzle.usage.ram", func() interface{} {
		v, _ := mem.VirtualMemory()
		return (v.UsedPercent)
	})

	monitoring.RegisterFunc("nozzle.usage.cpu", func() interface{} {
		CPU, _ := cpu.Percent(0, false)
		return (CPU[0])
	})

	pcfClient, err := s.PCFClient()
	if err != nil {
		s.logger.Error("Failed to get info from CF Server", nil)
		return err
	}

	appCache, err := s.AppCache(pcfClient)
	if err != nil {
		s.logger.Error("Failed to start App Cache", nil)
		return err
	}

	err = appCache.Open()
	if err != nil {
		s.logger.Error("Failed to open App Cache", nil)
		return err
	}
	defer appCache.Close()

	eventSink, err := s.EventSink(appCache)
	if err != nil {
		s.logger.Error("Failed to create event sink", nil)
		return err
	}

	s.logger.Info("Running splunk-firehose-nozzle with following configuration variables ", s.config.ToMap())

	eventRouter, err := s.EventRouter(appCache, eventSink)
	if err != nil {
		s.logger.Error("Failed to create event router", nil)
		return err
	}

	eventSource := s.EventSource(pcfClient)
	noz := s.Nozzle(eventSource, eventRouter)

	// Continuous Loop will run forever
	go func() {
		err := noz.Start()
		if err != nil {
			s.logger.Error("Firehose consumer exits with error", err)
		}
		shutdownChan <- os.Interrupt
	}()

	go metric.Start()

	<-shutdownChan

	s.logger.Info("Splunk Nozzle is going to exit gracefully")
	metric.Stop()
	noz.Close()
	return eventSink.Close()
}
