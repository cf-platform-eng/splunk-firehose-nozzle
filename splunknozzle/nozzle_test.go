package splunknozzle_test

import (
	"fmt"
	"os"
	"time"

	"code.cloudfoundry.org/lager/v3"

	"github.com/cloudfoundry-community/splunk-firehose-nozzle/monitoring"
	. "github.com/cloudfoundry-community/splunk-firehose-nozzle/splunknozzle"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func newConfig() *Config {
	return &Config{
		ApiEndpoint:  "http://localhost:9911",
		User:         "admin",
		Password:     "admin",
		ClientID:     "admin",
		ClientSecret: "admin",

		SplunkToken: "token",
		SplunkHost:  "localhost:8088",
		SplunkIndex: "main",

		JobHost: "localhost",

		SkipSSLCF:      true,
		SkipSSLSplunk:  true,
		SubscriptionID: "splunk-sub",
		KeepAlive:      time.Second * 25,

		AddAppInfo:         "AppName,OrgName,OrgGuid,SpaceName,SpaceGuid",
		IgnoreMissingApps:  true,
		MissingAppCacheTTL: time.Second * 30,
		AppCacheTTL:        time.Second * 30,
		AppLimits:          0,
		OrgSpaceCacheTTL:   time.Second * 30,

		BoltDBPath:   "/tmp/boltdb.db",
		WantedEvents: "LogMessage",
		ExtraFields:  "tag:value",

		FlushInterval: time.Second * 5,
		QueueSize:     1000,
		BatchSize:     100,
		Retries:       1,
		HecWorkers:    8,

		Version: "1.0",
		Branch:  "develop",
		Commit:  "f1c3178f4df3e51e7f08abf046ac899bca49e93b",
		BuildOS: "MacOS",

		TraceLogging:              false,
		Debug:                     false,
		StatusMonitorInterval:     time.Second * 5,
		SelectedMonitoringMetrics: "splunk.events.dropped.count",
		SplunkMetricIndex:         "metric",
	}
}

var _ = Describe("SplunkFirehoseNozzle", func() {
	var (
		config *Config
		noz    *SplunkFirehoseNozzle
		logger lager.Logger
	)

	BeforeEach(func() {
		config = newConfig()
		logger = lager.NewLogger("test")
		noz = NewSplunkFirehoseNozzle(config, logger)
	})

	It("EventSink", func() {
		c := testing.NewMemoryCacheMock()
		_, err := noz.EventSink(c)
		Ω(err).ShouldNot(HaveOccurred())

		config.Debug = true
		_, err = noz.EventSink(c)
		Ω(err).ShouldNot(HaveOccurred())
	})

	It("PCFClient", func() {
		port := 9911
		cc := testing.NewCloudControllerMock(port)
		started := make(chan struct{})
		go func() {
			started <- struct{}{}
			cc.Start()
		}()
		<-started

		_, err := noz.PCFClient()
		Ω(err).ShouldNot(HaveOccurred())
		cc.Stop()
	})

	It("AppCache", func() {
		client := testing.NewAppClientMock(1)
		_, err := noz.AppCache(client)
		Ω(err).ShouldNot(HaveOccurred())

		config.AddAppInfo = ""
		_, err = noz.AppCache(client)
		Ω(err).ShouldNot(HaveOccurred())
	})

	It("EventRouter", func() {
		c := testing.NewMemoryCacheMock()
		s := testing.NewMemorySinkMock()
		_, err := noz.EventRouter(c, s)
		Ω(err).ShouldNot(HaveOccurred())
	})

	It("Monitoring Enabled", func() {
		enableMonitoring := noz.Metric()
		if _, ok := enableMonitoring.(*monitoring.Metrics); ok {
			Expect(ok).To(Equal(true))
		}

	})

	It("Monitoring Disabled", func() {
		config.StatusMonitorInterval = 0 * time.Second
		disableMonitoring := noz.Metric()
		_, ok := disableMonitoring.(*monitoring.Metrics)
		Expect(ok).To(Equal(false))

	})

	It("Nozzle", func() {
		src := testing.NewMemoryEventSourceMock(1, 10, -1)
		router := testing.NewEventRouterMock(false)
		n := noz.Nozzle(src, router)
		Expect(n).ToNot(BeNil())
	})

	It("Run without cloudcontroller, error out", func() {
		shutdownChan := make(chan os.Signal, 2)
		err := noz.Run(shutdownChan)
		Ω(err).Should(HaveOccurred())
	})

	It("Run with cloudcontroller", func() {
		config.AddAppInfo = ""
		port := 9911
		cc := testing.NewCloudControllerMock(port)
		started := make(chan struct{})
		go func() {
			started <- struct{}{}
			err := cc.Start()
			if err != nil {
				fmt.Printf("cc.Start() err: %s\n", err)
			}
		}()
		<-started

		shutdownChan := make(chan os.Signal, 2)
		go func() {
			time.Sleep(time.Second)
			shutdownChan <- os.Interrupt
		}()
		err := noz.Run(shutdownChan)
		Ω(err).ShouldNot(HaveOccurred())
	})
})
