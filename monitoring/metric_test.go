package monitoring_test

import (
	"strings"
	"time"

	"code.cloudfoundry.org/lager/v3"
	. "github.com/cloudfoundry-community/splunk-firehose-nozzle/monitoring"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/testing"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Monitoring", func() {
	var (
		selectedMonitoringMetrics string = "a,b"
		writer                    testing.EventWriterMetricMock
		Counter                   utils.Counter
		monitor                   Monitor
	)

	BeforeEach(func() {

		monitor = NewMetricsMonitor(lager.NewLogger("Test"), 2*time.Second, &writer, selectedMonitoringMetrics)
		monitor.RegisterFunc("a", func() interface{} { return 10 })
		Counter = monitor.RegisterCounter("b", utils.UintType)
	})

	It("Test Monitor Func and Register Counter", func() {
		checkLenFuncs := len(monitor.(*Metrics).CallerFuncs)
		checkLenCounters := len(monitor.(*Metrics).Counters)
		Expect(checkLenFuncs).To(Equal(1))
		Expect(checkLenCounters).To(Equal(1))

	})

	It("Test of Run", func() {

		Counter.Add(10)
		go monitor.Start()
		time.Sleep(3 * time.Second)
		monitor.Stop()
		value := writer.Read()[len(writer.CapturedEvents)-1]["fields"].(map[string]interface{})["metric_name:b"]
		Expect(value).To(Equal(uint64(10)))
		Expect(len(writer.CapturedEvents)).To(Equal(1))
	})

	It("Identical Key ", func() {
		Counter.Add(10)
		Counter := RegisterCounter("b", utils.UintType)
		Counter.Add(10)
		go monitor.Start()
		time.Sleep(3 * time.Second)
		monitor.Stop()
		value := writer.Read()[len(writer.CapturedEvents)-1]["fields"].(map[string]interface{})["metric_name:b"]
		Expect(value).To(Equal(uint64(20)))
	})

	It("Test when metric is disabled", func() {
		disabledMonitoringMetrics := "b"
		monitor = NewMetricsMonitor(lager.NewLogger("Test"), 2*time.Second, &writer, disabledMonitoringMetrics)
		monitor.RegisterFunc("a", func() interface{} { return 10 })
		Counter = monitor.RegisterCounter("b", utils.UintType)
		checkLenCounter := len(monitor.(*Metrics).Counters)
		checkLenFuncs := len(monitor.(*Metrics).CallerFuncs)
		Expect(checkLenFuncs).To(Equal(0))
		Expect(checkLenCounter).To(Equal(1))

	})

})

var _ = Describe("Parsing of Selected Metrics", func() {
	var (
		selectedMonitoringMetrics string = `["a", "b", "c", "d", "e", "f"]`
	)

	It("Test Parsing of Selected Metrics", func() {
		event := ParseSelectedMetrics(selectedMonitoringMetrics)
		Expect(len(event)).To(Equal(6))
		Expect(strings.Join(event, "")).To(Equal("abcdef"))

	})

})
