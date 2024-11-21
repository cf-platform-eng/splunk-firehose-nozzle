package monitoring

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"code.cloudfoundry.org/lager/v3"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/eventwriter"
	"github.com/cloudfoundry-community/splunk-firehose-nozzle/utils"
)

const (
	splunkMetric = "metric_name:"
)

type Metrics struct {
	CallerFuncs               map[string]MonitorFunc
	Counters                  map[string][]utils.Counter
	logger                    lager.Logger
	interval                  time.Duration
	ticker                    *time.Ticker
	writer                    eventwriter.Writer
	selectedMonitoringMetrics *utils.Set
	tickerMutex               sync.Mutex
}

func NewMetricsMonitor(logger lager.Logger, interval time.Duration, writer eventwriter.Writer, filter string) Monitor {

	if monitor != nil {
		monitor.Stop()
	}
	monitor = &Metrics{
		CallerFuncs:               make(map[string]MonitorFunc),
		Counters:                  make(map[string][]utils.Counter),
		logger:                    logger,
		interval:                  interval,
		writer:                    writer,
		selectedMonitoringMetrics: setValuesForSet(filter),
	}
	return monitor.(*Metrics)
}

func (m *Metrics) RegisterFunc(id string, mFunc MonitorFunc) {

	if m.selectedMonitoringMetrics.Contains(id) && m.interval > 0*time.Second {
		m.CallerFuncs[id] = mFunc
	}
}

func (m *Metrics) RegisterCounter(id string, varType utils.CounterType) utils.Counter {

	if m.selectedMonitoringMetrics.Contains(id) && m.interval > 0*time.Second {
		if varType == utils.UintType {
			ctr := new(utils.IntCounter)
			m.Counters[id] = append(m.Counters[id], ctr)
			return ctr
		}
	}
	return &utils.NopCounter{}
}

func (m *Metrics) extractFunc(metricEvent map[string]interface{}) {
	for key, Func := range m.CallerFuncs {
		valofFunc := Func()
		splunkKey := splunkMetric + key
		metricEvent[splunkKey] = valofFunc
	}
}

func (m *Metrics) extractCounter(metricEvent map[string]interface{}) {
	for key, listofCounters := range m.Counters {
		InitSum := listofCounters[0]
		for i := 1; i < len(listofCounters); i++ {
			InitSum.Add(listofCounters[i].Value())
		}
		splunkKey := splunkMetric + key
		metricEvent[splunkKey] = InitSum.Value()
	}
}

func (m *Metrics) Start() {
	if m.selectedMonitoringMetrics.Len() > 0 {
		ticker := time.NewTicker(m.interval)
		m.tickerMutex.Lock()
		m.ticker = ticker
		m.tickerMutex.Unlock()

		metricEvent := make(map[string]interface{})
		for {
			select {
			case <-ticker.C:
				m.extractFunc(metricEvent)
				m.extractCounter(metricEvent)
				finalMetricEvent := prepareBatch(metricEvent)
				events := []map[string]interface{}{
					finalMetricEvent,
				}
				m.writer.Write(events)
			}
		}
	}
}

func (m *Metrics) Stop() error {
	m.tickerMutex.Lock()
	if m.ticker != nil {
		m.ticker.Stop()
	}
	m.tickerMutex.Unlock()
	return nil
}

func prepareBatch(event map[string]interface{}) map[string]interface{} {
	finalevent := make(map[string]interface{})
	event["instance_index"] = os.Getenv("INSTANCE_INDEX")
	finalevent["fields"] = event
	finalevent["event"] = "metric"
	finalevent["sourcetype"] = "cf:nozzlemetrics"
	finalevent["time"] = utils.NanoSecondsToSeconds(time.Now().UnixNano())

	return finalevent

}

func setValuesForSet(selectedMetrics string) *utils.Set {
	s := utils.NewSet()
	listofSelectedMetrics := ParseSelectedMetrics(selectedMetrics)
	for i := 0; i < len(listofSelectedMetrics); i++ {
		s.Add(listofSelectedMetrics[i])
	}
	return s
}

func ParseSelectedMetrics(wantedMetrics string) []string {
	wantedMetrics = strings.TrimSpace(wantedMetrics)
	var events []string
	if err := json.Unmarshal([]byte(wantedMetrics), &events); err != nil {
		events = strings.Split(wantedMetrics, ",")
	}
	return events
}
