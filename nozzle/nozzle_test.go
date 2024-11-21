package nozzle_test

import (
	"time"

	"code.cloudfoundry.org/lager/v3"

	. "github.com/cloudfoundry-community/splunk-firehose-nozzle/nozzle"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-community/splunk-firehose-nozzle/testing"
)

var _ = Describe("Nozzle", func() {
	var (
		eventSource *testing.MemoryEventSourceMock
		eventRouter *testing.EventRouterMock
		nozzle      *Nozzle
	)

	Context("When there is error when opening event source", func() {
		BeforeEach(func() {
			eventSource = testing.NewMemoryEventSourceMock(-1, int64(10), -1)
			eventRouter = testing.NewEventRouterMock(false)
			config := &Config{
				Logger: lager.NewLogger("test"),
			}
			eventSource.MockOpenErr = true
			nozzle = New(eventSource, eventRouter, config)
		})

		It("receive error when opening event source", func() {
			err := nozzle.Start()
			Expect(err).To(Equal(testing.MockupErr))
		})
	})

	Context("When there are no errors from event source", func() {
		BeforeEach(func() {
			eventSource = testing.NewMemoryEventSourceMock(-1, int64(10), -1)
			eventRouter = testing.NewEventRouterMock(false)
			config := &Config{
				Logger: lager.NewLogger("test"),
			}
			nozzle = New(eventSource, eventRouter, config)
		})

		It("collects events from source and routes to sink", func() {
			go nozzle.Start()

			time.Sleep(time.Second)

			Eventually(func() []*events.Envelope {
				return eventRouter.Events()
			}).Should(HaveLen(10))
		})

		It("EventSource close", func() {
			go nozzle.Start()
			time.Sleep(time.Second)
			eventSource.Close()
			time.Sleep(time.Second)
			nozzle.Close()
		})
	})

	prepare := func(closeErr int, statusMonitorInterval time.Duration) func() {
		return func() {
			eventSource = testing.NewMemoryEventSourceMock(-1, int64(10), closeErr)
			eventRouter = testing.NewEventRouterMock(false)
			config := &Config{
				Logger:                lager.NewLogger("test"),
				StatusMonitorInterval: statusMonitorInterval,
			}
			nozzle = New(eventSource, eventRouter, config)
		}
	}

	runAndAssert := func(closeErr int) func() {
		return func() {
			done := make(chan error, 1)
			go func() {
				err := nozzle.Start()
				done <- err
			}()

			time.Sleep(time.Second)
			nozzle.Close()

			err := <-done
			if ce, ok := err.(*websocket.CloseError); ok {
				Expect(ce.Code).To(Equal(closeErr))
			} else {
				Expect(err).To(Equal(testing.MockupErr))
			}
		}
	}

	Context("When there is websocket.CloseNormalClosure from event source", func() {
		BeforeEach(prepare(websocket.CloseNormalClosure, time.Second*0))
		It("handles errors when collects events from source", runAndAssert(websocket.CloseNormalClosure))
	})

	Context("When there is websocket.ClosePolicyViolation from event source", func() {
		BeforeEach(prepare(websocket.ClosePolicyViolation, time.Second*0))
		It("handles errors when collects events from source", runAndAssert(websocket.ClosePolicyViolation))
	})

	Context("When there is websocket.CloseGoingAway from event source", func() {
		BeforeEach(prepare(websocket.CloseGoingAway, time.Second*0))
		It("handles errors when collects events from source", runAndAssert(websocket.CloseGoingAway))
	})

	Context("When there is other error from event source", func() {
		BeforeEach(prepare(0, time.Second*0))
		It("handles errors when collects events from source", runAndAssert(0))
	})

	Context("When there is an error from router", func() {
		BeforeEach(func() {
			eventSource = testing.NewMemoryEventSourceMock(-1, int64(10), -1)
			eventRouter = testing.NewEventRouterMock(true)
			config := &Config{
				Logger: lager.NewLogger("test"),
			}
			nozzle = New(eventSource, eventRouter, config)
		})

		It("collects events from source and routes to sink", func() {
			go nozzle.Start()

			time.Sleep(time.Second)

			Eventually(func() []*events.Envelope {
				return eventRouter.Events()
			}).Should(HaveLen(0))
		})
	})

	Context("When StatusMonitorInterval is provided", func() {
		BeforeEach(func() {
			eventSource = testing.NewMemoryEventSourceMock(-1, int64(10), -1)
			eventRouter = testing.NewEventRouterMock(false)
			config := &Config{
				Logger:                lager.NewLogger("test"),
				StatusMonitorInterval: time.Second,
			}
			nozzle = New(eventSource, eventRouter, config)
		})

		It("collects events from source and routes to sink", func() {
			go nozzle.Start()

			time.Sleep(time.Second)

			Eventually(func() []*events.Envelope {
				return eventRouter.Events()
			}).Should(HaveLen(10))
		})

		It("EventSource close", func() {
			go nozzle.Start()
			time.Sleep(time.Second)
			eventSource.Close()
			time.Sleep(time.Second)
			nozzle.Close()
		})
	})

	Context("When statusMonitorInterval provided and there is websocket.CloseNormalClosure from event source", func() {
		BeforeEach(prepare(websocket.CloseNormalClosure, time.Second*1))
		It("handles errors when collects events from source", runAndAssert(websocket.CloseNormalClosure))
	})

	Context("When statusMonitorInterval provided and there is websocket.ClosePolicyViolation from event source", func() {
		BeforeEach(prepare(websocket.ClosePolicyViolation, time.Second*1))
		It("handles errors when collects events from source", runAndAssert(websocket.ClosePolicyViolation))
	})

	Context("When statusMonitorInterval provided and there is websocket.CloseGoingAway from event source", func() {
		BeforeEach(prepare(websocket.CloseGoingAway, time.Second*1))
		It("handles errors when collects events from source", runAndAssert(websocket.CloseGoingAway))
	})

	Context("When statusMonitorInterval provided and there is other error from event source", func() {
		BeforeEach(prepare(0, time.Second*1))
		It("handles errors when collects events from source", runAndAssert(0))
	})

	Context("When statusMonitorInterval provided and there is an error from router", func() {
		BeforeEach(func() {
			eventSource = testing.NewMemoryEventSourceMock(-1, int64(10), -1)
			eventRouter = testing.NewEventRouterMock(true)
			config := &Config{
				Logger:                lager.NewLogger("test"),
				StatusMonitorInterval: time.Second,
			}
			nozzle = New(eventSource, eventRouter, config)
		})

		It("collects events from source and routes to sink", func() {
			go nozzle.Start()

			time.Sleep(time.Second)

			Eventually(func() []*events.Envelope {
				return eventRouter.Events()
			}).Should(HaveLen(0))
		})
	})

})
