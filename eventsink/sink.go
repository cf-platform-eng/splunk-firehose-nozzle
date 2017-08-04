package eventsink

//go:generate counterfeiter . Sink

type Sink interface {
	Open() error
	Close() error
	Write(fields map[string]interface{}, msg string) error
}
