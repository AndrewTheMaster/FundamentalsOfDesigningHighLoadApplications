package metrics

// Collector captures counters, gauges and histograms.
type Collector interface {
	IncCounter(name string, labels map[string]string, delta float64)
	SetGauge(name string, labels map[string]string, value float64)
	ObserveHistogram(name string, labels map[string]string, value float64)
} 