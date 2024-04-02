package retrypool

import "time"

type Option[T any] func(*options[T])

// WithRetryDelay initial pause between retries
func WithRetryDelay[T any](delay time.Duration) Option[T] {
	return func(o *options[T]) {
		o.retryDelay = delay
	}
}

// WithMaxRetryDelay is the maximum pause between retries. The retry time will start with RetryDelay and double after each unsuccessful error queue processing.
func WithMaxRetryDelay[T any](delay time.Duration) Option[T] {
	return func(o *options[T]) {
		o.maxRetryDelay = delay
	}
}

// WithProcessingDelay is the timeout based on which a context will be created and passed to the data processing function.
func WithProcessingDelay[T any](delay time.Duration) Option[T] {
	return func(o *options[T]) {
		o.processingDelay = delay
	}
}

// WithErrorFunc function for managing the lifetime of data in the error queue. If ErrorFunc returns true, the data is returned to the queue; otherwise, it is removed from it.
func WithErrorFunc[T any](f ErrorFunc[T]) Option[T] {
	return func(o *options[T]) {
		o.errorFunc = f
	}
}

// WithSuccessFunc. Called when data is successfully sent after an error
func WithSuccessFunc[T any](f SuccessFunc[T]) Option[T] {
	return func(o *options[T]) {
		o.successFunc = f
	}
}

// WithMaxAge is the maximum message lifetime. Used for monitoring purposes.
func WithMaxAge[T any](age time.Duration) Option[T] {
	return func(o *options[T]) {
		o.maxAge = age
	}
}

// WithMetricFuncs functions for working with metrics
func WithMetricFuncs[T any](
	dataLostCountFunc, metricRetryCountFunc, metricFirstCount MetricCounterFunc,
	metricTimeToMaxFunc, metricRetryDelayToMaxFunc MetricGaugeFunc,
) Option[T] {
	return func(o *options[T]) {
		o.dataLostCountFunc = dataLostCountFunc
		o.metricRetryCountFunc = metricRetryCountFunc
		o.metricFirstCount = metricFirstCount
		o.metricTimeToMaxFunc = metricTimeToMaxFunc
		o.metricRetryDelayToMaxFunc = metricRetryDelayToMaxFunc
	}
}

// options initialization parameters for RetryPool
type options[T any] struct {
	retryDelay      time.Duration  // initial pause between retries
	maxRetryDelay   time.Duration  // maximum pause between retries. Retry time will start with RetryDelay and double after each unsuccessful error queue processing
	processingDelay time.Duration  // timeout based on which a context will be created and passed to the data processing function
	errorFunc       ErrorFunc[T]   // function for managing the lifetime of data in the error queue. If ErrorFunc returns true, the data is returned to the queue; otherwise, it is removed from it
	successFunc     SuccessFunc[T] // called when data is successfully sent after an error
	maxAge          time.Duration  // maximum message lifetime. Used for monitoring purposes

	dataLostCountFunc         MetricCounterFunc // monitoring the number of lost data
	metricRetryCountFunc      MetricCounterFunc // monitoring the number of retry attempts
	metricFirstCount          MetricCounterFunc // monitoring the number of initial processing attempts
	metricTimeToMaxFunc       MetricGaugeFunc   // monitoring the maximum age of data in the queue
	metricRetryDelayToMaxFunc MetricGaugeFunc   // monitoring the delay before retrying data processing from the error queue
}
