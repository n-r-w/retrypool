// Package retrypool Execution of tasks for processing data in multiple workers with error control.
// In case of an error, the data is deferred to a buffer and periodic retries are made.
// Data is deleted from the error buffer through the ErrorFunc function.
package retrypool

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"
)

// StateInformer interface for collecting usage statistics without the need to specify a pointer to the RetryPool itself, which requires typing
type StateInformer interface {
	InboundQueueLen() int64
	RetryQueueLen() int64
	InboundQueueMax() int
	RetryQueueMax() int
}

// WorkFunc is the main function for processing data.
type WorkFunc[T any] func(ctx context.Context, v T, age time.Duration) error

// ErrorFunc is a function for error handling. The parameter "closing" is true when the last attempt is made before stopping.
type ErrorFunc[T any] func(err error, v T, age time.Duration, closing bool) (keep bool)

// SuccessFunc is called when data is successfully sent after an error
type SuccessFunc[T any] func(v T, age time.Duration)

// LogPanicFunc is a function for logging panics.
type LogPanicFunc func(ctx context.Context, recoverInfo any, stack []byte)

// MetricCounterFunc is a function for modifying Counter metrics
type MetricCounterFunc func()

// MetricGaugeFunc is a function for modifying Gauge metrics
type MetricGaugeFunc func(float64)

// RetryPool ...
type RetryPool[T any] struct {
	opts options[T] // parameters

	inboundQueueSize int         // size of the queue for processing. It receives both incoming data and data from the error queue
	retryQueueSize   int         // size of the error queue. When it exceeds, the Add method will return false
	workerCount      int         // number of goroutines that process the data
	workFunc         WorkFunc[T] // function for processing data

	muErrorQueue         sync.RWMutex
	errorQueue           *deque.Deque[bufferItem[T]] // queue for retries
	errorQueueLen        int64                       // atomic value of the current queue size, to avoid unnecessary lock in the Add method
	lastRetrySuccessBool int32                       // atomic boolean value. success of the last retry attempt

	muInboundQueue  sync.Mutex
	chInbound       chan bufferItem[T]          // unbuffered channel for receiving data for processing
	inboundQueue    *deque.Deque[bufferItem[T]] // queue for processing. Channels are not used to minimize memory allocation
	inboundQueueLen int64                       // atomic value of the current queue size, to avoid unnecessary locks

	wgInboundExtractor     *sync.WaitGroup // control the completion of the worker extracting data from the processing queue
	chInboundExtractor     chan struct{}   // channel for notification of new incoming data
	currentErrorRetryDelay int64           // current interval between retry attempts (time.Duration)

	wgCloseWorkers        *sync.WaitGroup // control the completion of the main workers
	processingCount       int64           // number of operations being processed
	retryProcessingCount  int64           // number of retry operations being processed
	wgCloseRetryExtractor *sync.WaitGroup // control the completion of the worker extracting data from the error queue

	isClosingBool int32 // closing state

	maxAge int64 // maximum age of data in the error queue, used for monitoring

	logPanicFunc LogPanicFunc // panic logging function

	dataLostCountFunc         MetricCounterFunc // monitoring the number of lost data
	metricRetryCountFunc      MetricCounterFunc // monitoring the number of retries
	metricFirstCount          MetricCounterFunc // monitoring the number of initial processing attempts
	metricTimeToMaxFunc       MetricGaugeFunc   // monitoring the maximum age of data in the queue
	metricRetryDelayToMaxFunc MetricGaugeFunc   // monitoring the delay before retrying data from the error queue
}

type bufferItem[T any] struct {
	v     T
	t     time.Time
	retry bool // first attempt or retry. Not protected because there can be no collisions. Needed for the SuccessFunc handler.
}

// New creates a RetryPool
func New[T any](inboundQueueSize, retryQueueSize, workerCount int, workFunc WorkFunc[T], opts ...Option[T]) *RetryPool[T] {
	if inboundQueueSize <= 0 || retryQueueSize <= 0 || workerCount <= 0 || workFunc == nil {
		panic("invalid parameters")
	}

	o := options[T]{}
	for _, opt := range opts {
		opt(&o)
	}

	const (
		defaultRetryDelay      = time.Second
		defaultProcessingDelay = time.Hour
		defaultMaxAge          = time.Minute * 10
	)

	if o.retryDelay <= 0 {
		o.retryDelay = defaultRetryDelay
	}
	if o.maxRetryDelay < o.retryDelay {
		o.maxRetryDelay = o.retryDelay
	}
	if o.processingDelay <= 0 {
		o.processingDelay = defaultProcessingDelay
	}
	if o.maxAge <= 0 {
		o.maxAge = defaultMaxAge
	}

	c := &RetryPool[T]{
		opts:                   o,
		muErrorQueue:           sync.RWMutex{},
		errorQueue:             deque.New[bufferItem[T]](),
		errorQueueLen:          0,
		lastRetrySuccessBool:   0,
		muInboundQueue:         sync.Mutex{},
		chInbound:              make(chan bufferItem[T]),
		inboundQueue:           deque.New[bufferItem[T]](),
		inboundQueueLen:        0,
		wgInboundExtractor:     &sync.WaitGroup{},
		chInboundExtractor:     make(chan struct{}, 1),
		currentErrorRetryDelay: int64(o.retryDelay),
		wgCloseWorkers:         &sync.WaitGroup{},
		processingCount:        0,
		retryProcessingCount:   0,
		wgCloseRetryExtractor:  &sync.WaitGroup{},
		isClosingBool:          0,
		inboundQueueSize:       inboundQueueSize,
		retryQueueSize:         retryQueueSize,
		workerCount:            workerCount,
		workFunc:               workFunc,
	}

	// Workers for processing queue
	c.wgCloseWorkers.Add(c.workerCount)
	for i := 0; i < c.workerCount; i++ {
		go c.worker()
	}

	// Worker for extracting from error queue
	c.wgCloseRetryExtractor.Add(1)
	go c.retryExtractor()

	// Worker for extracting incoming data from the queue
	c.wgInboundExtractor.Add(1)
	go c.inboundExtractor()

	return c
}

// InboundQueueLen returns the current size of the queue for processing data plus the number of items being processed.
// Used for statistics and debugging.
func (d *RetryPool[T]) InboundQueueLen() int64 {
	return atomic.LoadInt64(&d.inboundQueueLen) + atomic.LoadInt64(&d.processingCount)
}

// RetryQueueLen returns the current size of the error queue (data from this queue periodically goes back to InboundQueue) plus the number of items being retried.
// Used for statistics and debugging.
func (d *RetryPool[T]) RetryQueueLen() int64 {
	return atomic.LoadInt64(&d.errorQueueLen) + atomic.LoadInt64(&d.retryProcessingCount)
}

// InboundQueueMax is the maximum size of the queue for processing data
func (d *RetryPool[T]) InboundQueueMax() int {
	return d.inboundQueueSize
}

// RetryQueueMax is the maximum size of the error queue
func (d *RetryPool[T]) RetryQueueMax() int {
	return d.retryQueueSize
}

// RetryDelay initial pause between retries
func (d *RetryPool[T]) RetryDelay() time.Duration {
	return d.opts.retryDelay
}

// MaxRetryDelay is the maximum pause between retries
func (d *RetryPool[T]) MaxRetryDelay() time.Duration {
	return d.opts.maxRetryDelay
}

// ProcessingDelay is the timeout based on which a context will be created and passed to the data processing function
func (d *RetryPool[T]) ProcessingDelay() time.Duration {
	return d.opts.processingDelay
}

// MaxAge is the maximum lifetime of messages. Used for monitoring the state.
func (d *RetryPool[T]) MaxAge() time.Duration {
	return d.opts.maxAge
}

// Exec executes the processing immediately and if it returns an error, adds it to the queue.
// Returns an error if it occurred and it was not possible to add it to the queue due to overflow or stop.
func (d *RetryPool[T]) Exec(ctx context.Context, v T) error {
	if err := d.workFunc(ctx, v, 0); err != nil {
		if !d.Add(v) {
			return err
		}
	}

	return nil
}

// Add adds data to the processing queue. Returns false if the queue is full or if Stop was called.
func (d *RetryPool[T]) Add(v T) bool {
	// Check the error queue for overflow. This needs to be done here because data is continuously retrieved from the channel inside the worker.
	// We don't need 100% control over overflow, so we use a separate atomic variable.
	if atomic.LoadInt64(&d.errorQueueLen) >= int64(d.retryQueueSize) {
		if d.dataLostCountFunc != nil {
			d.dataLostCountFunc()
		}
		return false
	}

	return d.addHelper(bufferItem[T]{
		v:     v,
		t:     time.Now(),
		retry: false,
	}, true)
}

// Add data to the processing queue. Returns false if the buffer is full.
// regularCall - external call from the Add method
func (d *RetryPool[T]) addHelper(item bufferItem[T], regularCall bool) bool {
	d.muInboundQueue.Lock()
	defer d.muInboundQueue.Unlock() // We won't hang in this method, so we can unlock on exit.

	if regularCall && d.isClosing() {
		if d.dataLostCountFunc != nil {
			d.dataLostCountFunc()
		}
		return false // Call from the Add method while in the closing state
	}

	if d.inboundQueue.Len() > d.inboundQueueSize {
		if d.dataLostCountFunc != nil {
			d.dataLostCountFunc()
		}
		return false // Processing queue overflow
	}

	d.inboundQueue.PushBack(item)
	atomic.StoreInt64(&d.inboundQueueLen, int64(d.inboundQueue.Len()))

	select {
	case d.chInboundExtractor <- struct{}{}: // Notify about the presence of data
	default: // If chInboundExtractor is full, it means the notification is already there
	}

	return true
}

// Stop closes the reception of new tasks (Add will return false), then tries to process the buffer one last time.
func (d *RetryPool[T]) Stop() {
	d.muInboundQueue.Lock() // Lock to prevent race condition with addHelper for the isClosing state
	d.setClosing()          // Enter the closing state
	d.muInboundQueue.Unlock()

	d.wgCloseRetryExtractor.Wait() // Wait for the error queue handler to finish

	// Stop the inbound data queue handler
	close(d.chInboundExtractor)
	d.wgInboundExtractor.Wait()

	// Stop the data processing workers
	close(d.chInbound)
	d.wgCloseWorkers.Wait()

	// Report the loss of all data remaining in the error queue
	errorLostOnStop := fmt.Errorf("data lost due to retrypool shutdown")
	for {
		if item, ok := d.getFromErrorQueue(); ok {
			d.opts.errorFunc(errorLostOnStop, item.v, time.Since(item.t), true)
			if d.dataLostCountFunc != nil {
				d.dataLostCountFunc() // Data is lost
			}
		} else {
			break
		}
	}

	// Reset metric data
	d.timeToMaxMetric(0)

	if d.metricRetryDelayToMaxFunc != nil {
		d.metricRetryDelayToMaxFunc(0)
	}
}

// Data processing
func (d *RetryPool[T]) worker() {
	defer d.wgCloseWorkers.Done()

	for {
		// Retrieve data from the inbound queue
		item, ok := <-d.chInbound
		if !ok {
			return
		}

		ctx, cancelFunc := d.getContext()
		age := time.Since(item.t)

		if item.retry {
			if d.metricRetryCountFunc != nil {
				d.metricRetryCountFunc()
			}
		} else {
			if d.metricFirstCount != nil {
				d.metricFirstCount()
			}
		}

		err := d.execWithRecover(ctx, item.v, age) // Process the data
		atomic.AddInt64(&d.processingCount, -1)
		if item.retry {
			atomic.AddInt64(&d.retryProcessingCount, -1)
		}

		if err != nil {
			if item.retry {
				atomic.StoreInt32(&d.lastRetrySuccessBool, 0) // Failed to send data successfully after an error
			}

			closing := d.isClosing()

			if d.opts.errorFunc(err, item.v, time.Since(item.t), // Age at the time of processing
				// Pass the closing flag to indicate that we are permanently losing data and there will be no return to the queue
				closing) && !closing {
				// Add to the error queue on error. If the work is finished, do not add
				d.addToErrorQueue(item)
				d.timeToMaxMetric(time.Since(item.t)) // Record the lifetime in metrics
			} else {
				if d.dataLostCountFunc != nil {
					d.dataLostCountFunc() // Data is lost
				}
				d.timeToMaxMetric(0) // Reset the metric with data about the lifetime
			}
		} else if item.retry {
			atomic.StoreInt32(&d.lastRetrySuccessBool, 1) // Successfully sent data after an error

			if d.opts.successFunc != nil {
				d.opts.successFunc(item.v, age)
			}

			d.timeToMaxMetric(0) // Reset the metric with data about the lifetime

			if d.metricRetryDelayToMaxFunc != nil {
				d.metricRetryDelayToMaxFunc(0) // Reset the metric for the percentage of retry delays for a better graph
			}
		}

		cancelFunc()
	}
}

// Periodically retrieves data from the error queue and sends it to the processing queue
func (d *RetryPool[T]) retryExtractor() {
	defer d.wgCloseRetryExtractor.Done()

	for {
		if d.isClosing() {
			return
		}

		// The pause between retries is dynamically calculated based on a combination of factors
		delay := d.updateRetryDelay()
		time.Sleep(delay)

		if item, ok := d.getFromErrorQueue(); ok { // Retrieve items from the error queue
			// and send them to the unbuffered channel, from which the workers will retrieve the data
			// We can start waiting for chInbound to be available here, but it's fine
			d.addToInboundChannel(item)

			// Report the percentage of retry delays metric
			if d.metricRetryDelayToMaxFunc != nil {
				d.metricRetryDelayToMaxFunc(100.0 * float64(delay) / float64(d.opts.maxRetryDelay))
			}
		} else {
			// Reset the retry delay percentage metric for a better graph
			if d.metricRetryDelayToMaxFunc != nil {
				d.metricRetryDelayToMaxFunc(0)
			}
		}
	}
}

// Send to the unbuffered channel, from which the workers will retrieve the data
func (d *RetryPool[T]) addToInboundChannel(item bufferItem[T]) {
	atomic.AddInt64(&d.processingCount, 1)
	if item.retry {
		atomic.AddInt64(&d.retryProcessingCount, 1)
	}
	d.chInbound <- item
}

// Process the inbound data queue
func (d *RetryPool[T]) inboundExtractor() {
	defer d.wgInboundExtractor.Done()

	for {
		// Sleep until new data appears
		_, ok := <-d.chInboundExtractor

		// Retrieve data from the inbound queue
		for {
			d.muInboundQueue.Lock()
			if d.inboundQueue.Len() == 0 {
				d.muInboundQueue.Unlock()
				break
			}
			item := d.inboundQueue.PopFront()
			atomic.StoreInt64(&d.inboundQueueLen, int64(d.inboundQueue.Len()))
			d.muInboundQueue.Unlock()

			// Send to the unbuffered channel, from which the workers will retrieve the data
			d.addToInboundChannel(item)
		}

		if !ok {
			return
		}
	}
}

// In the closing process
func (d *RetryPool[T]) isClosing() bool {
	return atomic.LoadInt32(&d.isClosingBool) > 0
}

func (d *RetryPool[T]) setClosing() {
	atomic.StoreInt32(&d.isClosingBool, 1)
}

// Calculate the delay between retrieving data from the error queue
func (d *RetryPool[T]) updateRetryDelay() time.Duration {
	var currentDelay int64

	errorQueueLen := atomic.LoadInt64(&d.errorQueueLen)               // Current size of the error queue
	retryProcessingCount := atomic.LoadInt64(&d.retryProcessingCount) // Number of retry operations being processed

	if atomic.LoadInt32(&d.lastRetrySuccessBool) > 0 && // The last retry operation was successful
		errorQueueLen > 0 { // The error queue is not empty
		currentDelay = 0 // Remove the interval for quickly draining data from the queue
	} else if errorQueueLen+retryProcessingCount == 0 { // The error queue is empty and nothing has been sent for processing
		// There is no point in increasing the retry timeout, as there is nothing to send
		currentDelay = int64(d.opts.retryDelay) // Minimum interval
	} else {
		// Gradually increase the timeout to the maximum
		currentDelay = atomic.LoadInt64(&d.currentErrorRetryDelay) * 2 // nolint:gomnd
		if currentDelay > int64(d.opts.maxRetryDelay) {
			currentDelay = int64(d.opts.maxRetryDelay)
		}
	}

	atomic.StoreInt64(&d.currentErrorRetryDelay, currentDelay)
	return time.Duration(currentDelay)
}

// Retrieve one item from the error queue
func (d *RetryPool[T]) getFromErrorQueue() (item bufferItem[T], hasData bool) {
	d.muErrorQueue.Lock()
	defer d.muErrorQueue.Unlock()

	if d.errorQueue.Len() == 0 {
		return bufferItem[T]{}, false
	}

	item = d.errorQueue.PopFront()
	atomic.StoreInt64(&d.errorQueueLen, int64(d.errorQueue.Len()))

	return item, true
}

// Add to the error queue
func (d *RetryPool[T]) addToErrorQueue(item bufferItem[T]) {
	d.muErrorQueue.Lock()
	item.retry = true
	d.errorQueue.PushBack(item)
	atomic.StoreInt64(&d.errorQueueLen, int64(d.errorQueue.Len()))
	d.muErrorQueue.Unlock()
}

func (d *RetryPool[T]) getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d.opts.processingDelay)
}

// Execute the handler with panic recovery
func (d *RetryPool[T]) execWithRecover(ctx context.Context, v T, age time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if d.logPanicFunc != nil {
				d.logPanicFunc(ctx, r, debug.Stack())
			}

			err = fmt.Errorf("panic in work function recovered: %v", r)
		}
	}()

	return d.workFunc(ctx, v, age)
}

func (d *RetryPool[T]) timeToMaxMetric(age time.Duration) {
	// These are not entirely correct calculations, as otherwise we would have to iterate over all the data in the error queue
	// But it's sufficient for monitoring purposes, as it's better than not showing the situation at all
	if (age == 0 && d.RetryQueueLen() == 0) || atomic.LoadInt64(&d.maxAge) < int64(age) {
		atomic.StoreInt64(&d.maxAge, int64(age))
	}

	if d.metricTimeToMaxFunc != nil {
		d.metricTimeToMaxFunc(100.0 * float64(atomic.LoadInt64(&d.maxAge)) / float64(d.opts.maxAge))
	}
}
