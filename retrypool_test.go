package retrypool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	inboundQueueSize = 2000
	errorQueueSize   = 2000
	retryDelay       = time.Millisecond
	maxRetryDelay    = time.Millisecond * 100
	processingDelay  = time.Second
	workerCount      = 2

	keepAge        = time.Second
	addWorkerCount = 4
	addOpsCount    = 200
)

// Add data and immediately terminate the work. There should be no unprocessed data remaining upon termination.
func Test_RetryPoolDrain(t *testing.T) {
	t.Parallel()

	mutex := sync.Mutex{}
	checkMap := make(map[int]bool)
	hasError := int32(1)

	workFunc := func(ctx context.Context, v int, age time.Duration) error {
		if v%2 == 0 && atomic.LoadInt32(&hasError) > 0 {
			return fmt.Errorf("error %d", v)
		}
		mutex.Lock()
		delete(checkMap, v)
		mutex.Unlock()
		time.Sleep(time.Millisecond * 10)
		return nil
	}

	errorFunc := func(err error, v int, age time.Duration, closing bool) bool {
		return age <= keepAge
	}

	pool := New(inboundQueueSize, errorQueueSize, workerCount, workFunc, errorFunc,
		WithMaxAge[int](time.Minute),
		WithRetryDelay[int](retryDelay),
		WithMaxRetryDelay[int](maxRetryDelay),
		WithProcessingDelay[int](processingDelay))

	wg := &sync.WaitGroup{}
	wg.Add(addWorkerCount)
	counter := 0

	for i := 0; i < addWorkerCount; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < addOpsCount; n++ {
				mutex.Lock()
				counter++
				c := counter
				checkMap[counter] = true
				mutex.Unlock()
				if !pool.Add(c) {
					panic("please increase InboundQueueSize")
				}
			}
		}()
	}
	wg.Wait()

	atomic.StoreInt32(&hasError, 0)

	mutex.Lock()
	checkMap[counter+1] = true
	mutex.Unlock()
	require.NoError(t, pool.Exec(context.Background(), counter+1))

	pool.Stop()
	require.Empty(t, checkMap)

	require.Equal(t, pool.errorQueue.Len(), 0)
}

// Add data and let it work. There should be no unprocessed data remaining upon termination.
func Test_RetryPoolProcess(t *testing.T) {
	t.Parallel()

	const tooOldTarget = 10

	mutex := sync.Mutex{}
	checkMap := make(map[int]bool)
	var tooOldCount int64
	hasError := int32(1)

	workFunc := func(ctx context.Context, v int, age time.Duration) error {
		if v >= 1 && v <= tooOldTarget && atomic.LoadInt32(&hasError) > 0 {
			return fmt.Errorf("error %d", v)
		}

		mutex.Lock()
		delete(checkMap, v)
		mutex.Unlock()

		time.Sleep(time.Millisecond * 10)

		return nil
	}

	errorFunc := func(err error, v int, age time.Duration, closing bool) bool {
		if age <= keepAge {
			return true
		}

		atomic.AddInt64(&tooOldCount, 1)
		return false
	}

	pool := New(inboundQueueSize, errorQueueSize, workerCount, workFunc, errorFunc,
		WithMaxAge[int](time.Minute),
		WithRetryDelay[int](retryDelay),
		WithMaxRetryDelay[int](maxRetryDelay),
		WithProcessingDelay[int](processingDelay))

	wg := &sync.WaitGroup{}
	wg.Add(addWorkerCount)
	counter := 0

	for i := 0; i < addWorkerCount; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < addOpsCount; n++ {
				mutex.Lock()
				counter++
				c := counter
				checkMap[counter] = true
				mutex.Unlock()
				if !pool.Add(c) {
					panic("please increase InboundQueueSize")
				}
			}
		}()
	}
	wg.Wait()

	time.Sleep(time.Second * 2)

	pool.Stop()

	require.EqualValues(t, tooOldTarget, len(checkMap))
	require.EqualValues(t, 0, pool.errorQueue.Len())
	require.EqualValues(t, tooOldTarget, tooOldCount)
}

// Check for inbound queue overflow
func Test_RetryPoolInboundOverflow(t *testing.T) {
	t.Parallel()

	mutex := sync.Mutex{}
	checkMap := make(map[int]bool)

	workFunc := func(ctx context.Context, v int, age time.Duration) error {
		mutex.Lock()
		delete(checkMap, v)
		mutex.Unlock()

		if v >= 1 && v <= 10 {
			return fmt.Errorf("error %d", v)
		}

		time.Sleep(time.Millisecond * 10)

		return nil
	}
	errorFunc := func(err error, v int, age time.Duration, closing bool) bool {
		return true
	}

	pool := New(10, errorQueueSize, workerCount, workFunc, errorFunc,
		WithMaxAge[int](time.Minute),
		WithRetryDelay[int](retryDelay),
		WithMaxRetryDelay[int](maxRetryDelay),
		WithProcessingDelay[int](processingDelay))

	wg := &sync.WaitGroup{}
	wg.Add(addWorkerCount)
	counter := 0

	var isOverflow int32

	for i := 0; i < addWorkerCount; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < addOpsCount; n++ {
				mutex.Lock()
				counter++
				c := counter
				mutex.Unlock()

				if !pool.Add(c) {
					atomic.StoreInt32(&isOverflow, 1)
				} else {
					mutex.Lock()
					checkMap[c] = true
					mutex.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	time.Sleep(time.Second * 2)

	pool.Stop()

	require.Empty(t, checkMap)
	require.EqualValues(t, 0, pool.errorQueue.Len())
	require.EqualValues(t, 1, atomic.LoadInt32(&isOverflow))
}

// Check for error queue overflow
func Test_RetryPoolErrorOverflow(t *testing.T) {
	t.Parallel()

	mutex := sync.Mutex{}
	checkMap := make(map[int]bool)

	workFunc := func(ctx context.Context, v int, age time.Duration) error {
		mutex.Lock()
		delete(checkMap, v)
		mutex.Unlock()

		if v >= 1 && v <= 10 {
			return fmt.Errorf("error %d", v)
		}

		time.Sleep(time.Millisecond * 10)

		return nil
	}
	errorFunc := func(err error, v int, age time.Duration, closing bool) bool {
		return true
	}

	pool := New(inboundQueueSize, 1, workerCount, workFunc, errorFunc,
		WithMaxAge[int](time.Minute),
		WithRetryDelay[int](retryDelay),
		WithMaxRetryDelay[int](maxRetryDelay),
		WithProcessingDelay[int](processingDelay))

	wg := &sync.WaitGroup{}
	wg.Add(addWorkerCount)
	counter := 0

	var isOverflow int32

	for i := 0; i < addWorkerCount; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < addOpsCount; n++ {
				mutex.Lock()
				counter++
				c := counter
				mutex.Unlock()

				if !pool.Add(c) {
					atomic.StoreInt32(&isOverflow, 1)
				} else {
					mutex.Lock()
					checkMap[c] = true
					mutex.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	time.Sleep(time.Second * 2)

	pool.Stop()

	require.Empty(t, checkMap)
	require.EqualValues(t, 0, pool.errorQueue.Len())
	require.EqualValues(t, 1, atomic.LoadInt32(&isOverflow))
}

// Sending errors after recovery
func Test_RetryPoolResend(t *testing.T) {
	t.Parallel()

	hasError := int32(1)

	workFunc := func(ctx context.Context, v int64, age time.Duration) error {
		if v%2 == 0 && atomic.LoadInt32(&hasError) > 0 {
			return fmt.Errorf("error %d", v)
		}
		return nil
	}
	errorFunc := func(err error, v int64, age time.Duration, closing bool) bool {
		return true
	}

	pool := New(inboundQueueSize, errorQueueSize, workerCount, workFunc, errorFunc,
		WithMaxAge[int64](time.Minute),
		WithRetryDelay[int64](time.Millisecond*100),
		WithMaxRetryDelay[int64](time.Millisecond*200),
		WithProcessingDelay[int64](processingDelay))

	wg := &sync.WaitGroup{}
	wg.Add(addWorkerCount)
	counter := int64(0)

	for i := 0; i < addWorkerCount; i++ {
		go func() {
			defer wg.Done()
			for n := 0; n < addOpsCount; n++ {
				if !pool.Add(atomic.AddInt64(&counter, 1)) {
					panic("please increase InboundQueueSize")
				}
			}
		}()
	}
	wg.Wait()

	time.Sleep(pool.MaxRetryDelay())
	require.NotEqualValues(t, 0, pool.errorQueue.Len())

	atomic.StoreInt32(&hasError, 0)
	time.Sleep(pool.MaxRetryDelay() * 3)
	require.EqualValues(t, 0, pool.errorQueue.Len())

	require.EqualValues(t, int64(pool.RetryDelay()), atomic.LoadInt64(&pool.currentErrorRetryDelay))

	pool.Stop()
}
