[![Go Reference](https://pkg.go.dev/badge/github.com/n-r-w/retrypool.svg)](https://pkg.go.dev/github.com/n-r-w/retrypool)
[![Go Coverage](https://github.com/n-r-w/retrypool/wiki/coverage.svg)](https://raw.githack.com/wiki/n-r-w/retrypool/coverage.html)
![CI Status](https://github.com/n-r-w/retrypool/actions/workflows/go.yml/badge.svg)
[![Stability](http://badges.github.io/stability-badges/dist/stable.svg)](http://github.com/badges/stability-badges)
[![Go Report](https://goreportcard.com/badge/github.com/n-r-w/retrypool)](https://goreportcard.com/badge/github.com/n-r-w/retrypool)

# RetryPool

The RetryPool package provides a mechanism for executing tasks with automatic retry and error handling capabilities. It allows you to define a work function that performs the actual task, an error handling function that determines whether to retry or stop based on the error, and a success handling function that is called when the task is successfully completed.

The RetryPool is created with a specified number of worker goroutines and a maximum number of tasks that can be queued. Tasks are added to the pool using the Add method, and the pool executes them concurrently. The pool automatically retries failed tasks based on the error handling function and configurable retry settings.

Its also supports various configuration options, such as maximum age for a task, retry delay, maximum retry delay, and processing delay. These options allow you to fine-tune the behavior of the RetryPool according to your specific requirements.

## Installation

```bash
go get github.com/n-r-w/retrypool
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/n-r-w/retrypool"
)

type Data struct {
    Value int
}

func main() {
    // Define your work function
    workFunc := func(ctx context.Context, v Data, age time.Duration) error {
        if age < time.Second/2 {
            fmt.Printf("Processing with error: %v after %v\n", v, age)
            return fmt.Errorf("error %v", v)
        }

        fmt.Printf("Processing: %v after %v\n", v, age)
        return nil
    }

    // Define your error handling function
    errorFunc := func(err error, v Data, age time.Duration, closing bool) bool {
        fmt.Printf("Error: %v after %v\n", v, age)

        if closing {
            fmt.Printf("Closing: %v after %v\n", v, age)
        }

        return age < time.Minute // Retry for a minute
    }

    // Define your success handling function
    successFunc := func(v Data, age time.Duration) {
        fmt.Printf("Success: %v after %v\n", v, age)
    }

    // Create a new RetryPool
    pool := retrypool.New(100, 100, 2, workFunc, errorFunc,
        retrypool.WithMaxAge[Data](time.Minute*10),
        retrypool.WithRetryDelay[Data](time.Second/2),
        retrypool.WithMaxRetryDelay[Data](time.Second),
        retrypool.WithProcessingDelay[Data](time.Hour),
        retrypool.WithSuccessFunc[Data](successFunc))

    // Add tasks to the pool
    pool.Add(Data{Value: 1})
    
    time.Sleep(time.Second * 2)

    pool.Stop()
}
```

Output:

```
Processing with error: {1} after 19.191µs
Error: {1} after 36.772µs
Processing: {1} after 1.000079775s
Success: {1} after 1.000079775s
```
