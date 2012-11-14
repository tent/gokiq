package gokiq

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Job struct {
	Type  string        `json:"class"`
	Args  []interface{} `json:"args"`
	Queue string        `json:"queue"`
	ID    string        `json:"jid"`

	Retry      interface{} `json:"retry"` // can be int (number of retries) or bool (true means default)
	MaxRetries int
	RetryCount int `json:"retry_count,omitmepty"`
	RetriedAt  int `json:"retried_at,omitempty"`

	ErrorMessage string `json:"error_message,omitempty"`
	ErrorType    string `json:"error_class,omitempty"`
	FailedAt     string `json:"failed_at,omitempty"`
}

type message struct {
	job *Job
	die bool
}

const (
	TimestampFormat = "2006-01-02 15:04:05 MST"
	redisTimeout    = 1
	maxIdleRedis    = 1
)

type QueueConfig map[string]int

type Worker interface {
	Perform([]interface{}) error
}

var Workers = NewWorkerConfig()

type WorkerConfig struct {
	RedisServer string
	Queues      QueueConfig
	WorkerCount int
	ReportError func(error, *Job)

	workerMapping map[string]reflect.Type
	randomQueues  []string
	redisPool     *redis.Pool
	workQueue     chan message
	doneQueue     chan bool
	done          bool
	sync.Mutex
}

func NewWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		ReportError:   func(error, *Job) {},
		workerMapping: make(map[string]reflect.Type),
		workQueue:     make(chan message),
		doneQueue:     make(chan bool),
	}
}

func (w *WorkerConfig) Register(name string, worker Worker) {
	w.workerMapping[name] = reflect.Indirect(reflect.ValueOf(worker)).Type()
}

func (w *WorkerConfig) Run() {
	w.denormalizeQueues()
	w.connectRedis()

	for i := 0; i < w.WorkerCount; i++ {
		go w.worker()
	}

	go w.retryScheduler()
	go w.quitHandler()

	for {
		if w.done {
			w.Lock() // we're done, so block until quitHandler() calls os.Exit()
		}

		w.Lock() // don't let quitHandler() stop us in the middle of the iteration
		msg, err := redis.Bytes(w.redisQuery("BLPOP", append(w.queueList(), redisTimeout)))
		if err != nil {
			w.handleError(err)
			time.Sleep(redisTimeout * time.Second) // likely a transient redis error, sleep before retrying
			continue
		}

		job := &Job{}
		json.Unmarshal(msg, job)
		if err != nil {
			w.handleError(err)
			continue
		}

		w.workQueue <- message{job: job}
		w.Unlock()
	}
}

// create a slice of queues with duplicates using the assigned frequencies
func (w *WorkerConfig) denormalizeQueues() {
	for queue, x := range w.Queues {
		for i := 0; i < x; i++ {
			w.randomQueues = append(w.randomQueues, "queue:"+queue)
		}
	}
}

// get a random slice of unique queues from the slice of denormalized queues
func (w *WorkerConfig) queueList() []interface{} {
	size := len(w.Queues)
	res := make([]interface{}, 0, size)
	queues := make(map[string]bool, size)

	indices := rand.Perm(len(w.randomQueues))[:size]
	for _, i := range indices {
		queue := w.randomQueues[i]
		if _, ok := queues[queue]; !ok {
			queues[queue] = true
			res = append(res, queue)
		}
	}

	return res
}

func (w *WorkerConfig) handleError(err error) {
	// TODO: log message to stdout
	w.ReportError(err, nil)
}

// checks the sorted set of scheduled retries and queues them when it's time
func (w *WorkerConfig) retryScheduler() {
	for _ = range time.Tick(time.Second) {

	}
}

// listens for SIGINT, SIGTERM, and SIGQUIT to perform a clean shutdown
func (w *WorkerConfig) quitHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGQUIT)

	for _ = range c {
		w.Lock()           // wait for the current runner iteration to finish
		w.done = true      // tell the runner that we're done
		close(w.workQueue) // tell worker goroutines to stop after they finish their current job
		for i := 0; i < w.WorkerCount; i++ {
			<-w.doneQueue // wait for workers to finish
		}
		os.Exit(0)
	}
}

func (w *WorkerConfig) connectRedis() {
	w.redisPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", w.RedisServer)
	}, maxIdleRedis)
}

func (w *WorkerConfig) redisQuery(command string, args ...interface{}) (interface{}, error) {
	conn := w.redisPool.Get()
	defer conn.Close()
	return conn.Do(command, args...)
}

func (w *WorkerConfig) worker() {
	for msg := range w.workQueue {
		if msg.die {
			return
		}

		job := msg.job
		typ, ok := w.workerMapping[msg.job.Type]
		if !ok {
			err := fmt.Errorf("Unknown worker type: %s", job.Type)
			w.scheduleRetry(job, err)
			continue
		}

		// wrap Perform() in a function so that we can recover from panics
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// TODO: log stack trace
					err = panicToError(r)
				}
			}()
			err = reflect.New(typ).Interface().(Worker).Perform(msg.job.Args)
		}()
		if err != nil {
			w.scheduleRetry(job, err)
		}
	}
	w.doneQueue <- true
}

func (w *WorkerConfig) scheduleRetry(job *Job, err error) {
	w.ReportError(err, job)
	// set failure details
	// determine next retry and add to retry set
	// log failure stat
}

func panicToError(err interface{}) error {
	if str, ok := err.(string); ok {
		return fmt.Errorf(str)
	}
	return err.(error)
}
