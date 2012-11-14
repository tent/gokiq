package gokiq

import (
	"encoding/json"
	"fmt"
	"math"
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

func (job *Job) FromJSON(data []byte) error {
	err := json.Unmarshal(data, job)
	if err != nil {
		return err
	}
	if max, ok := job.Retry.(float64); ok {
		job.MaxRetries = int(max)
	} else if r, ok := job.Retry.(bool); ok && !r {
	} else {
		job.MaxRetries = defaultMaxRetries
	}
	return nil
}

func (job *Job) JSON() []byte {
	res, _ := json.Marshal(job)
	return res
}

type message struct {
	job *Job
	die bool
}

const (
	TimestampFormat     = "2006-01-02 15:04:05 MST"
	redisTimeout        = 1
	maxIdleRedis        = 1
	defaultMaxRetries   = 25
	defaultPollInterval = 5
)

type QueueConfig map[string]int

type Worker interface {
	Perform([]interface{}) error
}

var Workers = NewWorkerConfig()

type WorkerConfig struct {
	RedisServer    string
	RedisNamespace string
	Queues         QueueConfig
	WorkerCount    int
	PollInterval   int
	ReportError    func(error, *Job)

	workerMapping map[string]reflect.Type
	randomQueues  []string
	redisPool     *redis.Pool
	workQueue     chan message
	doneQueue     chan bool
	sync.RWMutex  // R is locked by Run() and scheduler(), W is locked by quitHandler() when it receives a signal
}

func NewWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		PollInterval:  defaultPollInterval,
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

	go w.scheduler()
	go w.quitHandler()

	for {
		w.RLock() // don't let quitHandler() stop us in the middle of a job
		msg, err := redis.Bytes(w.redisQuery("BLPOP", append(w.queueList(), redisTimeout)))
		if err == redis.ErrNil {
			continue
		}
		if err != nil {
			w.handleError(err)
			time.Sleep(redisTimeout * time.Second) // likely a transient redis error, sleep before retrying
			continue
		}

		job := &Job{}
		err = job.FromJSON(msg)
		if err != nil {
			w.handleError(err)
			continue
		}
		w.workQueue <- message{job: job}

		w.RUnlock()
	}
}

// create a slice of queues with duplicates using the assigned frequencies
func (w *WorkerConfig) denormalizeQueues() {
	for queue, x := range w.Queues {
		for i := 0; i < x; i++ {
			w.randomQueues = append(w.randomQueues, w.nsKey("queue:"+queue))
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

// checks the sorted set of scheduled jobs and retries and queues them when it's time
// TODO: move this to a Lua script
func (w *WorkerConfig) scheduler() {
	pollSets := []string{w.nsKey("retry"), w.nsKey("schedule")}

	for _ = range time.Tick(time.Duration(w.PollInterval) * time.Second) {
		w.RLock() // don't let quitHandler() stop us in the middle of a run
		conn := w.redisPool.Get()
		now := fmt.Sprintf("%f", currentTimeFloat())
		for _, set := range pollSets {
			conn.Send("MULTI")
			conn.Send("ZRANGEBYSCORE", set, "-inf", now)
			conn.Send("ZREMRANGEBYSCORE", set, "-inf", now)
			res, err := redis.Values(conn.Do("EXEC"))
			if err == redis.ErrNil { // TODO: check if this is possible
				continue
			}
			if err != nil {
				w.handleError(err)
				continue
			}

			messages, _ := redis.Values(res, nil)
			if messages == nil { // TODO: check if this is possible
				continue
			}

			for _, msg := range messages {
				parsedMsg := &struct{ queue string }{}
				msgBytes := msg.([]byte)
				err := json.Unmarshal(msgBytes, parsedMsg)
				if err != nil {
					w.handleError(err)
					continue
				}
				conn.Send("MULTI")
				conn.Send("SADD", w.nsKey("queues"), parsedMsg.queue)
				conn.Send("RPUSH", w.nsKey("queue:"+parsedMsg.queue), msgBytes)
				_, err = conn.Do("EXEC")
				if err != nil {
					w.handleError(err)
				}
			}
		}
		conn.Close()
		w.RUnlock()
	}
}

// listens for SIGINT, SIGTERM, and SIGQUIT to perform a clean shutdown
func (w *WorkerConfig) quitHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGQUIT)

	for _ = range c {
		w.Lock()           // wait for the current run loop and scheduler iterations to finish
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

	job.RetryCount += 1

	if job.RetryCount < job.MaxRetries {
		job.ErrorType = fmt.Sprintf("%T", err)
		job.ErrorMessage = err.Error()
		job.FailedAt = time.Now().UTC().Format(TimestampFormat)

		nextRetry := currentTimeFloat() + retryDelay(job.RetryCount)

		w.redisQuery("ZADD", w.nsKey("retry"), fmt.Sprintf("%f", nextRetry), job.JSON())
	}
}

func (w *WorkerConfig) nsKey(key string) string {
	if w.RedisNamespace != "" {
		return w.RedisNamespace + ":" + key
	}
	return key
}

// formula from Sidekiq (originally from delayed_job)
func retryDelay(count int) float64 {
	return math.Pow(float64(count), 4) + 15 + (float64(rand.Intn(30)) * (float64(count) + 1))
}

func currentTimeFloat() float64 {
	return float64(time.Now().UnixNano()) / float64(time.Second)
}

func panicToError(err interface{}) error {
	if str, ok := err.(string); ok {
		return fmt.Errorf(str)
	}
	return err.(error)
}
