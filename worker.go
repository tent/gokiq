package gokiq

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Job struct {
	Type  string        `json:"class"`
	Args  []interface{} `json:"args"`
	Queue string        `json:"queue"`
	ID    string        `json:"jid"`

	Retry      interface{} `json:"retry"` // can be int (number of retries) or bool (true is default number)
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
	redisTimeout    = 10
	maxIdleRedis    = 1
)

type QueueConfig map[string]int

type Worker interface {
	Perform([]interface{}) error
}

var Workers = NewWorkerConfig()

type WorkerConfig struct {
	RedisServer  string
	Queues       QueueConfig
	WorkerCount  int
	ErrorHandler func(error)

	workerMapping map[string]reflect.Type
	randomQueues  []string
	redisPool     *redis.Pool
}

func NewWorkerConfig() *WorkerConfig {
	return &WorkerConfig{workerMapping: make(map[string]reflect.Type), ErrorHandler: func(error) {}}
}

func (w *WorkerConfig) Register(name string, worker Worker) {
	w.workerMapping[name] = reflect.Indirect(reflect.ValueOf(worker)).Type()
}

func (w *WorkerConfig) Run() {
	workQueue := make(chan message)
	w.denormalizeQueues()
	w.connectRedis()

	for i := 0; i < w.WorkerCount; i++ {
		go w.worker(workQueue)
	}

	go w.retryScheduler()
	//go w.quitHandler()

	for {
		msg, err := redis.Bytes(w.redisQuery("BLPOP", append(w.queueList(), redisTimeout)))
		if err != nil {
			w.handleError(err)
			continue
		}

		job := &Job{}
		json.Unmarshal(msg, job)
		if err != nil {
			w.handleError(err)
			continue
		}

		workQueue <- message{job: job}
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

func (w *WorkerConfig) worker(incoming chan message) {
	for msg := range incoming {
		if msg.die {
			return
		}

		typ, ok := w.workerMapping[msg.job.Type]
		if !ok {
			// fail
		}
		reflect.New(typ).Interface().(Worker).Perform(msg.job.Args)
		// handle errors
	}
}

// create a slice of queues with duplicates using the assigned frequencies
func (w *WorkerConfig) denormalizeQueues() {
	for queue, x := range w.Queues {
		for i := 0; i < x; i++ {
			w.randomQueues = append(w.randomQueues, queue)
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
	// TODO: sleep if redis connection error and try reconnect?
	w.ErrorHandler(err)
}

func (w *WorkerConfig) retryScheduler() {
	for _ = range time.Tick(1 * time.Second) {

	}
}
