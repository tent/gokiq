package gokiq

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Job struct {
	Type  string           `json:"class"`
	Args  *json.RawMessage `json:"args"`
	Queue string           `json:"queue,omitempty"`
	ID    string           `json:"jid"`

	Retry interface{} `json:"retry"` // can be int (number of retries) or bool (true means default)

	MaxRetries   int    `json:"-"`
	RetryCount   int    `json:"retry_count"`
	ErrorMessage string `json:"error_message,omitempty"`
	ErrorType    string `json:"error_class,omitempty"`
	RetriedAt    string `json:"retried_at,omitempty"`
	FailedAt     string `json:"failed_at,omitempty"`

	StartTime time.Time `json:"-"`
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
	dateFormat          = "2006-01-02"
	redisTimeout        = 1
	defaultMaxRetries   = 25
	defaultPollInterval = 5 * time.Second
	defaultStopTimeout  = 8 * time.Second
	defaultWorkerCount  = 25
	defaultRedisServer  = "127.0.0.1:6379"
	keyExpiry           = 86400 // one day
)

type QueueConfig map[string]int

func (q QueueConfig) String() string {
	str := ""
	for queue, priority := range q {
		str += fmt.Sprintf("%s=%d,", queue, priority)
	}
	return str[:len(str)-1]
}

type Worker interface {
	Perform() error
}

type ReportableErrorChecker interface {
	ReportableError(error) bool
}

var Workers = NewWorkerConfig()

type WorkerConfig struct {
	RedisPool      *redis.Pool
	RedisNamespace string
	Queues         QueueConfig
	WorkerCount    int
	PollInterval   time.Duration
	StopTimeout    time.Duration
	ReportError    func(error, *Job)

	// worker id -> job mapping
	work    map[string]*Job
	workMtx sync.Mutex

	workerMapping map[string]reflect.Type
	randomQueues  []string
	workQueue     chan message
	done          sync.WaitGroup
	sync.RWMutex  // R is locked by Run() and scheduler(), W is locked by quitHandler() when it receives a signal
}

func NewWorkerConfig() *WorkerConfig {
	w := &WorkerConfig{
		PollInterval:  defaultPollInterval,
		StopTimeout:   defaultStopTimeout,
		WorkerCount:   defaultWorkerCount,
		Queues:        QueueConfig{"default": 1},
		ReportError:   func(error, *Job) {},
		workerMapping: make(map[string]reflect.Type),
		workQueue:     make(chan message),
		work:          make(map[string]*Job),
	}
	w.RedisPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", defaultRedisServer)
	}, w.WorkerCount+1)
	return w
}

func Register(worker Worker, queue string, retries int) {
	Client.Register(worker, queue, retries)
	Workers.Register(worker)
	Workers.Queues[queue] = 1
}

func (w *WorkerConfig) Register(worker Worker) {
	t := workerType(worker)
	w.workerMapping[t.Name()] = t
}

func (w *WorkerConfig) RegisterName(name string, worker Worker) {
	w.workerMapping[name] = workerType(worker)
}

func (w *WorkerConfig) Run() {
	log.Printf("state=starting worker_count=%d queues=%q pid=%d", w.WorkerCount, w.Queues, pid)
	w.denormalizeQueues()

	for i := 0; i < w.WorkerCount; i++ {
		go w.worker(workerID(i))
	}
	w.done.Add(w.WorkerCount)

	go w.scheduler()
	go w.quitHandler()

	log.Printf(`state=started pid=%d`, pid)
	for {
		w.run()
	}
}

func (w *WorkerConfig) run() {
	w.RLock() // don't let quitHandler() stop us in the middle of a job
	defer w.RUnlock()

	msg, err := redis.Values(w.redisQuery("BLPOP", append(w.queueList(), redisTimeout)...))
	if err == redis.ErrNil {
		return
	}
	if err != nil {
		w.handleError(err)
		time.Sleep(redisTimeout * time.Second) // likely a transient redis error, sleep before retrying
		return
	}

	job := &Job{}
	err = job.FromJSON(msg[1].([]byte))
	if err != nil {
		w.handleError(err)
		return
	}
	job.Queue = string(msg[0].([]byte)[len(w.nsKey("queue:")):])
	w.workQueue <- message{job: job}
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
	queues := make(map[string]struct{}, size)

	indices := rand.Perm(len(w.randomQueues))[:size]
	for _, i := range indices {
		queue := w.randomQueues[i]
		if _, ok := queues[queue]; !ok {
			queues[queue] = struct{}{}
			res = append(res, queue)
		}
	}

	return res
}

func (w *WorkerConfig) handleError(err error) {
	log.Printf(`event=error error_type=%T error_message="%s" pid=%d`, err, err, pid)
	w.ReportError(err, nil)
}

// checks the sorted set of scheduled jobs and retries and queues them when it's time
// TODO: move this to a Lua script
func (w *WorkerConfig) scheduler() {
	pollSets := []string{w.nsKey("retry"), w.nsKey("schedule")}

	for _ = range time.Tick(w.PollInterval) {
		w.RLock() // don't let quitHandler() stop us in the middle of a run
		conn := w.RedisPool.Get()
		now := fmt.Sprintf("%f", timeFloat(time.Now()))
		for _, set := range pollSets {
			conn.Send("MULTI")
			conn.Send("ZRANGEBYSCORE", set, "-inf", now)
			conn.Send("ZREMRANGEBYSCORE", set, "-inf", now)
			res, err := redis.Values(conn.Do("EXEC"))
			if err != nil {
				w.handleError(err)
				continue
			}

			for _, msg := range res[0].([]interface{}) {
				parsedMsg := &struct {
					Queue string `json:"queue"`
				}{}
				msgBytes := msg.([]byte)
				err := json.Unmarshal(msgBytes, parsedMsg)
				if err != nil {
					w.handleError(err)
					continue
				}
				if _, err = conn.Do("RPUSH", w.nsKey("queue:"+parsedMsg.Queue), msgBytes); err != nil {
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

	sig := <-c
	signal.Stop(c)
	log.Printf("state=stopping signal=%s pid=%d", sig, pid)
	w.Lock()           // wait for the current run loop and scheduler iterations to finish
	close(w.workQueue) // tell worker goroutines to stop after they finish their current job
	w.clearWorkerSet()
	done := make(chan struct{})
	go func() {
		w.done.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(w.StopTimeout):
		log.Printf("state=stop_timeout timeout=%s pid=%d", w.StopTimeout, pid)
		w.requeueJobs()
	}
	log.Printf("state=stopped pid=%d", pid)
	os.Exit(0)
}

func (w *WorkerConfig) clearWorkerSet() {
	key := w.nsKey("workers")
	res, _ := redis.Strings(w.redisQuery("SMEMBERS", key))
	workerIDs := make([]interface{}, 1, w.WorkerCount+1)
	substr := ":" + strconv.Itoa(pid) + "-"
	workerIDs[0] = key
	for _, s := range res {
		if strings.Contains(s, substr) {
			workerIDs = append(workerIDs, s)
		}
	}
	if len(workerIDs) > 1 {
		w.redisQuery("SREM", workerIDs...)
	}
}

func (w *WorkerConfig) requeueJobs() {
	w.workMtx.Lock()
	jobQueues := make(map[string][]*Job)
	workers := make(map[*Job]string)
	for worker, job := range w.work {
		workers[job] = worker
		jobQueues[job.Queue] = append(jobQueues[job.Queue], job)
	}
	w.workMtx.Unlock()

	for queue, jobs := range jobQueues {
		jobJSON := make([]interface{}, len(jobs)+1)
		for i, job := range jobs {
			jobJSON[i+1] = job.JSON()
		}
		jobJSON[0] = w.nsKey("queue:" + queue)
		_, err := w.redisQuery("RPUSH", jobJSON...)
		for _, job := range jobs {
			log.Printf("event=job_requeue job_id=%s job_type=%s queue=%s success=%t worker_id=%s pid=%d", job.ID, job.Type, queue, err == nil, workers[job], pid)
		}
	}
}

func (w *WorkerConfig) redisQuery(command string, args ...interface{}) (interface{}, error) {
	conn := w.RedisPool.Get()
	defer conn.Close()
	return conn.Do(command, args...)
}

var typeOfJob = reflect.TypeOf((*Job)(nil))

func setJob(worker Worker, job *Job) {
	val := reflect.ValueOf(worker)
	if val.Kind() != reflect.Ptr {
		return
	}
	wstruct := val.Elem()
	if wstruct.Kind() != reflect.Struct {
		return
	}
	wtype := wstruct.Type()
	for i := 0; i < wtype.NumField(); i++ {
		field := wtype.Field(i)
		if field.Type == typeOfJob {
			wstruct.Field(i).Set(reflect.ValueOf(job))
			break
		}
	}
}

func (w *WorkerConfig) worker(id string) {
	for msg := range w.workQueue {
		if msg.die {
			break
		}

		job := msg.job
		typ, ok := w.workerMapping[msg.job.Type]
		if !ok {
			err := UnknownWorkerError{job.Type}
			w.scheduleRetry(job, err, true)
			continue
		}

		w.trackJobStart(job, id)

		// wrap Perform() in a function so that we can recover from panics
		var err error
		var worker Worker
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = newPanicError(r)
				}
			}()
			worker = reflect.New(typ).Interface().(Worker)
			err = json.Unmarshal(*job.Args, worker)
			if err != nil {
				return
			}
			setJob(worker, job)
			err = worker.Perform()
		}()
		if err != nil {
			report := true
			if checker, ok := worker.(ReportableErrorChecker); ok {
				report = checker.ReportableError(err)
			}
			w.scheduleRetry(job, err, report)
		}
		w.trackJobFinish(job, id, err == nil)
	}
	w.done.Done()
}

func (w *WorkerConfig) scheduleRetry(job *Job, err error, report bool) {
	if report {
		w.ReportError(err, job)
	}

	now := time.Now().UTC().Format(TimestampFormat)
	if job.FailedAt == "" {
		job.FailedAt = now
	} else {
		job.RetryCount += 1
	}
	if job.RetryCount > 0 {
		job.RetriedAt = now
	}

	log.Printf("event=job_error job_id=%s job_type=%s queue=%s retries=%d max_retries=%d error_type=%T error_message=%q pid=%d", job.ID, job.Type, job.Queue, job.RetryCount, job.MaxRetries, err, err, pid)

	if job.RetryCount < job.MaxRetries {
		job.ErrorType = fmt.Sprintf("%T", err)
		job.ErrorMessage = err.Error()

		nextRetry := timeFloat(time.Now()) + retryDelay(job.RetryCount)

		w.redisQuery("ZADD", w.nsKey("retry"), strconv.FormatFloat(nextRetry, 'f', -1, 64), job.JSON())
	}
}

type runningJob struct {
	Queue     string `json:"queue"`
	Job       *Job   `json:"payload"`
	Timestamp int64  `json:"run_at"`
}

func (w *WorkerConfig) trackJobStart(job *Job, workerID string) {
	conn := w.RedisPool.Get()
	defer conn.Close()

	w.workMtx.Lock()
	w.work[workerID] = job
	w.workMtx.Unlock()

	conn.Send("MULTI")
	conn.Send("SADD", w.nsKey("workers"), workerID)
	conn.Send("SETEX", w.nsKey("worker:"+workerID+":started"), keyExpiry, time.Now().UTC().String())
	payload := &runningJob{job.Queue, job, time.Now().Unix()}
	json, _ := json.Marshal(payload)
	conn.Send("SETEX", w.nsKey("worker:"+workerID), keyExpiry, json)
	_, err := conn.Do("EXEC")
	if err != nil {
		w.handleError(err)
	}

	job.StartTime = time.Now()
	log.Printf("event=job_start job_id=%s job_type=%s queue=%s worker_id=%s pid=%d", job.ID, job.Type, job.Queue, workerID, pid)
}

func (w *WorkerConfig) trackJobFinish(job *Job, workerID string, success bool) {
	log.Printf("event=job_finish job_id=%s job_type=%s queue=%s duration=%v success=%t worker_id=%s pid=%d", job.ID, job.Type, job.Queue, time.Since(job.StartTime), success, workerID, pid)

	conn := w.RedisPool.Get()
	defer conn.Close()

	w.workMtx.Lock()
	delete(w.work, workerID)
	w.workMtx.Unlock()

	date := time.Now().Format(dateFormat)
	conn.Send("MULTI")
	conn.Send("SREM", w.nsKey("workers"), workerID)
	conn.Send("DEL", w.nsKey("worker:"+workerID+":started"))
	conn.Send("DEL", w.nsKey("worker:"+workerID))
	conn.Send("INCR", w.nsKey("stat:processed"))
	conn.Send("INCR", w.nsKey("stat:processed:"+date))
	if !success {
		conn.Send("INCR", w.nsKey("stat:failed"))
		conn.Send("INCR", w.nsKey("stat:failed:"+date))
	}
	_, err := conn.Do("EXEC")
	if err != nil {
		w.handleError(err)
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
	return math.Pow(float64(count), 4) + 15 + float64(rand.Intn(30)*(count+1))
}

func timeFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second)
}

type StackFrame struct {
	PC   uintptr
	File string
	Line int
}

type PanicError struct {
	Err   interface{}
	Stack []StackFrame
}

func (err *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", err.Err)
}

func newPanicError(v interface{}) *PanicError {
	err := &PanicError{Err: v}
	for i := 2; ; i++ {
		var frame StackFrame
		var ok bool
		frame.PC, frame.File, frame.Line, ok = runtime.Caller(i)
		if !ok {
			break
		}
		err.Stack = append(err.Stack, frame)
	}
	return err
}

var (
	pid         = os.Getpid()
	hostname, _ = os.Hostname()
)

func workerID(i int) string {
	return fmt.Sprintf("%s:%d-%d", hostname, pid, i)
}

func workerType(worker Worker) reflect.Type {
	return reflect.Indirect(reflect.ValueOf(worker)).Type()
}

type UnknownWorkerError struct{ Type string }

func (e UnknownWorkerError) Error() string {
	return "gokiq: Unknown worker type: " + e.Type
}
