package gokiq

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	. "launchpad.net/gocheck"
)

// Hook gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type WorkerSuite struct{}

var _ = Suite(&WorkerSuite{})

var workChan = make(chan struct{})

type TestWorker struct {
	Data []string `json:"args"`
	Job  *Job     `json:"-"`
}

func (w *TestWorker) Perform(job *Job) error {
	if w.Data[0] == "foo" && job != nil {
		workChan <- struct{}{}
	} else {
		fmt.Printf("error: %#v %#v\n", w.Data, job)
	}
	return nil
}

func (w *TestWorker) Args() interface{} { return w.Args }

func MaybeFail(c *C, err error) {
	if err != nil {
		c.Log(err)
		c.FailNow()
	}
}

func (s *WorkerSuite) SetUpSuite(c *C) {
	Workers.Register(&TestWorker{})
	Workers.ReportError = func(err error, job *Job) {
		fmt.Printf("%#v\n", err)
	}
}

func (s *WorkerSuite) TestWorkerLoop(c *C) {
	go Workers.worker("a")

	data := json.RawMessage([]byte(`{"args":["foo"]}`))
	job := &Job{
		Type:  "TestWorker",
		Args:  &data,
		Queue: "default",
		ID:    "123",
		Retry: false,
	}

	Workers.workQueue <- message{job: job}

	select {
	case <-workChan:
	case <-time.After(time.Second):
		c.Error("assertion timeout")
	}
}

var RetryParseTests = []struct {
	json     string
	expected int
}{
	{`{"retry": false}`, 0},
	{`{"retry": true}`, 25},
	{`{"retry": 5}`, 5},
	{`{"retry": "foo"}`, 25},
}

func (s *WorkerSuite) TestJobRetryParsing(c *C) {
	for _, test := range RetryParseTests {
		job := &Job{}
		err := job.FromJSON([]byte(test.json))
		MaybeFail(c, err)
		c.Assert(job.MaxRetries, Equals, test.expected)
	}
}

func (s *WorkerSuite) TestJobRedisLogging(c *C) {
	job := &Job{
		Type:  "TestWorker",
		Queue: "default",
		ID:    "123",
		Retry: true,
	}

	_, err := Workers.redisQuery("FLUSHDB")
	MaybeFail(c, err)

	Workers.trackJobStart(job, "test")

	isMember, err := redis.Bool(Workers.redisQuery("SISMEMBER", "workers", "test"))
	MaybeFail(c, err)
	c.Assert(isMember, Equals, true)

	timestamp, err := redis.Bytes(Workers.redisQuery("GET", "worker:test:started"))
	MaybeFail(c, err)
	if len(timestamp) < 29 {
		c.Fatalf("Expected %#v to be a timestamp", timestamp)
	}

	msg, err := redis.Bytes(Workers.redisQuery("GET", "worker:test"))
	MaybeFail(c, err)
	jobMsg := &runningJob{}
	err = json.Unmarshal(msg, jobMsg)
	MaybeFail(c, err)
	c.Assert(jobMsg.Queue, Equals, "default")
	c.Assert(jobMsg.Job.ID, Equals, "123")
	c.Assert(jobMsg.Timestamp, Not(Equals), 0)

	Workers.trackJobFinish(job, "test", false)

	isMember, err = redis.Bool(Workers.redisQuery("SISMEMBER", "workers", "test"))
	MaybeFail(c, err)
	c.Assert(isMember, Equals, false)

	ts, err := Workers.redisQuery("GET", "worker:test:started")
	MaybeFail(c, err)
	c.Assert(ts, IsNil)

	mg, err := Workers.redisQuery("GET", "worker:test")
	MaybeFail(c, err)
	c.Assert(mg, IsNil)
}

func init() {
	log.SetOutput(ioutil.Discard)
}
