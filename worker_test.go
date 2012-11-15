package gokiq

import (
	"encoding/json"
	"testing"

	"github.com/garyburd/redigo/redis"
	. "launchpad.net/gocheck"
)

// Hook gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type WorkerSuite struct{}

var _ = Suite(&WorkerSuite{})

type TestWorker struct {
	Foo string
}

func (w *TestWorker) Perform(args []interface{}) error {
	args[0].(chan bool) <- args[1].(bool)
	return nil
}

func MaybeFail(c *C, err error) {
	if err != nil {
		c.Log(err)
		c.FailNow()
	}
}

func (s *WorkerSuite) SetUpSuite(c *C) {
	Workers.Register("TestWorker", &TestWorker{})
	Workers.connectRedis()
}

func (s *WorkerSuite) TestWorkerLoop(c *C) {
	testChan := make(chan bool)

	go Workers.worker("a")

	job := &Job{
		Type:  "TestWorker",
		Args:  []interface{}{testChan, true},
		Queue: "default",
		ID:    "123",
		Retry: false,
	}

	Workers.workQueue <- message{job: job}

	res := <-testChan
	c.Assert(res, Equals, true)
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
		Args:  []interface{}{"a"},
		Queue: "default",
		ID:    "123",
		Retry: true,
	}

	_, err := Workers.redisQuery("FLUSHDB")
	MaybeFail(c, err)

	Workers.logJobStart(job, "test")

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

	Workers.logJobFinish(job, "test", false)

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
