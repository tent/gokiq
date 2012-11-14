package gokiq

import (
	"testing"

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
}

func (s *WorkerSuite) TestWorkerLoop(c *C) {
	testChan := make(chan bool)

	go Workers.worker()

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
