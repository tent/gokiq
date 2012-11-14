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

func (s *WorkerSuite) SetUpSuite(c *C) {
	Workers.Register("TestWorker", &TestWorker{})
}

func (s *WorkerSuite) TestWorkerLoop(c *C) {
	queue := make(chan message)
	testChan := make(chan bool)

	go Workers.worker(queue)

	job := &Job{
		Type: "TestWorker",
		Args: []interface{}{testChan, true},
		Queue: "default",
		ID: "123",
		Retry: false,
	}

	queue <- message{job: job}

	res := <-testChan
	c.Assert(res, Equals, true)
}
