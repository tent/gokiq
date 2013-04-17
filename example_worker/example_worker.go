package main

import (
	"time"

	"github.com/cupcake/gokiq"
)

type ExampleWorker struct {
	Data []int `json:"args"`
}

func (w *ExampleWorker) Args() interface{} { return w.Data }

func (w *ExampleWorker) Perform() error {
	doSomething(w.Data[0])
	time.Sleep(100 * time.Millisecond)
	return nil
}

func doSomething(i int) {
}

func main() {
	gokiq.Workers.Register("ExampleWorker", &ExampleWorker{})
	gokiq.Workers.WorkerCount = 200
	gokiq.Workers.Run()
}
