package main

import (
	"github.com/tent/synergizer/gokiq"
)

type ExampleWorker struct{}

func (w *ExampleWorker) Perform(args []interface{}) error {
	doSomething(args[0].(float64))
	return nil
}

func doSomething(i float64) {
}

func main() {
	gokiq.Workers.Register("ExampleWorker", &ExampleWorker{})
	gokiq.Workers.Run()
}
