package main

import (
	"fmt"
	"time"

	"github.com/tent/synergizer/gokiq"
)

type ExampleWorker struct{}

func (w *ExampleWorker) Perform(args []interface{}) error {
	return nil
}

func main() {
	gokiq.Client.Register("ExampleWorker", &ExampleWorker{}, "default", 5)
	gokiq.Client.Connect()

	fmt.Println("Queuing a broken job...")
	gokiq.Client.QueueJob(&ExampleWorker{}) // has no arguments, worker will panic due to out of bounds slice access

	for _ = range time.Tick(5 * time.Millisecond) {
		gokiq.Client.QueueJob(&ExampleWorker{}, 1)
	}
}
