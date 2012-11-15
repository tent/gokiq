package main

import (
	"fmt"

	"github.com/tent/synergizer/gokiq"
)

type ExampleWorker struct{}

func (w *ExampleWorker) Perform(args []interface{}) error {
	return nil
}

const JobCount = 50

func main() {
	gokiq.Client.Register("ExampleWorker", &ExampleWorker{}, "default", 5)
	gokiq.Client.Connect()

	fmt.Println("Queuing a broken job...")
	gokiq.Client.QueueJob(&ExampleWorker{}) // has no arguments, worker will panic due to out of bounds slice access

	fmt.Printf("Queuing %d jobs...", JobCount)
	for i := 0; i < JobCount; i++ {
		gokiq.Client.QueueJob(&ExampleWorker{}, 1)
	}
}
