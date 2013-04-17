package main

import (
	"fmt"
	"time"

	"github.com/cupcake/gokiq"
)

type ExampleWorker struct {
	Data []int `json:"args"`
}

func (w *ExampleWorker) Perform() error { return nil }

func (w *ExampleWorker) Args() interface{} { return w.Data }

func main() {
	gokiq.Client.Register("ExampleWorker", &ExampleWorker{}, "default", 5)
	gokiq.Client.Connect()

	fmt.Println("Queuing a broken job...")
	gokiq.Client.QueueJob(&ExampleWorker{}) // has no arguments, worker will panic due to out of bounds slice access

	fmt.Println("Queuing a job every 5ms...")
	for _ = range time.Tick(5 * time.Millisecond) {
		gokiq.Client.QueueJob(&ExampleWorker{[]int{1}})
	}
}
