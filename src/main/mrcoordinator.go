package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"mit_ds_2021/mr"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		go m.CreateMapTask(m.InputFile)
		m.Group.Add(1)
		m.Group.Wait()
		go m.CreateReduceTask(m.IntermediateFile)
		m.Group.Add(1)
		m.Group.Wait()
		//if m.MapSignal {
		//	m.MapSignal = false
		//
		//} else if m.ReduceSignal {
		//	m.ReduceSignal = false
		//
		//} else {
		//	fmt.Printf("master no task...\n")
		//}
	}

	time.Sleep(time.Second)
}
