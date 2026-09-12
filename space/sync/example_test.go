package sync_test

import (
	"fmt"

	spacesync "github.com/jcalabro/atmos/space/sync"
)

func ExampleAlphaLimits() {
	limits := spacesync.AlphaLimits()
	fmt.Println(limits.MaxAuthors, limits.PageSize)
	// Output: 10000 1000
}

func ExampleAlphaSchedulerOptions() {
	options := spacesync.AlphaSchedulerOptions(func(result spacesync.JobResult) {
		if result.Err != nil {
			fmt.Println(result.Err)
		}
	})
	fmt.Println(options.Workers, options.QueueCapacity)
	// Output: 32 10000
}
