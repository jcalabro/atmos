package space_test

import (
	"fmt"

	"github.com/jcalabro/atmos/space"
)

func ExampleAlphaCARLimits() {
	limits := space.AlphaCARLimits()
	fmt.Println(limits.MaxRecords, limits.MaxTotalSize>>20)
	// Output: 100000 256
}
