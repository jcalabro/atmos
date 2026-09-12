package host_test

import (
	"fmt"

	spacehost "github.com/jcalabro/atmos/space/host"
)

func ExampleAlphaLimits() {
	limits := spacehost.AlphaLimits()
	fmt.Println(limits.DeliveryWorkers, limits.Registration.PerSpace)
	// Output: 32 1000
}
