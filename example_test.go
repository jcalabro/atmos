package atmos_test

import (
	"fmt"

	"github.com/jcalabro/atmos"
)

func ExampleParseDID() {
	did, err := atmos.ParseDID("did:plc:ewvi7nxzy7mbhbztlou63zpc")
	if err != nil {
		panic(err)
	}
	fmt.Println(did.Method())
	fmt.Println(did.Identifier())
	// Output:
	// plc
	// ewvi7nxzy7mbhbztlou63zpc
}

func ExampleParseHandle() {
	h, err := atmos.ParseHandle("alice.bsky.social")
	if err != nil {
		panic(err)
	}
	fmt.Println(h.Normalize())
	fmt.Println(h.TLD())
	// Output:
	// alice.bsky.social
	// social
}

func ExampleParseTID() {
	tid, err := atmos.ParseTID("3jqfcqzm3fp2j")
	if err != nil {
		panic(err)
	}
	fmt.Println(tid.Time().UTC().Format("2006-01-02"))
	// Output:
	// 2023-03-08
}

func ExampleParseNSID() {
	nsid, err := atmos.ParseNSID("app.bsky.feed.post")
	if err != nil {
		panic(err)
	}
	fmt.Println(nsid.Authority())
	fmt.Println(nsid.Name())
	// Output:
	// feed.bsky.app
	// post
}

func ExampleParseATURI() {
	uri, err := atmos.ParseATURI("at://did:plc:ewvi7nxzy7mbhbztlou63zpc/app.bsky.feed.post/3jqfcqzm3fp2j")
	if err != nil {
		panic(err)
	}
	fmt.Println(uri.Authority())
	fmt.Println(uri.Collection())
	fmt.Println(uri.RecordKey())
	// Output:
	// did:plc:ewvi7nxzy7mbhbztlou63zpc
	// app.bsky.feed.post
	// 3jqfcqzm3fp2j
}

func ExampleTIDClock() {
	clock := atmos.NewTIDClock(0)
	tid1 := clock.Next()
	tid2 := clock.Next()
	fmt.Println(tid1.Integer() < tid2.Integer())
	// Output:
	// true
}
