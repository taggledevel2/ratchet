package data_test

import (
	"fmt"

	"github.com/DailyBurn/ratchet/data"
)

type testStruct struct {
	A int
	B int
}

func ExampleNewJSON() {
	t := testStruct{A: 1, B: 2}

	d, _ := data.NewJSON(t)

	fmt.Println(string(d))
	// Output: {"A":1,"B":2}
}

func ExampleParseJSON() {
	d := []byte(`{"A":1,"B":2}`)
	t := testStruct{}
	data.ParseJSON(d, &t)

	fmt.Println(fmt.Sprintf("%+v", t))
	// Output: {A:1 B:2}
}

func ExampleObjectsFromJSON() {
	d := []byte(`[{"One":1, "Two":2},
		      {"Three":3, "Four":4}]`)

	objects, _ := data.ObjectsFromJSON(d)

	fmt.Println(fmt.Sprintf("%+v", objects))
	// Output: [map[One:1 Two:2] map[Three:3 Four:4]]
}
