//Tests files MUST end in _test.go, otherwise the go test command will not recognize them as test files.
// these files are run using go test command rather than go run.

// When we run test files, go hides the output unless we do go run -v ./..., the -v shows the output.

package tests

import (
	"testing"

	"github.com/SK3720/Distributed-KV-Store/internal/kv"
)

// testing.T is a struct that is passed to each test function. It contains methods for reporting test failures and logging additional information. The testing framework automatically creates an instance of testing.T for each test function and passes it as an argument.
// it is a part of the tests package
func TestStore(t *testing.T) {

	store := kv.NewStore()

	// Test Put + Get
	store.Put("name", "Shiv")

	value, ok := store.Get("name")
	println(value, ok) // prints the value string and the ok boolean
	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "Shiv" {
		t.Fatalf("expected Shiv, got %s", value)
	}

	// Test overwrite
	store.Put("name", "Bob")

	//you MUST handle both values returned by the Get function, even if you don't use the second value. Otherwise, you'll get a compile error.
	value, _ = store.Get("name")

	if value != "Bob" {
		t.Fatalf("expected Bob, got %s", value)
	}

	// Test Delete
	store.Delete("name")

	_, ok = store.Get("name")

	if ok {
		t.Fatal("expected key to be deleted")
	}

	// Test missing key
	_, ok = store.Get("does-not-exist")

	if ok {
		t.Fatal("expected missing key")
	}
}
