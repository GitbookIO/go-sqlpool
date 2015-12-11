package counter

import (
	"testing"
)

func TestCounter(t *testing.T) {
	var c Counter

	if c.Value() != 0 {
		t.Errorf("An empty counter should be at zero")
	}

	c.Increment()
	c.Increment()

	if c.Value() != 2 {
		t.Errorf("Incrementing should be obvious")
	}

	c.Decrement()
	c.Increment()

	if c.Value() != 2 {
		t.Errorf("Increments and Decrements should cancel out")
	}

	c.Decrement()
	c.Decrement()

	if c.Value() != 0 {
		t.Errorf("We should be back to zero :)")
	}
}
