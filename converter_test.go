package petal_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/uemuramikio/petal"
	"testing"
	"time"
)

func TestConvertFixed(t *testing.T) {
	t1 := time.Unix(1596240000, 0)
	id := petal.ToID(t1)
	assert.Equal(t, uint64(77188615372800000), id, "unexpected id")

	t2 := petal.ToTime(id)
	assert.Equal(t, petal.ToPetalTime(t1), petal.ToPetalTime(t2), "roundtrip failed")
}

func TestConvertFixedSub(t *testing.T) {
	t1 := time.Unix(1596240000, 777000000)
	id := petal.ToID(t1)
	assert.Equal(t, uint64(77188618631774208), id, "unexpected id")

	t2 := petal.ToTime(id)
	assert.Equal(t, petal.ToPetalTime(t1), petal.ToPetalTime(t2), "roundtrip failed")
}

func TestConvertNow(t *testing.T) {
	t1 := time.Now().In(time.UTC)
	id := petal.ToID(t1)
	t2 := petal.ToTime(id)
	f := "2006-01-02T15:04:05.000"
	assert.Equal(t, t1.Format(f), t2.Format(f), "roundtrip failed")
}

func TestDump(t *testing.T) {
	testCases := []struct {
		id  uint64
		ts  time.Time
		did uint64
		wid uint64
		seq uint64
	}{
		{
			77410210506764288,
			time.Date(2020, 8, 1, 14, 40, 32, 396000000, time.UTC),
			31,
			7,
			0,
		},
		{
			77410210506764289,
			time.Date(2020, 8, 1, 14, 40, 32, 396000000, time.UTC),
			31,
			7,
			1,
		},
	}
	for _, tc := range testCases {
		ts, did, wid, seq := petal.Dump(tc.id)
		assert.Equal(t, ts, tc.ts, fmt.Sprintf("%d timestamp is not expected. got %s expected %s", tc.id, ts, tc.ts))
		assert.Equal(t, did, tc.did, fmt.Sprintf("%d datacenterID is not expected. got %d expected %d", tc.id, did, tc.did))
		assert.Equal(t, wid, tc.wid, fmt.Sprintf("%d workerID is not expected. got %d expected %d", tc.id, wid, tc.wid))
		assert.Equal(t, seq, tc.seq, fmt.Sprintf("%d sequence is not expected. got %d expected %d", tc.id, seq, tc.seq))
	}
}
