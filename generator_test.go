package petal

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

var nextDatacenterID uint32
var nextWorkerID uint32

func getNextMachineID() (uint, uint) {
	atomic.AddUint32(&nextDatacenterID, 1)
	atomic.AddUint32(&nextWorkerID, 1)
	return uint(nextDatacenterID), uint(nextWorkerID)
}

func TestInvalidMachineID(t *testing.T) {
	_, err := NewGenerator(&Option{
		DatacenterID: 31,
		WorkerID:     31,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	assert.Nil(t, err, "unexpected error")

	_, err = NewGenerator(&Option{
		DatacenterID: 32,
		WorkerID:     31,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	assert.Equal(t, err, ErrInvalidDatacenterID, "invalid error for overranged datacenterID")

	_, err = NewGenerator(&Option{
		DatacenterID: 31,
		WorkerID:     32,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	assert.Equal(t, err, ErrInvalidWorkerID, "invalid error for overranged workerID")
}

func TestUniqueMachineID(t *testing.T) {
	datacenterID, workerID := getNextMachineID()

	_, err := NewGenerator(&Option{
		DatacenterID: datacenterID,
		WorkerID:     workerID,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	assert.Nil(t, err, "failed to create first generator")

	datacenterID, workerID = getNextMachineID()
	_, err = NewGenerator(&Option{
		DatacenterID: datacenterID,
		WorkerID:     workerID,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	assert.Nil(t, err, "failed to create second generator")

	// duplicate!!
	//g, _ := NewGenerator(mayBeDup)
	//assert.Nil(t, g, "machine ID must be unique")
}

func TestGenerateAnID(t *testing.T) {
	datacenterID, workerID := getNextMachineID()
	g, err := NewGenerator(&Option{
		DatacenterID: datacenterID,
		WorkerID:     workerID,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	assert.Nil(t, err, "failed to create new generator")

	var id uint64
	now := time.Now()

	t.Log("generate")
	{
		ident, err := g.NextID()
		assert.Nil(t, err, "failed to generate id")

		assert.True(t, id >= 0, "invalid id")

		id = ident
	}

	t.Logf("id = %d", id)

	t.Log("restore timestamp")
	{
		timestampSince := uint64(Epoch.UnixNano()) / uint64(time.Millisecond)
		ts := (id & 0x7FFFFFFFFFC00000 >> (MachineIDBits + SequenceBits)) + timestampSince
		nowMsec := uint64(now.UnixNano()) / uint64(time.Millisecond)

		// To avoid failure would cause by timestamp on execution
		assert.True(t, nowMsec == ts || nowMsec+1 == ts, "failed to restore timestamp")
	}

	t.Log("restore machine ID")
	{
		did := uint(id & 0x3FF000 >> DatacenterIdShift)
		wid := uint(id & 0x1F000 >> SequenceBits)
		assert.True(t, did == datacenterID, "failed to restore datacenter ID")
		assert.True(t, wid == workerID, "failed to restore worker ID")
	}
}

func TestGenerateSomeIDs(t *testing.T) {
	datacenterID, workerID := getNextMachineID()
	g, _ := NewGenerator(&Option{
		DatacenterID: datacenterID,
		WorkerID:     workerID,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	var ids []uint64

	for i := 0; i < 1000; i++ {
		id, err := g.NextID()
		assert.Nil(t, err, "failed to generate id")

		for _, otherID := range ids {
			assert.True(t, otherID != id, "id duplicated!!")
		}
		l := len(ids)
		assert.True(t, l >= 0 && (ids == nil || id > ids[l-1]), "generated smaller id!!")

		ids = append(ids, id)
	}

	t.Logf("%d ids are tested", len(ids))
}

func TestClockRollback(t *testing.T) {
	datacenterID, workerID := getNextMachineID()
	g, _ := NewGenerator(&Option{
		DatacenterID: datacenterID,
		WorkerID:     workerID,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	_, err := g.NextID()
	if err != nil {
		t.Fatalf("failed to generate id: %s", err)
	}

	setNowFunc(func() time.Time {
		return time.Now().Add(-10 * time.Minute)
	})

	_, err = g.NextID()
	assert.NotNil(t, err, "when server clock rollback, generater must return error")
}

func BenchmarkGenerateID(b *testing.B) {
	datacenterID, workerID := getNextMachineID()
	g, _ := NewGenerator(&Option{
		DatacenterID: datacenterID,
		WorkerID:     workerID,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = g.NextID()
	}
}
