package petal

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
	"testing"
	"time"
)

var DefaultEndpoints = []string{"localhost:2379"}
var DefaultPort = 50051
var DefaultTimeout = 5 * time.Hour

func TestInit(t *testing.T) {
	holder := &EtcdHolder{
		Ip:           "192.168.0.1",
		Port:         DefaultPort,
		DatacenterID: 0,
		WorkerID:     1,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	}
	err := holder.Init()
	assert.Nil(t, err)
	workerID, err := holder.GetWorkerID()
	assert.Nil(t, err)
	assert.Equal(t, uint(1), workerID)
	Log.Infof("workerID is : %d", workerID)
	err = holder.Delete(context.TODO(), buildEtcdKey(holder.DatacenterID, workerID))
	assert.Nil(t, err)
}

func TestDuplicatedInit(t *testing.T) {
	holder := &EtcdHolder{
		Ip:           "192.168.1.1",
		Port:         DefaultPort,
		DatacenterID: 1,
		WorkerID:     1,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	}
	err := holder.Init()
	assert.Nil(t, err)
	_, err = holder.GetWorkerID()
	assert.Nil(t, err)
	// Repeated calls to the init method
	_, err = holder.GetWorkerID()
	assert.Equal(t, err, ErrDuplicationWorkerID)
	err = holder.Delete(context.TODO(), buildEtcdKey(holder.DatacenterID, holder.WorkerID))
	assert.Nil(t, err)
}

func TestGenerateWorkerID(t *testing.T) {
	holder := &EtcdHolder{
		Ip:           "192.168.1.1",
		Port:         DefaultPort,
		DatacenterID: 2,
		WorkerID:     0,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	}
	err := holder.Init()
	assert.Nil(t, err)
	workerID, err := holder.GetWorkerID()
	Log.Infof("workerID is : %d", workerID)
	err = holder.Delete(context.TODO(), buildEtcdKey(holder.DatacenterID, workerID))
	assert.Nil(t, err)
}

func TestGenerateDuplicatedWorkerID(t *testing.T) {
	holder := &EtcdHolder{
		Ip:           "192.168.1.1",
		Port:         DefaultPort,
		DatacenterID: 2,
		WorkerID:     0,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	}
	err := holder.Init()
	assert.Nil(t, err)
	workerID, err := holder.GetWorkerID()
	Log.Infof("workerID is : %d", workerID)
	// second time to generate workerID
	holder.WorkerID = 0
	workerID1, err := holder.GetWorkerID()
	assert.Nil(t, err)
	assert.True(t, workerID != workerID1)
	Log.Infof("the second workerID is : %d", workerID1)
	err = holder.Delete(context.TODO(), buildEtcdKey(holder.DatacenterID, workerID))
	assert.Nil(t, err)
	err = holder.Delete(context.TODO(), buildEtcdKey(holder.DatacenterID, workerID1))
	assert.Nil(t, err)
}

func TestWorkerIDOverflow(t *testing.T) {
	datacenterID := uint(3)
	holder := &EtcdHolder{
		Ip:           "10.23.156.1",
		Port:         DefaultPort,
		DatacenterID: datacenterID,
		WorkerID:     0,
		EtcdOption: &EtcdOption{
			Endpoints: DefaultEndpoints,
			Timeout:   DefaultTimeout,
		},
	}
	err := holder.Init()
	assert.Nil(t, err)
	for i := 0; i <= MaxWorkerID; i++ {
		workerID, err := holder.GetWorkerID()
		assert.Nil(t, err)
		if err != nil {
			break
		}
		Log.Infof("current workerID is: %d", workerID)
		holder.WorkerID = 0
	}
	_, err = holder.GetWorkerID()
	assert.Equal(t, err, ErrOverflowWorkerID)

	withRange := clientv3.WithRange(fmt.Sprintf("%s/%d/%s", prefixEtcdPath, datacenterID, "\\0"))
	err = holder.Delete(context.TODO(), buildEtcdKey(datacenterID, 0), withRange)
	assert.Nil(t, err)
}
