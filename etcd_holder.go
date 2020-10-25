package petal

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"hash/fnv"
	"os"
	"time"
)

// EtcdHolder contains etcd config and operator.
type EtcdHolder struct {
	Ip           string
	Port         int
	DatacenterID uint
	WorkerID     uint
	Client       *clientv3.Client
	*EtcdOption
}

const (
	prefixEtcdPath = "/petal"
	leaseTTL       = 5
)

// Init a etcd client
func (holder *EtcdHolder) Init() error {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   holder.Endpoints,
		DialTimeout: holder.Timeout,
	})
	if err != nil {
		return err
	}
	holder.Client = client

	return nil
}

// GetWorkerID from etcd
// If the workerID is zero,then generate a unique workerID in the datacenterID range
// If the workerID is greater than 0, then register a workerID on Etcd
func (holder *EtcdHolder) GetWorkerID() (uint, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), holder.Timeout)
	defer cancel()

	if holder.WorkerID == 0 {
		err := holder.generateWorkerId(ctx)
		if err != nil {
			return 0, err
		}
	} else {
		holderBytes, err := json.Marshal(holder)
		if err != nil {
			return 0, err
		}

		key := buildEtcdKey(holder.DatacenterID, holder.WorkerID)
		value := string(holderBytes)
		err = holder.PutNx(ctx, key, value)
		if err != nil {
			return 0, err
		}
	}

	return holder.WorkerID, nil
}

// If the workerID is not specified, a workerID is generated
func (holder *EtcdHolder) generateWorkerId(ctx context.Context) error {
	count := uint32(0)

	h := fnv.New32a()
	//ip:port
	_, err := h.Write([]byte(fmt.Sprintf("%s:%d", holder.Ip, holder.Port)))
	if err != nil {
		return err
	}
	keyHash := h.Sum32()

	holderBytes, err := json.Marshal(holder)
	if err != nil {
		return err
	}
	for {
		if count > MaxWorkerID {
			return ErrOverflowWorkerID
		}
		workerID := (keyHash + count) & MaxWorkerID
		key := buildEtcdKey(holder.DatacenterID, uint(workerID))

		err := holder.PutNx(ctx, key, string(holderBytes))
		if err == ErrDuplicationWorkerID {
			count++
			continue
		}
		if err != nil {
			return err
		}

		holder.WorkerID = uint(workerID)
		break
	}

	return nil
}

// PutNx key:value to etcd if the key is not exist
func (holder *EtcdHolder) PutNx(ctx context.Context, key, value string) error {
	leaseResp, err := holder.Client.Grant(context.Background(), leaseTTL)
	if err != nil {
		return err
	}

	resp, err := holder.Client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(key), "=", 0),
	).Then(clientv3.OpPut(key, value, clientv3.WithLease(leaseResp.ID))).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrDuplicationWorkerID
	}

	holder.keepAlive(leaseResp)

	return nil
}

// Delete  deletes a key, or optionally using WithRange(end), [key, end).
func (holder *EtcdHolder) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) error {
	_, err := holder.Client.Delete(ctx, key, opts...)
	if err != nil {
		return err
	}

	return nil
}

func (holder *EtcdHolder) keepAlive(leaseResp *clientv3.LeaseGrantResponse) {
	leaseRespChan, err := holder.Client.KeepAlive(context.TODO(), leaseResp.ID)
	if err != nil {
		Log.Errorf("lease keep alive failed,err=%v\nexit server now\n", err)
		os.Exit(1)
	}

	go func() {
		for {
			select {
			case leaseKeepResp := <-leaseRespChan:
				if leaseKeepResp != nil {
					Log.Debug("lease success")
					time.Sleep(leaseTTL*time.Second - 1*time.Second)
					continue
				}
				return
			}
		}
	}()
}

// key format: prefix/datacenterID/workerID
func buildEtcdKey(datacenterId, workerId uint) string {
	return fmt.Sprintf("%s/%d/%d", prefixEtcdPath, datacenterId, workerId)
}
