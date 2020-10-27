package petal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	etcdnaming "go.etcd.io/etcd/clientv3/naming"
	"go.etcd.io/etcd/proxy/grpcproxy"
	"google.golang.org/grpc"
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
	etcdWorkerIdPrefix = "/petal/worker"
	etcdServicePrefix  = "/petal/service"
	leaseTTL           = 5
	leaseRetryTimes    = 5
)

// Init a etcd client
func (holder *EtcdHolder) Init() error {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   holder.Endpoints,
		DialTimeout: holder.Timeout,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	})
	if err != nil {
		return fmt.Errorf("init etcd clientv3 failed[EtcdOption: %+v],err=%s", *holder.EtcdOption, err.Error())
	}
	holder.Client = client

	return nil
}

// GetWorkerID from etcd
// If the workerID is zero,then generate a unique workerID in the datacenterID range
// If the workerID is greater than 0, then register a workerID on Etcd
func (holder *EtcdHolder) GetWorkerID() (uint, error) {
	ctx, cancel := context.WithCancel(context.TODO())
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

		key := buildEtcdWorkerKey(holder.DatacenterID, holder.WorkerID)
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
		key := buildEtcdWorkerKey(holder.DatacenterID, uint(workerID))

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

	holder.keepAlive(key, leaseResp)

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

func (holder *EtcdHolder) GetGrpcConnection() (*grpc.ClientConn, error) {
	resolver := &etcdnaming.GRPCResolver{Client: holder.Client}
	roundRobin := grpc.RoundRobin(resolver)
	return grpc.Dial(etcdServicePrefix, grpc.WithBalancer(roundRobin), grpc.WithBlock(), grpc.WithInsecure())
}

func (holder *EtcdHolder) RegisterServer() {
	addr := fmt.Sprintf("%s:%d", holder.Ip, holder.Port)
	grpcproxy.Register(holder.Client, etcdServicePrefix, addr, leaseTTL)
}

//keepAlive keeps the given lease alive forever.
func (holder *EtcdHolder) keepAlive(key string, leaseResp *clientv3.LeaseGrantResponse) {
	leaseRespChan, err := holder.Client.KeepAlive(context.TODO(), leaseResp.ID)
	if err != nil {
		Log.Errorf("lease keep alive failed,err=%v\nexit server now\n", err)
		os.Exit(1)
	}

	go func() {
		failedCount := 0
		sleepTime := (leaseTTL - 1) * time.Second
		for {
			select {
			case leaseKeepResp := <-leaseRespChan:
				if leaseKeepResp != nil {
					if failedCount != 0 {
						failedCount = 0
					}
					Log.Debugf("lease success[key=%s]", key)
					time.Sleep(sleepTime)
					continue
				}
				if failedCount < leaseRetryTimes {
					failedCount++
					Log.Warnf("lease failed,failedCount=%d", failedCount)
					if failedCount < leaseRetryTimes {
						time.Sleep(sleepTime)
						continue
					}
				}
				Log.Errorf("All attempts to renew the lease have failed[retryTimes=%d],exit now", leaseRetryTimes)
				os.Exit(1)
			}
		}
	}()
}

// key format: prefix/datacenterID/workerID
func buildEtcdWorkerKey(datacenterId, workerId uint) string {
	return fmt.Sprintf("%s/%d/%d", etcdWorkerIdPrefix, datacenterId, workerId)
}
