// +----------------+---------------------+-----------------+-----------------+
// | timestamp (ms) | machine id (10bit)                    | sequence number |
// +                +---------------------+-----------------+                 +
// |         (41bit)| datacenter id (5bit)| worker id (5bit)|          (12bit)|
// +----------------+---------------------+-----------------+-----------------+

package petal

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var nowFunc = time.Now
var nowMutex sync.RWMutex

func setNowFunc(f func() time.Time) {
	nowMutex.Lock()
	defer nowMutex.Unlock()
	nowFunc = f
}

func now() time.Time {
	nowMutex.RLock()
	defer nowMutex.RUnlock()
	return nowFunc()
}

// Epoch is petal epoch time (2020-01-01 00:00:00 UTC)
// Generated ID includes elapsed time form Epoch.
var Epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	WorkerIDBits      = 5
	DatacenterIDBits  = 5
	MachineIDBits     = DatacenterIDBits + WorkerIDBits
	SequenceBits      = 12
	TimestampShift    = SequenceBits + WorkerIDBits + DatacenterIDBits
	DatacenterIdShift = SequenceBits + WorkerIDBits
	MaxWorkerID       = 1<<WorkerIDBits - 1
	MaxDatacenterID   = 1<<DatacenterIDBits - 1
	SequenceMask      = 1<<SequenceBits - 1
)

var newGeneratorLock sync.Mutex

const TimeUnit = int64(time.Millisecond)

// errors
var (
	ErrInvalidDatacenterID = errors.New("invalid datacenter id")
	ErrInvalidWorkerID     = errors.New("invalid worker id")
	ErrDuplicationWorkerID = errors.New("duplication worker id")
	ErrOverflowWorkerID    = errors.New("no more workerId available in the datacenter")
)

func checkMachineID(datacenterID, workerID uint) error {
	if datacenterID > MaxDatacenterID {
		return ErrInvalidDatacenterID
	}

	if workerID > MaxWorkerID {
		return ErrInvalidWorkerID
	}

	return nil
}

// Generator is an interface to generate unique ID.
type Generator interface {
	NextID() (uint64, error)
	DatacenterID() uint
	WorkerID() uint
}

type generator struct {
	datacenterID  uint
	workerID      uint
	lastTimestamp int64
	sequence      uint
	lock          *sync.Mutex
	startedAt     int64
}

// NewGenerator returns new generator.
func NewGenerator(option *Option) (Generator, error) {
	// To keep machine ID be unique.
	newGeneratorLock.Lock()
	defer newGeneratorLock.Unlock()

	if err := checkMachineID(option.DatacenterID, option.WorkerID); err != nil {
		return nil, err
	}
	ip, err := getIp()
	if err != nil {
		return nil, err
	}
	holder := EtcdHolder{
		Ip:           ip,
		Port:         option.ServerPort,
		DatacenterID: option.DatacenterID,
		WorkerID:     option.WorkerID,
		EtcdOption:   option.EtcdOption,
	}
	err = holder.Init()
	if err != nil {
		return nil, err
	}
	holder.RegisterServer()
	workerId, err := holder.GetWorkerID()
	if err != nil {
		return nil, err
	}

	return &generator{
		datacenterID: option.DatacenterID,
		workerID:     workerId,
		startedAt:    ToPetalTime(Epoch),
		lock:         new(sync.Mutex),
	}, nil
}

func (g *generator) DatacenterID() uint {
	return g.datacenterID
}

func (g *generator) WorkerID() uint {
	return g.workerID
}

// Return a freshly generated Petal ID
func (g *generator) NextID() (uint64, error) {
	g.lock.Lock()
	defer g.lock.Unlock()

	current := g.currentElapsedTime()

	// for rewind of server clock
	if current < g.lastTimestamp {
		return 0, fmt.Errorf("invalid system time")
	}

	if current == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & SequenceMask
		if g.sequence == 0 {
			// overflow
			current = g.waitUntilNextTick(current)
		}
	} else {
		//current > g.lastTimestamp
		g.sequence = 0
	}
	g.lastTimestamp = current

	return uint64(g.lastTimestamp)<<TimestampShift |
		uint64(g.datacenterID)<<DatacenterIdShift |
		uint64(g.workerID)<<SequenceBits |
		uint64(g.sequence), nil
}

// currentElapsedTime returns the time elapsed from Epoch to now().
func (g *generator) currentElapsedTime() int64 {
	return ToPetalTime(now()) - g.startedAt
}

// waitUntilNextTick wait next tick
func (g *generator) waitUntilNextTick(ts int64) int64 {
	next := g.currentElapsedTime()

	for next <= ts {
		next = g.currentElapsedTime()
		time.Sleep(50 * time.Nanosecond)
	}

	return next
}

// ToPetalTime convert time.Time to PetalTime(time.Millisecond)
func ToPetalTime(t time.Time) int64 {
	return t.UnixNano() / TimeUnit
}

// getIp return the first interface address
func getIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				return ip.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("can not find the client ip address")
}
