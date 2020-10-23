package petal

import (
	"time"
)

// ToTime returns the time when id was generated.
func ToTime(id uint64) time.Time {
	ts := id >> (MachineIDBits + SequenceBits)
	d := time.Duration(int64(ts) * int64(time.Millisecond))
	return Epoch.Add(d)
}

// ToID returns the minimum id which will be generated at time t.
func ToID(t time.Time) uint64 {
	d := t.Sub(Epoch)
	ts := d.Nanoseconds() / TimeUnit
	return uint64(ts) << (MachineIDBits + SequenceBits)
}

// Dump returns the structure of id.
func Dump(id uint64) (t time.Time, datacenterID, workerID uint64, sequence uint64) {
	datacenterID = (id & (MaxDatacenterID << DatacenterIdShift)) >> DatacenterIdShift
	workerID = (id & (MaxWorkerID << SequenceBits)) >> SequenceBits
	sequence = id & SequenceMask
	return ToTime(id), datacenterID, workerID, sequence
}
