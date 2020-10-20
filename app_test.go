package petal

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/uemuramikio/petal/pb"
	"testing"
)

func TestGenerate(t *testing.T) {
	app, err := NewApp(Option{DatacenterID: 0, WorkerID: 0})
	assert.Nil(t, err)
	s := server{gen: app.gen}
	generator, err := s.Generate(context.Background(), new(pb.Request))
	assert.Nil(t, err)
	assert.NotNil(t, generator.Id)
}

func TestNextID(t *testing.T) {
	app, err := NewApp(Option{DatacenterID: 0, WorkerID: 1})
	assert.Nil(t, err)
	s := server{gen: app.gen}
	generator, err := s.Generate(context.Background(), new(pb.Request))
	assert.Nil(t, err)
	nextID, err := s.NextID()
	assert.Nil(t, err)
	Log.Debugf("Next Generated ID: %d", nextID)
	assert.True(t, nextID > generator.Id)
}
