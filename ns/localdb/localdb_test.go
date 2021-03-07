package localdb

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hectodns/hectodns/ns"
)

func newStorage(t *testing.T) (s *Storage, path string) {
	dbfile, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	require.NoError(t, dbfile.Close())

	s, err = NewStorage(dbfile.Name())
	require.NoError(t, err)
	require.NotNil(t, s)
	return s, dbfile.Name()
}

func TestNewStorage_ok(t *testing.T) {
	s, path := newStorage(t)
	defer os.Remove(path)

	require.NoError(t, s.Close(context.TODO()))
}

func TestStorage_CreateZone_ok(t *testing.T) {
	s, path := newStorage(t)
	defer os.Remove(path)
	defer s.Close(context.TODO())

	f := fuzz.New().NilChance(0)
	var zoneName string
	f.Fuzz(&zoneName)

	zoneInput := ns.ZoneInput{Name: zoneName}
	zone0, err := s.CreateZone(context.TODO(), zoneInput)

	require.NoError(t, err)
	require.NotNil(t, zone0)

	assert.Equal(t, zoneName, zone0.Name)
	assert.True(t, zone0.CreatedAt > 0)
	assert.True(t, zone0.UpdatedAt > 0)

	zone1, err := s.QueryZone(context.TODO(), struct{ Name string }{zoneName})
	require.NoError(t, err)
	require.NotNil(t, zone1)

	assert.Equal(t, zone0, zone1)
}

func TestStorage_CreateRecord_ok(t *testing.T) {
	s, path := newStorage(t)
	defer os.Remove(path)
	defer s.Close(context.TODO())

	f := fuzz.New().NilChance(0)
	var (
		zoneName   string
		recordName string
	)

	f.Fuzz(&zoneName)
	f.Fuzz(&recordName)

	zoneInput := ns.ZoneInput{Name: zoneName}
	recordInput := ns.RecordInput{ZoneName: zoneName, Name: recordName}

	_, err := s.CreateZone(context.TODO(), zoneInput)
	require.NoError(t, err)

	record0, err := s.CreateRecord(context.TODO(), recordInput)
	require.NoError(t, err)
	require.NotNil(t, record0)

	assert.Equal(t, recordName, record0.Name)
	assert.Equal(t, zoneName, record0.ZoneName)
	assert.True(t, record0.ID != 0, "id must be set")

	record1, err := s.QueryRecord(context.TODO(), struct {
		ZoneName string
		ID       uint64
	}{zoneName, record0.ID})

	require.NoError(t, err)
	assert.Equal(t, record0, record1)
}
