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
	zone, err := s.CreateZone(context.TODO(), zoneInput)

	require.NoError(t, err)
	require.NotNil(t, zone)

	assert.Equal(t, zoneName, zone.Name)
	assert.True(t, zone.CreatedAt > 0)
	assert.True(t, zone.UpdatedAt > 0)

	// TODO: query database to ensure zone is created.
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

	record, err := s.CreateRecord(context.TODO(), recordInput)
	require.NoError(t, err)
	require.NotNil(t, record)

	assert.Equal(t, recordName, record.Name)
	assert.Equal(t, zoneName, record.ZoneName)
	assert.True(t, record.ID != 0, "id must be set")

	// TODO: query database to ensure zone is created.
}
