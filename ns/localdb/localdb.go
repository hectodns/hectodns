package localdb

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"

	"github.com/hectodns/hectodns/ns"
)

const (
	origin = "$ORIGIN"
	sep    = "."
)

// Storage is an implementation of a nameql.Backend interface that performs management
// of zones and resource records. The Storage utilize file-based storage to keep the
// data persisted on disk.
type Storage struct {
	conn *bbolt.DB
}

// NewStorage creates a new instance of storage for the given path.
//
// Method returns an error when file system is unavailable for more than 1 second.
func NewStorage(path string) (*Storage, error) {
	// Prevent hangs on file opening and wait only for a second.
	conn, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	log.Info().Msgf("opened the storage %q", path)
	return &Storage{conn: conn}, nil
}

// Close closes the storage.
func (s *Storage) Close(ctx context.Context) error {
	return s.conn.Close()
}

// CreateZone creates a new bucket with the specified zone name and puts zone
// information into the $ORIGIN key.
func (s *Storage) CreateZone(ctx context.Context, input ns.ZoneInput) (
	zone *ns.Zone, err error,
) {
	zone = ns.NewZone(input)
	if err = s.conn.Update(func(tx *bbolt.Tx) error {
		zoneBucket, err := tx.CreateBucketIfNotExists([]byte(input.Name))
		if err != nil {
			return errors.WithMessagef(err, "zone %q was not created", input.Name)
		}

		zoneBytes, err := json.Marshal(zone)
		if err != nil {
			return err
		}

		return zoneBucket.Put([]byte(origin), zoneBytes)
	}); err != nil {
		return nil, err
	}
	return zone, nil
}

// selectZone returns a zone bucket with the specified name.
//
// Returns ns.ErrZoneNotFound when the database is missing zone with given name.
func (s *Storage) selectZone(tx *bbolt.Tx, zoneName string) (*bbolt.Bucket, error) {
	zone := tx.Bucket([]byte(zoneName))
	if zone == nil {
		return nil, errors.WithMessagef(ns.ErrZoneNotFound, zoneName)
	}
	return zone, nil
}

// QueryZone attempts to find a zone by it's name.
func (s *Storage) QueryZone(ctx context.Context, input struct{ Name string }) (*ns.Zone, error) {
	var zone ns.Zone

	query := func(tx *bbolt.Tx) error {
		zoneBucket, err := s.selectZone(tx, input.Name)
		if err != nil {
			return err
		}

		zoneBytes := zoneBucket.Get([]byte(origin))
		if zoneBytes == nil {
			return errors.Errorf("zone %q corrupted, no origin", input.Name)
		}

		err = json.Unmarshal(zoneBytes, &zone)
		return errors.WithMessagef(err, "zone %q corrupted, invalid data", input.Name)
	}

	if err := s.conn.View(query); err != nil {
		return nil, err
	}
	return &zone, nil
}

func (s *Storage) QueryZones(ctx context.Context) (zones []ns.Zone, err error) {
	if err = s.conn.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			zoneBytes := b.Get([]byte(origin))
			if zoneBytes == nil {
				return errors.Errorf("zone %q corrupted, no origin", name)
			}

			var zone ns.Zone
			err := json.Unmarshal(zoneBytes, &zone)
			if err != nil {
				return errors.WithMessagef(err, "zone %q corrupted, invalid data", name)
			}

			zones = append(zones, zone)
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return zones, nil
}

// CreateRecord creates a new record within a specified zone, puts resource record
// information into the separate bucket.
//
// The method works in append-only format, does not support replacement operation.
func (s *Storage) CreateRecord(ctx context.Context, input ns.RecordInput) (
	record *ns.Record, err error,
) {
	if err = s.conn.Update(func(tx *bbolt.Tx) error {
		zoneBucket, err := s.selectZone(tx, input.ZoneName)
		if err != nil {
			return err
		}

		// Assign a new sequence number in order to differentiate the records by
		// their keys. The transaction is writable, therefore no need to validate
		// the returned error.
		id, _ := zoneBucket.NextSequence()
		key := strings.Join([]string{input.Name, strconv.FormatUint(id, 10)}, sep)

		record = ns.NewRecord(id, input)
		recordBytes, err := json.Marshal(record)
		if err != nil {
			return err
		}

		return zoneBucket.Put([]byte(key), recordBytes)
	}); err != nil {
		return nil, err
	}
	return record, nil
}

// QueryRecord attempts to find a record in a specified Zone with the ID.
func (s *Storage) QueryRecord(ctx context.Context, input struct {
	ZoneName string
	ID       uint64
}) (record *ns.Record, err error) {
	query := func(tx *bbolt.Tx) error {
		zoneBucket, err := s.selectZone(tx, input.ZoneName)
		if err != nil {
			return err
		}

		cur := zoneBucket.Cursor()
		keySuffix := sep + strconv.FormatUint(input.ID, 10)
		for key, value := cur.First(); key != nil; key, value = cur.Next() {
			if strings.HasSuffix(string(key), keySuffix) {
				err = json.Unmarshal(value, &record)
				if err != nil {
					return err
				}
				break
			}
		}
		return nil
	}

	if err = s.conn.View(query); err != nil {
		return nil, err
	}
	if record == nil {
		return nil, errors.Errorf("record id=%d in %q not found", input.ID, input.ZoneName)
	}
	return record, nil
}

func (s *Storage) QueryRecords(ctx context.Context) (records []ns.Record, err error) {
	return nil, nil
}
