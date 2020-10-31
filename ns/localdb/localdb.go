package localdb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hectodns/hectodns/ns"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"go.etcd.io/bbolt"
)

const (
	zonesBucketName = "zones"
)

// Local storage keeps records altogether with zone information within
// a single key. This structure represents joined zone information.
type Row struct {
	// Seq is updated each time, when the row is replaced, the new
	// sequence number if used to set a unique identifier for records.
	Seq int64

	ns.Zone
	Records []ns.Record `json:"records"`
}

func unmarshalRow(b []byte) (*Row, error) {
	if b == nil {
		return nil, nil
	}

	// Allocate the new instance of a row, and unmarshal the JSON.
	var row Row
	err := json.Unmarshal(b, &row)

	if err != nil {
		return nil, err
	}
	return &row, nil
}

type Storage struct {
	conn *bbolt.DB
}

func NewStorage(path string) (*Storage, error) {
	// Prevent hangs on file opening and wait only for a second.
	conn, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	log.Info().Msgf("opened the storage %q", path)
	storage := Storage{conn: conn}

	if err = storage.init(); err != nil {
		defer storage.Close(context.TODO())
		return nil, err
	}

	return &storage, nil
}

func (s *Storage) Close(ctx context.Context) error {
	return s.conn.Close()
}

func (s *Storage) init() error {
	return s.conn.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(zonesBucketName))
		return err
	})
}

func (s *Storage) getZones(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	bucket := tx.Bucket([]byte(zonesBucketName))
	if bucket == nil {
		return nil, errors.Errorf("corrupted storage, %q does not exist", zonesBucketName)
	}
	return bucket, nil
}

func (s *Storage) replace(name string, fn func(oldrow *Row) (Row, error)) (
	newrow Row, err error,
) {
	if err = s.conn.Update(func(tx *bbolt.Tx) error {
		zones, err := s.getZones(tx)
		if err != nil {
			return err
		}

		seq, err := zones.NextSequence()
		if err != nil {
			return err
		}

		row, err := unmarshalRow(zones.Get([]byte(name)))
		if err != nil {
			return err
		} else {
			row.Seq = int64(seq)
		}

		newrow, err = fn(row)

		if err != nil {
			return err
		}

		b, err := json.Marshal(newrow)
		if err != nil {
			return err
		}
		return zones.Put([]byte(name), b)
	}); err != nil {
		return Row{}, err
	}

	return newrow, err
}

func (s *Storage) CreateZone(ctx context.Context, input ns.ZoneInput) (
	zone *ns.Zone, err error,
) {
	var (
		row Row
	)

	if row, err = s.replace(input.Name, func(_ *Row) (Row, error) {
		return Row{Zone: ns.NewZone(input)}, nil
	}); err != nil {
		return nil, err
	}

	return &row.Zone, nil
}

func (s *Storage) CreateRecord(ctx context.Context, input ns.RecordInput) (
	r *ns.Record, err error,
) {
	var (
		idx int
		row Row
	)

	// On record creation replace a whole zone with an updated list of records.
	if row, err = s.replace(input.ZoneName, func(row *Row) (Row, error) {
		if row == nil {
			return Row{}, errors.WithMessagef(ns.ErrZoneNotFound, input.ZoneName)
		}

		// TODO: organize records in the radix tree for a fast lookup.
		row.Records = append(row.Records, ns.NewRecord(row.Seq, input))
		idx = len(row.Records) - 1

		return *row, nil
	}); err != nil {
		return nil, err
	}

	return &row.Records[idx], nil
}

func (s *Storage) QueryZones(ctx context.Context) ([]ns.Zone, error) {
	return nil, nil
}

func (s *Storage) QueryRecords(ctx context.Context) (rr []ns.Record, err error) {
	var row *Row

	if err = s.conn.View(func(tx *bbolt.Tx) error {
		zones, err := s.getZones(tx)
		if err != nil {
			return err
		}

		row, err = unmarshalRow(zones.Get([]byte("google.com")))
		return err
	}); err != nil {
		return nil, err
	}

	return row.Records, nil
}
