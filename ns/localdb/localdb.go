package localdb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hectodns/hectodns/ns"
	"github.com/rs/zerolog/log"

	"go.etcd.io/bbolt"
)

const (
	zonesBucketName = "zones"
)

// Local storage keeps records altogether with zone information within
// a single key. This structure represents joined zone information.
type Row struct {
	ns.Zone
	Records []ns.Record
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

	log.Info().Msgf("opened storage %q", path)

	return &Storage{conn: conn}, nil
}

func (s *Storage) Close(ctx context.Context) error {
	return s.conn.Close()
}

func (s *Storage) replace(name string, fn func(oldrow *Row) Row) (newrow Row, err error) {
	err = s.conn.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(zonesBucketName))
		if err != nil {
			return err
		}

		var oldrow *Row

		b := bucket.Get([]byte(name))
		if b != nil {
			err = json.Unmarshal(b, oldrow)
			if err != nil {
				return err
			}
		}

		newrow = fn(oldrow)
		b, err = json.Marshal(newrow)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(name), b)
	})

	if err != nil {
		newrow = Row{}
	}
	return newrow, err
}

func (s *Storage) CreateZone(ctx context.Context, input ns.ZoneInput) (
	zone *ns.Zone, err error,
) {
	row, err := s.replace(input.Name, func(_ *Row) Row {
		return Row{Zone: ns.NewZone(input)}
	})

	if err == nil {
		zone = &row.Zone
	}
	return
}

func (s *Storage) CreateRecord(ctx context.Context, input ns.RecordInput) (
	record *ns.Record, err error,
) {
	newrec := ns.NewRecord(input)

	_, err = s.replace(input.ZoneName, func(row *Row) Row {
		row.Records = append(row.Records, newrec)
		return *row
	})

	if err == nil {
		record = &newrec
	}
	return
}

func (s *Storage) QueryZones(ctx context.Context) ([]ns.Zone, error) {
	return nil, nil
}

func (s *Storage) QueryRecords(ctx context.Context) ([]ns.Record, error) {
	return nil, nil
}
