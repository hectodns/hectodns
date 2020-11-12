package localdb

import (
	"context"
	"encoding"
	"encoding/json"
	"strconv"
	"time"

	"github.com/hectodns/hectodns/ns"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"go.etcd.io/bbolt"
)

const (
	origin = "@"
)

type ZoneRow struct {
	Zone ns.Zone `json:"zone"`
}

type RecordRow struct {
	Record ns.Record `json:"record"`
}

type Storage struct {
	conn *bbolt.DB
}

type Row interface {
	encoding.BinaryMarshaler
}

func NewStorage(path string) (*Storage, error) {
	// Prevent hangs on file opening and wait only for a second.
	conn, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}

	log.Info().Msgf("opened the storage %q", path)
	storage := Storage{conn: conn}

	return &storage, nil
}

func (s *Storage) Close(ctx context.Context) error {
	return s.conn.Close()
}

func (s *Storage) CreateZone(ctx context.Context, input ns.ZoneInput) (
	zone *ns.Zone, err error,
) {
	row := ZoneRow{Zone: ns.NewZone(input)}

	if err := s.conn.Update(func(tx *bbolt.Tx) error {
		zone, err := tx.CreateBucketIfNotExists([]byte(row.Zone.Name))
		if err != nil {
			return err
		}

		_, err = zone.CreateBucketIfNotExists([]byte("records"))
		if err != nil {
			return err
		}

		// TODO: create SOA record on zone creation.
		return nil
	}); err != nil {
		return nil, err
	}

	return &row.Zone, nil
}

func (s *Storage) CreateRecord(ctx context.Context, input ns.RecordInput) (
	r *ns.Record, err error,
) {
	var rr *ns.Record

	if err := s.conn.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(input.ZoneName))
		if root == nil {
			return errors.WithMessagef(err, "zone %q not found", input.ZoneName)
		}

		records := root.Bucket([]byte("records"))

		id, _ := records.NextSequence()
		rec := ns.NewRecord(int64(id), input)
		rr = &rec

		b, err := json.Marshal(RecordRow{Record: *rr})
		if err != nil {
			return err
		}

		return records.Put([]byte(strconv.FormatInt(int64(id), 10)), b)
	}); err != nil {
		return nil, err
	}

	return rr, nil
}

func (s *Storage) QueryZones(ctx context.Context) ([]ns.Zone, error) {
	var zones []ns.Zone

	if err := s.conn.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, row *bbolt.Bucket) error {
			zones = append(zones, ns.Zone{Name: string(name)})
			return nil
		})
	}); err != nil {
		return nil, err
	}

	return zones, nil
}

func (s *Storage) QueryRecords(ctx context.Context) (rr []ns.Record, err error) {
	if err := s.conn.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(_ []byte, zone *bbolt.Bucket) error {
			records := zone.Bucket([]byte("records"))

			return records.ForEach(func(k, v []byte) error {
				var r RecordRow
				err := json.Unmarshal(v, &r)
				if err != nil {
					return err
				}

				rr = append(rr, r.Record)
				return nil
			})
		})
	}); err != nil {
		return nil, err
	}

	return rr, nil
}
