package nameql

import (
	"context"
	"net/http"

	"github.com/hectodns/hectodns/ns"

	"github.com/pkg/errors"
	"github.com/resly/resly"
)

type Backend interface {
	CreateZone(ctx context.Context, input ns.ZoneInput) (*ns.Zone, error)
	CreateRecord(ctx context.Context, input ns.RecordInput) (*ns.Record, error)
	QueryZones(ctx context.Context) ([]ns.Zone, error)
	QueryRecords(ctx context.Context) ([]ns.Record, error)

	Close(context.Context) error
}

type StubBackend struct{}

func (b StubBackend) CreateZone(context.Context, ns.ZoneInput) (*ns.Zone, error) {
	return nil, errors.New("not implemented")
}

func (b StubBackend) CreateRecord(context.Context, ns.RecordInput) (*ns.Record, error) {
	return nil, errors.New("not implemented")
}

func (b StubBackend) QueryZones(context.Context) ([]ns.Zone, error) {
	return nil, errors.New("not implemented")
}

func (b StubBackend) QueryRecords(context.Context) ([]ns.Record, error) {
	return nil, errors.New("not implemented")
}

func NewHandler(backend Backend) http.Handler {
	rs := resly.Server{Name: "hectodns"}

	rs.AddType(resly.NewType(ns.ZoneInput{}, nil))
	rs.AddType(resly.NewType(ns.Zone{}, nil))
	rs.AddType(resly.NewType(ns.RecordInput{}, nil))
	rs.AddType(resly.NewType(ns.Record{}, nil))

	rs.AddQuery("zones", backend.QueryZones)
	rs.AddQuery("records", backend.QueryRecords)

	rs.HandleMutation("createZone", backend.CreateZone)
	rs.HandleMutation("createRecord", backend.CreateRecord)

	return rs.HandleHTTP()
}
