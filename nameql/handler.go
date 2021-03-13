package nameql

import (
	"context"
	"net/http"

	"github.com/hectodns/hectodns/ns"

	"github.com/activegraph/activegraph"
	"github.com/pkg/errors"
)

type Backend interface {
	CreateZone(ctx context.Context, input ns.ZoneInput) (*ns.Zone, error)
	QueryZone(ctx context.Context, input struct{ Name string }) (*ns.Zone, error)
	QueryZones(ctx context.Context) ([]ns.Zone, error)

	CreateRecord(ctx context.Context, input ns.RecordInput) (*ns.Record, error)
	QueryRecord(ctx context.Context, input struct {
		ZoneName string
		ID       uint64
	}) (*ns.Record, error)
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
	rs := activegraph.Server{}

	rs.HandleType(activegraph.NewType(ns.ZoneInput{}, nil))
	rs.HandleType(activegraph.NewType(ns.Zone{}, nil))
	{
		rs.HandleQuery("zone", backend.QueryZone)
		rs.HandleQuery("zones", backend.QueryZones)
		rs.HandleMutation("createZone", backend.CreateZone)
	}

	rs.HandleType(activegraph.NewType(ns.RecordInput{}, nil))
	rs.HandleType(activegraph.NewType(ns.Record{}, nil))
	{
		rs.HandleQuery("record", backend.QueryRecord)
		rs.HandleQuery("records", backend.QueryRecords)
		rs.HandleMutation("createRecord", backend.CreateRecord)
	}

	return rs.HandleHTTP()
}
