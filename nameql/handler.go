package nameql

import (
	"context"
	"net/http"

	"github.com/hectodns/hectodns/ns"

	"github.com/activegraph/activegraph"
	"github.com/pkg/errors"
)

type ActiveStorage interface {
	CreateZone(ctx context.Context, input ns.ZoneInput) (*ns.Zone, error)
	QueryZone(ctx context.Context, input struct{ Name string }) (*ns.Zone, error)
	QueryZones(ctx context.Context) ([]ns.Zone, error)

	CreateRecord(ctx context.Context, input ns.RecordInput) (*ns.Record, error)
	QueryRecord(ctx context.Context, input struct {
		ZoneName string
		ID       uint64
	}) (*ns.Record, error)
	QueryRecords(ctx context.Context) ([]ns.Record, error)

	// OpenReadTransaction creates a new read-only transaction to execute the
	// provided operation.
	//
	// This method is executed before all query operations to make the result
	// consistent. For backends without transactional support, consider simply
	// calling a specified operation.
	OpenReadTransaction(ctx context.Context, op func(context.Context) error) error

	// OpenWriteTransaction creates a read/write tansaction to execute the
	// provided operation.
	//
	// This method is executed before all mutating operations.
	OpenWriteTransaction(ctx context.Context, op func(context.Context) error) error

	// Close closes a storage.
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

func NewHandler(activestorage ActiveStorage) http.Handler {
	rs := activegraph.Server{}

	// In case of the mutation operation, open a single write transaction for
	// all mutations specified, so the changes are always atomic.
	rs.AppendAroundOp(activegraph.OperationMutation, func(
		rw activegraph.ResponseWriter, r *activegraph.Request, h activegraph.Handler,
	) {
		activestorage.OpenWriteTransaction(r.Context(), func(ctx context.Context) error {
			h.Serve(rw, r.WithContext(ctx))
			// TODO: abort transaction when operation has failed.
			return nil
		})
	})

	// Open a read-only transaction for query operations, some databases, like
	// "bbolt" are required to open a read-only transaction to consistently
	// access the data.
	rs.AppendAroundOp(activegraph.OperationQuery, func(
		rw activegraph.ResponseWriter, r *activegraph.Request, h activegraph.Handler,
	) {
		activestorage.OpenReadTransaction(r.Context(), func(ctx context.Context) error {
			h.Serve(rw, r.WithContext(ctx))
			return nil
		})
	})

	rs.HandleType(activegraph.NewType(ns.ZoneInput{}, nil))
	rs.HandleType(activegraph.NewType(ns.Zone{}, nil))
	{
		rs.HandleQuery("zone", activestorage.QueryZone)
		rs.HandleQuery("zones", activestorage.QueryZones)
		rs.HandleMutation("createZone", activestorage.CreateZone)
	}

	rs.HandleType(activegraph.NewType(ns.RecordInput{}, nil))
	rs.HandleType(activegraph.NewType(ns.Record{}, nil))
	{
		rs.HandleQuery("record", activestorage.QueryRecord)
		rs.HandleQuery("records", activestorage.QueryRecords)
		rs.HandleMutation("createRecord", activestorage.CreateRecord)
	}

	return rs.HandleHTTP()
}
