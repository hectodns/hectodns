package nameql

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/resly/resly"
)

type ZoneInput struct {
	Name string `json:"name"`
}

type Zone struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt int    `json:"createdAt"`
	UpdatedAt int    `json:"updatedAt"`
}

type RecordInput struct {
	ZoneID   string  `json:"zoneId"`
	TTL      int     `json:"ttl"`
	Name     string  `json:"name"`
	Class    *string `json:"class"`
	Type     string  `json:"type"`
	Data     string  `json:"data"`
	Priority *string `json:"priority"`
}

type Record struct {
	ID        string  `json:"id"`
	ZoneID    string  `json:"zoneId"`
	TTL       int     `json:"ttl"`
	Name      string  `json:"name"`
	Class     *string `json:"class"`
	Type      string  `json:"type"`
	Data      string  `json:"data"`
	Priority  *string `json:"priority"`
	CreatedAt int     `json:"createdAt"`
	UpdatedAt int     `json:"updatedAt"`
}

type Backend interface {
	CreateZone(ctx context.Context, input ZoneInput) (*Zone, error)
	CreateRecord(ctx context.Context, input RecordInput) (*Record, error)
	QueryZones(ctx context.Context) ([]Zone, error)
	QueryRecords(ctx context.Context) ([]Record, error)
}

type StubBackend struct{}

func (b StubBackend) CreateZone(context.Context, ZoneInput) (*Zone, error) {
	return nil, errors.New("not implemented")
}

func (b StubBackend) CreateRecord(context.Context, RecordInput) (*Record, error) {
	return nil, errors.New("not implemented")
}

func (b StubBackend) QueryZones(context.Context) ([]Zone, error) {
	return nil, errors.New("not implemented")
}

func (b StubBackend) QueryRecords(context.Context) ([]Record, error) {
	return nil, errors.New("not implemented")
}

func NewHandler(backend Backend) http.Handler {
	rs := resly.Server{Name: "hectodns"}

	rs.AddType(resly.NewType(ZoneInput{}, nil))
	rs.AddType(resly.NewType(Zone{}, nil))
	rs.AddType(resly.NewType(RecordInput{}, nil))
	rs.AddType(resly.NewType(Record{}, nil))

	rs.AddQuery("zones", backend.QueryZones)
	rs.AddQuery("records", backend.QueryRecords)

	rs.HandleMutation("createZone", backend.CreateZone)
	rs.HandleMutation("createRecord", backend.CreateRecord)

	return rs.HandleHTTP()
}
