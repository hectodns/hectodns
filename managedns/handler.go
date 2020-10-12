package managedns

import (
	"context"
	"net/http"

	"github.com/resly/resly"
)

type DomainInput struct {
	Name string `json:"name"`
}

type Domain struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt int    `json:"createdAt"`
	UpdatedAt int    `json:"updatedAt"`
}

type RecordInput struct {
	DomainID string  `json:"domainId"`
	TTL      int     `json:"ttl"`
	Name     string  `json:"name"`
	Class    *string `json:"class"`
	Type     string  `json:"type"`
	Data     string  `json:"data"`
	Priority *string `json:"priority"`
}

type Record struct {
	ID        string  `json:"id"`
	DomainID  string  `json:"domainId"`
	TTL       int     `json:"ttl"`
	Name      string  `json:"name"`
	Class     *string `json:"class"`
	Type      string  `json:"type"`
	Data      string  `json:"data"`
	Priority  *string `json:"priority"`
	CreatedAt int     `json:"createdAt"`
	UpdatedAt int     `json:"updatedAt"`
}

func createDomain(ctx context.Context, input DomainInput) (*Domain, error) {
	return nil, nil
}

func createRecord(ctx context.Context, input RecordInput) (*Record, error) {
	return nil, nil
}

func queryRecords(ctx context.Context) ([]Record, error) {
	return nil, nil
}

func queryDomains(ctx context.Context) ([]Domain, error) {
	return nil, nil
}

func HandleGraphQL() http.Handler {
	rs := resly.Server{Name: "hectodns"}

	rs.AddType(resly.NewType(DomainInput{}, nil))
	rs.AddType(resly.NewType(Domain{}, nil))
	rs.AddType(resly.NewType(RecordInput{}, nil))
	rs.AddType(resly.NewType(Record{}, nil))

	rs.AddQuery("records", queryRecords)
	rs.AddQuery("domains", queryDomains)

	rs.HandleMutation("createDomain", createDomain)
	rs.HandleMutation("createRecord", createRecord)

	return rs.HandleHTTP()
}
