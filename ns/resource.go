package ns

import (
	"time"
)

type ZoneInput struct {
	Name string `json:"name"`
}

type Zone struct {
	Name      string `json:"name"`
	CreatedAt int64  `json:"createdAt"`
	UpdatedAt int64  `json:"updatedAt"`
}

func NewZone(input ZoneInput) Zone {
	return Zone{
		Name:      input.Name,
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}
}

type RecordInput struct {
	ZoneName string  `json:"zoneName"`
	TTL      int     `json:"ttl"`
	Name     string  `json:"name"`
	Class    *string `json:"class"`
	Type     string  `json:"type"`
	Data     string  `json:"data"`
	Priority *string `json:"priority"`
}

type Record struct {
	ZoneName  string  `json:"zoneName"`
	TTL       int     `json:"ttl"`
	Name      string  `json:"name"`
	Class     *string `json:"class"`
	Type      string  `json:"type"`
	Data      string  `json:"data"`
	Priority  *string `json:"priority"`
	CreatedAt int64   `json:"createdAt"`
	UpdatedAt int64   `json:"updatedAt"`
}

func NewRecord(input RecordInput) Record {
	return Record{
		ZoneName:  input.ZoneName,
		TTL:       input.TTL,
		Name:      input.Name,
		Class:     input.Class,
		Type:      input.Type,
		Data:      input.Data,
		Priority:  input.Priority,
		CreatedAt: time.Now().Unix(),
		UpdatedAt: time.Now().Unix(),
	}
}
