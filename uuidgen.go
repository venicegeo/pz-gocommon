package piazza

import (
	"time"
)

type UuidGen interface {
	// low-level interfaces
	PostToUuids(count int) (*UuidGenResponse, error)
	GetFromAdminStats() (*UuidGenAdminStats, error)
	GetFromAdminSettings() (*UuidGenAdminSettings, error)
	PostToAdminSettings(*UuidGenAdminSettings) error
}

type UuidGenResponse struct {
	Data []string
}

type UuidGenAdminStats struct {
	StartTime   time.Time `json:"starttime"`
	NumRequests int       `json:"num_requests"`
	NumUUIDs    int       `json:"num_uuids"`
}

type UuidGenAdminSettings struct {
	Debug bool `json:"debug"`
}

type MockUuidGen struct{}

func (*MockUuidGen) PostToUuids(count int) (*UuidGenResponse, error) {
	m := &UuidGenResponse{Data: []string{"xxx"}}
	return m, nil
}

func (*MockUuidGen) GetFromAdminStats() (*UuidGenAdminStats, error) {
	return &UuidGenAdminStats{}, nil
}

func (*MockUuidGen) GetFromAdminSettings() (*UuidGenAdminSettings, error) {
	return &UuidGenAdminSettings{}, nil
}

func (*MockUuidGen) PostToAdminSettings(*UuidGenAdminSettings) error {
	return nil
}
