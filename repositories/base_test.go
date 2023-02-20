package repositories_test

import (
	"context"
	"github.com/antinvestor/service-ledger/config"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/suite"
)

type BaseTestSuite struct {
	suite.Suite
	service *frame.Service
	ctx     context.Context
}

func (bs *BaseTestSuite) Setup() {

	bs.ctx = context.Background()
	configLedger := config.LedgerConfig{
		ConfigurationDefault: frame.ConfigurationDefault{
			ServerPort:         "",
			DatabasePrimaryURL: "postgres://ant:secret@localhost:5434/service_profile?sslmode=disable",
			DatabaseReplicaURL: "postgres://ant:secret@localhost:5434/service_profile?sslmode=disable",
		},
		PartitionServiceURI: "",
	}

	bs.service = frame.NewService("ledger tests", frame.Config(&configLedger), frame.NoopDriver())
	_ = bs.service.Run(bs.ctx, "")
}
