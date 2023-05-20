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

func (bs *BaseTestSuite) SetupSuite() {

	configLedger := config.LedgerConfig{
		ConfigurationDefault: frame.ConfigurationDefault{
			ServerPort:         "",
			DatabasePrimaryURL: "postgres://ant:secret@localhost:5437/service_ledger?sslmode=disable",
			DatabaseReplicaURL: "postgres://ant:secret@localhost:5437/service_ledger?sslmode=disable",
		},
		PartitionServiceURI: "",
	}

	bs.ctx, bs.service = frame.NewService("ledger tests",
		frame.Config(&configLedger),
		frame.Datastore(bs.ctx),
		frame.NoopDriver())
	_ = bs.service.Run(bs.ctx, "")
}
