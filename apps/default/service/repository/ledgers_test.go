package repository_test

import (
	"context"
	models2 "github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/tests/testdef"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LedgersSuite struct {
	tests.BaseTestSuite
	ledger *models2.Ledger
}

func (ls *LedgersSuite) setupFixtures(ctx context.Context, svc *frame.Service) {

	// Create test ledger.
	ledgersDB := repository.NewLedgerRepository(svc)

	lg, err := ledgersDB.Create(ctx, &models2.Ledger{Type: models2.LEDGER_TYPE_ASSET})
	if err != nil {
		ls.Errorf(err, "Error creating ledger")
	}

	ls.ledger = lg

}

func (ls *LedgersSuite) TestLedgersInfoAPI() {
	ls.WithTestDependancies(ls.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ls.CreateService(t, dep)
		ls.setupFixtures(ctx, svc)

		ledgersDB := repository.NewLedgerRepository(svc)
		lg, err := ledgersDB.GetByID(ctx, ls.ledger.ID)
		assert.Equal(t, nil, err, "Error while getting ledger "+lg.ID)
		assert.Equal(t, lg.ID, lg.ID, "Invalid ledger id")

	})
}

func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
