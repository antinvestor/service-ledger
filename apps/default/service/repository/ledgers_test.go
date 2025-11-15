package repository_test

import (
	"context"
	"testing"

	models "github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LedgersSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func (ls *LedgersSuite) setupFixtures(ctx context.Context, svc *frame.Service) {
	// Create test ledger.
	ledgersDB := repository.NewLedgerRepository(svc)

	lg, err := ledgersDB.Create(ctx, &models.Ledger{Type: models.LedgerTypeAsset})
	if err != nil {
		ls.Errorf(err, "Error creating ledger")
	}

	ls.ledger = lg
}

func (ls *LedgersSuite) TestLedgersInfoAPI() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		svc, ctx, _ := ls.CreateService(t, dep)
		ls.setupFixtures(ctx, svc)

		ledgersDB := repository.NewLedgerRepository(svc)
		lg, err := ledgersDB.GetByID(ctx, ls.ledger.ID)
		assert.Nil(t, err, "Error while getting ledger "+lg.ID)
		assert.Equal(t, ls.ledger.ID, lg.ID, "Invalid ledger id")
	})
}

func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
