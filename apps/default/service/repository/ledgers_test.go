package repository_test

import (
	"context"
	"testing"

	models "github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/datastore"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LedgersSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func (ls *LedgersSuite) setupFixtures(ctx context.Context, svc *frame.Service) {
	// Create test ledger.
	dbPool := svc.DatastoreManager().GetPool(ctx, datastore.DefaultPoolName)
	workMan := svc.WorkManager()
	ledgersDB := repository.NewLedgerRepository(ctx, dbPool, workMan)

	lg := &models.Ledger{Type: models.LedgerTypeAsset}
	err := ledgersDB.Create(ctx, lg)
	if err != nil {
		ls.T().Fatalf("Error creating ledger: %v", err)
	}
	ls.ledger = lg
}

func (ls *LedgersSuite) TestLedgersInfoAPI() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, svc, _ := ls.CreateService(t, dep)
		ls.setupFixtures(ctx, svc)

		dbPool := svc.DatastoreManager().GetPool(ctx, datastore.DefaultPoolName)
		workMan := svc.WorkManager()
		ledgersDB := repository.NewLedgerRepository(ctx, dbPool, workMan)
		lg, err := ledgersDB.GetByID(ctx, ls.ledger.ID)
		require.NoError(t, err, "Error while getting ledger")
		assert.Equal(t, ls.ledger.ID, lg.ID, "Invalid ledger id")
	})
}

func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
