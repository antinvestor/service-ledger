package repository_test

import (
	"context"
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AccountsSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func TestAccountsSuite(t *testing.T) {
	suite.Run(t, new(AccountsSuite))
}

func (as *AccountsSuite) setupFixtures(ctx context.Context, resources *tests.ServiceResources) {
	// Create test accounts using cached repositories
	ledgersDB := resources.LedgerRepository
	accountsDB := resources.AccountRepository

	as.ledger = &models.Ledger{Type: models.LedgerTypeAsset}
	err := ledgersDB.Create(ctx, as.ledger)
	as.Require().NoError(err, "Unable to create ledger for account")

	account := &models.Account{LedgerID: as.ledger.ID, Currency: "UGX", LedgerType: models.LedgerTypeAsset}
	account.ID = "100"
	err = accountsDB.Create(ctx, account)
	if err != nil {
		as.Require().NoError(err, "Unable to create account")
	}
}

func (as *AccountsSuite) TestAccountsInfoAPI() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)
		as.setupFixtures(ctx, resources)

		// Use cached account repository from dependencies
		account, err := resources.AccountRepository.GetByID(ctx, "100")
		require.NoError(t, err, "Error getting account info api account")
		assert.Equal(t, "100", account.ID, "Invalid account Reference")
		assert.True(t, account.Balance.Valid && account.Balance.Decimal.IsZero(), "Invalid account balance")
	})
}
