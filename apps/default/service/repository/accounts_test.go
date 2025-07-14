package repository_test

import (
	"context"
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	repository "github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/tests/testdef"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AccountsSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func (as *AccountsSuite) setupFixtures(ctx context.Context, svc *frame.Service) {
	// Create test accounts.
	ledgersDB := repository.NewLedgerRepository(svc)
	accountsDB := repository.NewAccountRepository(svc)

	as.ledger = &models.Ledger{Type: models.LedgerTypeAsset}
	var err error
	as.ledger, err = ledgersDB.Create(ctx, as.ledger)
	as.Require().NoError(err, "Unable to create ledger for account")

	account := &models.Account{LedgerID: as.ledger.ID, Currency: "UGX", LedgerType: models.LedgerTypeAsset}
	account.ID = "100"
	_, err = accountsDB.Create(ctx, account)
	if err != nil {
		as.Require().NoError(err, "Unable to create account")
	}
}

func (as *AccountsSuite) TestAccountsInfoAPI() {
	as.WithTestDependancies(as.T(), func(t *testing.T, dep *testdef.DependancyOption) {
		svc, ctx := as.CreateService(t, dep)
		as.setupFixtures(ctx, svc)

		accountsDB := repository.NewAccountRepository(svc)
		account, err := accountsDB.GetByID(ctx, "100")
		if err != nil {
			require.NoError(t, err, "Error getting account info api account")
		} else {
			assert.Nil(t, err, "Error while getting acccount")
			assert.Equal(t, "100", account.ID, "Invalid account Reference")
			assert.True(t, account.Balance.Valid && account.Balance.Decimal.IsZero(), "Invalid account balance")
		}
	})
}

func TestAccountsSuite(t *testing.T) {
	suite.Run(t, new(AccountsSuite))
}
