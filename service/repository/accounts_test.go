package repository_test

import (
	models2 "github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/repository"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type AccountsSuite struct {
	BaseTestSuite
	ledger *models2.Ledger
}

func (as *AccountsSuite) SetupSuite() {

	as.BaseTestSuite.SetupSuite()

	//Create test accounts.
	ledgersDB := repository.NewLedgerRepository(as.service)
	accountsDB := repository.NewAccountRepository(as.service)

	as.ledger = &models2.Ledger{Type: models2.LEDGER_TYPE_ASSET}
	var err error
	as.ledger, err = ledgersDB.Create(as.ctx, as.ledger)
	if err != nil {
		as.Errorf(err, "Unable to create ledger for account")
	}

	account := &models2.Account{LedgerID: as.ledger.ID, Currency: "UGX"}
	account.ID = "100"
	_, err = accountsDB.Create(as.ctx, account)
	if err != nil {
		as.Errorf(err, "Unable to create account")
	}
}

func (as *AccountsSuite) TestAccountsInfoAPI() {

	t := as.T()

	accountsDB := repository.NewAccountRepository(as.service)
	account, err := accountsDB.GetByID(as.ctx, "100")
	if err != nil {
		as.Errorf(err, "Error getting account info api account")
	} else {
		assert.Equal(t, nil, err, "Error while getting acccount")
		assert.Equal(t, "100", account.ID, "Invalid account Reference")
		assert.True(t, account.Balance.Valid && account.Balance.Decimal.IsZero(), "Invalid account balance")
	}
}

func TestAccountsSuite(t *testing.T) {
	suite.Run(t, new(AccountsSuite))
}
