package models

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type AccountsSuite struct {
	suite.Suite
	db *sql.DB
	ledger *Ledger

}

func (as *AccountsSuite) SetupTest() {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	assert.NotEmpty(as.T(), databaseURL)
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Panic("Unable to connect to Database:", err)
	}
	as.db = db

	//Create test accounts.
	ledgersDB := NewLedgerDB(as.db)
	accountsDB := NewAccountDB(as.db)

	as.ledger = &Ledger{Type: "ASSET"}
	as.ledger, err = ledgersDB.CreateLedger(as.ledger)
	if err != nil {
		as.Errorf(err, "Unable to create ledger for account")
	}
	accountsDB.CreateAccount(&Account{Reference:"100", LedgerID: as.ledger.ID,  Ledger: as.ledger.Reference.String, Currency: "UGX",})

}

func (as *AccountsSuite) TestAccountsInfoAPI() {
	t := as.T()

	accountsDB := NewAccountDB(as.db)
	account, err := accountsDB.GetByRef("100")
	if err != nil{
		as.Errorf(err,"Error getting account info api account")
	}else {
		assert.Equal(t, nil, err, "Error while getting acccount")
		assert.Equal(t, "100", account.Reference, "Invalid account Reference")
		assert.Equal(t, 0, account.Balance, "Invalid account balance")
	}
}




func (as *AccountsSuite) TearDownSuite() {

	t := as.T()
	_, err :=  as.db.Exec(`DELETE FROM accounts WHERE reference = $1`, "100")
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = as.db.Exec(`DELETE FROM ledgers WHERE ledger_id = $1`, as.ledger.ID)
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}

func TestAccountsSuite(t *testing.T) {
	suite.Run(t, new(AccountsSuite))
}
