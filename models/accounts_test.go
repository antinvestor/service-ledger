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
	ledgerId int64
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

	as.ledgerId, err = ledgersDB.CreateLedger(&Ledger{Type: "ASSET",})
	if err != nil {
		log.Panic("Unable to create ledger for account", err)
	}
	accountsDB.CreateAccount(&Account{Reference:"100",LedgerID: as.ledgerId})


}

func (as *AccountsSuite) TestAccountsInfoAPI() {
	t := as.T()

	accountsDB := NewAccountDB(as.db)
	account, err := accountsDB.GetByRef("100")
	assert.Equal(t, nil, err, "Error while getting acccount")
	if err != nil{
		log.Fatalf("we still have errors %v", err)
	}
	assert.Equal(t, "100", account.Reference, "Invalid account Reference")
	assert.Equal(t, 0, account.Balance, "Invalid account balance")
}

func (as *AccountsSuite) TearDownSuite() {

	t := as.T()
	_, err :=  as.db.Exec(`DELETE FROM accounts WHERE reference=$1`, "100")
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = as.db.Exec(`DELETE FROM ledgers WHERE ledger_id = $1`, as.ledgerId)
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}

func TestAccountsSuite(t *testing.T) {
	suite.Run(t, new(AccountsSuite))
}
