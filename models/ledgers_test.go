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

type LedgersSuite struct {
	suite.Suite
	db *sql.DB
	insertedLedgerId int64
}

func (ls *LedgersSuite) SetupTest() {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	assert.NotEmpty(ls.T(), databaseURL)
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Panic("Unable to connect to Database:", err)
	} else {
		log.Println("Successfully established connection to database.")
		ls.db = db
	}

	//Create test ledger.
	ledgersDB := NewLedgerDB(db)
	ls.insertedLedgerId, err = ledgersDB.CreateLedger(&Ledger{Type: "ASSET",})
	if err != nil {
		log.Panic("Unable to create ledger for account", err)
	}

}

func (ls *LedgersSuite) TestLedgersInfoAPI() {
	t := ls.T()

	ledgersDB := NewLedgerDB(ls.db)
	ledger, err := ledgersDB.GetByID(ls.insertedLedgerId)
	assert.Equal(t, nil, err, "Error while getting ledger "+string(ls.insertedLedgerId))
	assert.Equal(t, ls.insertedLedgerId, ledger.ID, "Invalid ledger id")

}

func (ls *LedgersSuite) TearDownSuite() {
	log.Println("Cleaning up the model ledger test database")

	t := ls.T()
	_, err := ls.db.Exec(`DELETE FROM ledgers WHERE ledger_id = $1`, ls.insertedLedgerId)
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
}


func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
