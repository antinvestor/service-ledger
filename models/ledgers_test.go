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
	db     *sql.DB
	ledger *Ledger
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
	ledgersDB := NewLedgerDB(ls.db)
	ls.ledger = &Ledger{Type: "ASSET",}
	ls.ledger, err = ledgersDB.CreateLedger(ls.ledger)
	if err != nil {
		ls.Errorf(err,"Error creating ledger", err)
	}


}

func (ls *LedgersSuite) TestLedgersInfoAPI() {
	t := ls.T()

	ledgersDB := NewLedgerDB(ls.db)
	lg, err := ledgersDB.GetByRef(ls.ledger.Reference.String)
	assert.Equal(t, nil, err, "Error while getting ledger "+ lg.Reference.String)
	assert.Equal(t, lg.ID, lg.ID, "Invalid ledger id")

}

func (ls *LedgersSuite) TearDownSuite() {
	log.Println("Cleaning up the model ledger test database")

	t := ls.T()
	_, err := ls.db.Exec(`DELETE FROM ledgers WHERE reference = $1`, ls.ledger.Reference)
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
}


func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
