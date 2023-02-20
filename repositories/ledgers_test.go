package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"log"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LedgersSuite struct {
	BaseTestSuite
	ledger *models.Ledger
}

func (ls *LedgersSuite) SetupTest() {
	ls.Setup()

	//Create test ledger.
	ledgersDB := repositories.NewLedgerRepository(ls.service)

	lg, err := ledgersDB.Create(ls.ctx, &models.Ledger{Type: models.LEDGER_TYPE_ASSET})
	if err != nil {
		ls.Errorf(err, "Error creating ledger", err)
	}

	ls.ledger = lg

}

func (ls *LedgersSuite) TestLedgersInfoAPI() {
	t := ls.T()

	ledgersDB := repositories.NewLedgerRepository(ls.service)
	lg, err := ledgersDB.GetByID(ls.ctx, ls.ledger.ID)
	assert.Equal(t, nil, err, "Error while getting ledger "+lg.ID)
	assert.Equal(t, lg.ID, lg.ID, "Invalid ledger id")

}

func (ls *LedgersSuite) TearDownSuite() {
	log.Println("Cleaning up the model ledger test database")

	t := ls.T()
	err := ls.service.DB(ls.ctx, false).Exec(`DELETE FROM ledgers WHERE id = $1`, ls.ledger.ID).Error
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
}

func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
