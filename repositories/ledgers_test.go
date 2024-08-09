package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LedgersSuite struct {
	BaseTestSuite
	ledger *models.Ledger
}

func (ls *LedgersSuite) SetupSuite() {
	ls.BaseTestSuite.SetupSuite()

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

func TestLedgersSuite(t *testing.T) {
	suite.Run(t, new(LedgersSuite))
}
