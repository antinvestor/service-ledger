package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
	"log"
	"math/big"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SearchSuite struct {
	BaseTestSuite
	ledgerDB repositories.LedgerRepository
	accDB    repositories.AccountRepository
	txnDB    repositories.TransactionRepository

	ledger *models.Ledger
}

func (ss *SearchSuite) SetupSuite() {

	ss.Setup()
	t := ss.T()

	log.Println("Successfully established connection to database.")
	ss.accDB = repositories.NewAccountRepository(ss.service)
	ss.txnDB = repositories.NewTransactionRepository(ss.service)
	ss.ledgerDB = repositories.NewLedgerRepository(ss.service)

	lg, err := ss.ledgerDB.Create(ss.ctx, &models.Ledger{Type: models.LEDGER_TYPE_ASSET})
	if err != nil {
		ss.Errorf(err, "Unable to create ledger for search account")
	}

	ss.ledger = lg
	// Create test accounts
	acc1 := &models.Account{
		BaseModel: frame.BaseModel{ID: "acc1"},
		LedgerID:  ss.ledger.ID,
		Currency:  "UGX",
		Data: map[string]interface{}{
			"customer_id": "C1",
			"status":      "active",
			"created":     "2017-01-01",
		},
	}
	acc1, err = ss.accDB.Create(ss.ctx, acc1)
	assert.Equal(t, nil, err, "Error creating test account with %s", err)
	acc2 := &models.Account{
		BaseModel: frame.BaseModel{ID: "acc2"},
		LedgerID:  ss.ledger.ID,
		Currency:  "UGX",
		Data: map[string]interface{}{
			"customer_id": "C2",
			"status":      "inactive",
			"created":     "2017-06-30",
		},
	}
	acc2, err = ss.accDB.Create(ss.ctx, acc2)
	assert.Equal(t, nil, err, "Error creating test account")

	// Create test transactions
	txn1 := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "txn1"},
		Entries: []*models.TransactionEntry{
			{
				AccountID: "acc1",
				Amount:    big.NewInt(1000),
			},
			{
				AccountID: "acc2",
				Amount:    big.NewInt(-1000),
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-01",
			"months": []string{"jan", "feb", "mar"},
		},
	}
	tx, err := ss.txnDB.Transact(ss.ctx, txn1)
	assert.Equal(t, nil, err, "Error creating test transaction")
	assert.NotEqual(t, nil, tx, "Error creating test transaction")
	txn2 := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "txn2"},
		Entries: []*models.TransactionEntry{
			{
				AccountID: "acc1",
				Amount:    big.NewInt(100),
			},
			{
				AccountID: "acc2",
				Amount:    big.NewInt(-100),
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-15",
			"months": []string{"apr", "may", "jun"},
		},
	}
	tx, err = ss.txnDB.Transact(ss.ctx, txn2)
	assert.NotEqual(t, nil, tx, "Error creating test transaction")
	txn3 := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "txn3"},
		Entries: []*models.TransactionEntry{
			{
				AccountID: "acc1",
				Amount:    big.NewInt(400),
			},
			{
				AccountID: "acc2",
				Amount:    big.NewInt(-400),
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-30",
			"months": []string{"jul", "aug", "sep"},
		},
	}
	tx, err = ss.txnDB.Transact(ss.ctx, txn3)
	assert.NotEqual(t, nil, tx, "Error creating test transaction")
}

func (ss *SearchSuite) TestSearchAccountsWithBothMustAndShould() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "accounts")

	query := `{
        "query": {
            "must": {
                "fields": [
                    {"reference": {"eq": "acc1"}}
                ],
                "terms": [
                    {"status": "active"}
                ]
            },
            "should": {
                "terms": [
                    {"customer_id": "C1"}
                ],
                "ranges": [
                    {"created": {"gte": "2018-01-01", "lte": "2018-01-30"}}
                ]
            }
        }
    }`
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*models.Account)
	assert.Equal(t, 1, len(accounts), "Account count doesn't match")
	assert.Equal(t, "ACC1", accounts[0].ID, "Account Reference doesn't match")
}

func (ss *SearchSuite) TestSearchTransactionsWithBothMustAndShould() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "transactions")

	query := `{
        "query": {
            "must": {
                "fields": [
                    {"reference": {"eq": "txn1"}}
                ],
                "terms": [
                    {"action": "setcredit"}
                ]
            },
            "should": {
                "terms": [
                    {"months": ["jan", "feb", "mar"]},
                    {"months": ["apr", "may", "jun"]},
                    {"months": ["jul", "aug", "sep"]}
                ],
                "ranges": [
                    {"expiry": {"gte": "2018-01-01", "lte": "2018-01-30"}}
                ]
            }
        }
    }`
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ := results.([]*models.Transaction)
	assert.Equal(t, 1, len(transactions), "Transaction count doesn't match")
	if len(transactions) > 0 {
		assert.Equal(t, "TXN1", transactions[0].ID, "Transaction Reference doesn't match")
	}
}

func (ss *SearchSuite) TearDownSuite() {
	log.Println("Cleaning up the test database")

	t := ss.T()
	err := ss.service.DB(ss.ctx, false).Exec(`DELETE FROM entries WHERE transaction_id IN (
					SELECT transaction_id FROM transactions WHERE ID IN($1, $2, $3))`, "TXN1", "TXN2", "TXN3").Error
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
	err = ss.service.DB(ss.ctx, false).Exec(`DELETE FROM transactions WHERE ID IN($1, $2, $3)`, "TXN1", "TXN2", "TXN3").Error
	if err != nil {
		t.Fatal("Error deleting transactions:", err)
	}
	err = ss.service.DB(ss.ctx, false).Exec(`DELETE FROM accounts WHERE ID IN($1, $2)`, "ACC1", "ACC2").Error
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	err = ss.service.DB(ss.ctx, false).Exec(`DELETE FROM ledgers WHERE id = $1`, ss.ledger.ID).Error
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
