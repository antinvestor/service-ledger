package repositories_test

import (
	"database/sql"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
	"log"
	"testing"
	"time"

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

func toSlice[T any](result <-chan any) ([]T, error) {
	var resultSlice []T
	for t := range result {
		switch v := t.(type) {
		case T:
			resultSlice = append(resultSlice, v)
		case error:
			return nil, v
		}
	}
	return resultSlice, nil
}

func (ss *SearchSuite) SetupSuite() {

	ss.BaseTestSuite.SetupSuite()

	t := ss.T()

	log.Println("Successfully established connection to database.")
	ss.accDB = repositories.NewAccountRepository(ss.service)
	ss.txnDB = repositories.NewTransactionRepository(ss.service, ss.accDB)
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
	_, err = ss.accDB.Create(ss.ctx, acc1)
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
	_, err = ss.accDB.Create(ss.ctx, acc2)
	assert.Equal(t, nil, err, "Error creating test account")

	// Create test transactions
	txn1 := &models.Transaction{
		BaseModel:       frame.BaseModel{ID: "txn1"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    sql.NullTime{Time: time.Now().UTC(), Valid: true},
		ClearedAt:       sql.NullTime{Time: time.Now().UTC(), Valid: true},
		Entries: []*models.TransactionEntry{
			{
				AccountID: "acc1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(1000)),
			},
			{
				AccountID: "acc2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(1000)),
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
		BaseModel:       frame.BaseModel{ID: "txn2"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    sql.NullTime{Time: time.Now().UTC(), Valid: true},
		ClearedAt:       sql.NullTime{Time: time.Now().UTC(), Valid: true},
		Entries: []*models.TransactionEntry{
			{
				AccountID: "acc1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "acc2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-15",
			"months": []string{"apr", "may", "jun"},
		},
	}
	tx, _ = ss.txnDB.Transact(ss.ctx, txn2)
	assert.NotEqual(t, nil, tx, "Error creating test transaction")
	txn3 := &models.Transaction{
		BaseModel:       frame.BaseModel{ID: "txn3"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    sql.NullTime{Time: time.Now().UTC(), Valid: true},
		ClearedAt:       sql.NullTime{Time: time.Now().UTC(), Valid: true},
		Entries: []*models.TransactionEntry{
			{
				AccountID: "acc1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(400)),
			},
			{
				AccountID: "acc2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(400)),
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-30",
			"months": []string{"jul", "aug", "sep"},
		},
	}
	tx, err = ss.txnDB.Transact(ss.ctx, txn3)

	assert.NoError(t, err)

	assert.NotEqual(t, nil, tx, "Error creating test transaction")
}

func (ss *SearchSuite) TestSearchAccountsWithBothMustAndShould() {
	t := ss.T()
	ctx := ss.ctx

	query := `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "acc1"}}
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

	resultChannel, err := ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err := toSlice[*models.Account](resultChannel)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(accounts), "Account count doesn't match")
	assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
}

func (ss *SearchSuite) TestSearchTransactionsWithBothMustAndShould() {
	t := ss.T()
	ctx := ss.ctx

	query := `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "txn1"}}
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
	resultChannel, err := ss.txnDB.Search(ctx, query)
	assert.NoError(t, err)
	transactions, err := toSlice[*models.Transaction](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(transactions), "Transaction count doesn't match")
	if len(transactions) > 0 {
		assert.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")
	}
}

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
