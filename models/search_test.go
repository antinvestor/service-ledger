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

type SearchSuite struct {
	suite.Suite
	db    *sql.DB
	ledgerDB LedgerDB
	accDB AccountDB
	txnDB TransactionDB
	ledgerId int64
}

func (ss *SearchSuite) SetupSuite() {
	t := ss.T()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	assert.NotEmpty(t, databaseURL)
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Panic("Unable to connect to Database:", err)
	} else {
		log.Println("Successfully established connection to database.")
		ss.db = db
	}
	log.Println("Successfully established connection to database.")
	ss.accDB = NewAccountDB(db)
	ss.txnDB = NewTransactionDB(db)
	ss.ledgerDB = NewLedgerDB(db)

	ledgerID, err := ss.ledgerDB.CreateLedger(&Ledger{Type: "ASSET",})
	if err != nil {
		log.Panic("Unable to create ledger for account", err)
	}

	// Create test accounts
	acc1 := &Account{
		Reference: "acc1",
		LedgerID: ledgerID,
		Data: map[string]interface{}{
			"customer_id": "C1",
			"status":      "active",
			"created":     "2017-01-01",
		},
	}
	err = ss.accDB.CreateAccount(acc1)
	assert.Equal(t, nil, err, "Error creating test account")
	acc2 := &Account{
		Reference: "acc2",
		LedgerID: ledgerID,
		Data: map[string]interface{}{
			"customer_id": "C2",
			"status":      "inactive",
			"created":     "2017-06-30",
		},
	}
	err = ss.accDB.CreateAccount(acc2)
	assert.Equal(t, nil, err, "Error creating test account")

	// Create test transactions
	txn1 := &Transaction{
		Reference: "txn1",
		Entries: []*TransactionEntry{
			{
				Account: "acc1",
				Amount:    1000,
			},
			{
				Account: "acc2",
				Amount:    -1000,
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-01",
			"months": []string{"jan", "feb", "mar"},
		},
	}
	ok := ss.txnDB.Transact(txn1)
	assert.Equal(t, true, ok, "Error creating test transaction")
	txn2 := &Transaction{
		Reference: "txn2",
		Entries: []*TransactionEntry{
			{
				Account: "acc1",
				Amount:    100,
			},
			{
				Account: "acc2",
				Amount:    -100,
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-15",
			"months": []string{"apr", "may", "jun"},
		},
	}
	ok = ss.txnDB.Transact(txn2)
	assert.Equal(t, true, ok, "Error creating test transaction")
	txn3 := &Transaction{
		Reference: "txn3",
		Entries: []*TransactionEntry{
			{
				Account: "acc1",
				Amount:    400,
			},
			{
				Account: "acc2",
				Amount:    -400,
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-30",
			"months": []string{"jul", "aug", "sep"},
		},
	}
	ok = ss.txnDB.Transact(txn3)
	assert.Equal(t, true, ok, "Error creating test transaction")
}

func (ss *SearchSuite) TestSearchAccountsWithBothMustAndShould() {
	t := ss.T()
	engine, _ := NewSearchEngine(ss.db, "accounts")

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
	results, err := engine.Query(query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*AccountResult)
	assert.Equal(t, 1, len(accounts), "Account count doesn't match")
	assert.Equal(t, "acc1", accounts[0].Reference, "Account Reference doesn't match")
}

func (ss *SearchSuite) TestSearchTransactionsWithBothMustAndShould() {
	t := ss.T()
	engine, _ := NewSearchEngine(ss.db, "transactions")

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
	results, err := engine.Query(query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ := results.([]*TransactionResult)
	assert.Equal(t, 1, len(transactions), "Transaction count doesn't match")
	assert.Equal(t, "txn1", transactions[0].Reference, "Transaction Reference doesn't match")
}

func (ss *SearchSuite) TearDownSuite() {
	log.Println("Cleaning up the test database")

	t := ss.T()
	_, err := ss.db.Exec(`DELETE FROM entries WHERE transaction_id IN (
					SELECT transaction_id FROM transactions WHERE reference IN($1, $2, $3))`, "txn1", "txn2", "txn3")
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
	_, err = ss.db.Exec(`DELETE FROM transactions WHERE reference IN($1, $2, $3)`, "txn1", "txn2", "txn3")
	if err != nil {
		t.Fatal("Error deleting transactions:", err)
	}
	_, err = ss.db.Exec(`DELETE FROM accounts WHERE reference IN($1, $2)`, "acc1", "acc2")
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = ss.db.Exec(`DELETE FROM ledgers WHERE ledger_id = $1`, ss.ledgerId)
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
