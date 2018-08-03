package models

import (
	"database/sql"
	"log"
	"os"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TransactionsModelSuite struct {
	suite.Suite
	db *sql.DB
}

func (ts *TransactionsModelSuite) SetupSuite() {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	assert.NotEmpty(ts.T(), databaseURL)
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Panic("Unable to connect to Database:", err)
	} else {
		log.Println("Successfully established connection to database.")
		ts.db = db
	}

	//Create test accounts.
	ledgersDB := NewLedgerDB(ts.db)
	accountsDB := NewAccountDB(ts.db)


	ledgerID, err := ledgersDB.CreateLedger(&Ledger{Type: "ASSET",})
	if err != nil {
		log.Panic("Unable to create ledger for account", err)
	}
	accountsDB.CreateAccount(&Account{Reference:"a1",LedgerID: ledgerID})
	accountsDB.CreateAccount(&Account{Reference:"a2",LedgerID: ledgerID})
	accountsDB.CreateAccount(&Account{Reference:"a3",LedgerID: ledgerID})
	accountsDB.CreateAccount(&Account{Reference:"a4",LedgerID: ledgerID})
	accountsDB.CreateAccount(&Account{Reference:"b1",LedgerID: ledgerID})
	accountsDB.CreateAccount(&Account{Reference:"b2",LedgerID: ledgerID})

}

func (ts *TransactionsModelSuite) TestIsValid() {
	t := ts.T()

	transaction := &Transaction{
		Reference: "t001",
		Entries: []*TransactionEntry{
			{
				Account: "a1",
				Amount:    100,
			},
			{
				Account: "a2",
				Amount:    -100,
			},
		},
	}
	valid := transaction.IsValid()
	assert.Equal(t, valid, true, "Transaction should be valid")

	transaction.Entries[0].Amount = 200
	valid = transaction.IsValid()
	assert.Equal(t, valid, false, "Transaction should not be valid")
}

func (ts *TransactionsModelSuite) TestIsExists() {
	t := ts.T()

	transactionDB := NewTransactionDB(ts.db)
	exists, err := transactionDB.IsExists("t001")
	assert.Equal(t, err, nil, "Error while checking for existing transaction")
	assert.Equal(t, exists, false, "Transaction should not exist")

	transaction := &Transaction{
		Reference: "t001",
		Entries: []*TransactionEntry{
			{
				Account: "a1",
				Amount:    100,
			},
			{
				Account: "a2",
				Amount:    -100,
			},
		},
	}
	done := transactionDB.Transact(transaction)
	assert.Equal(t, done, true, "Transaction should be created")

	exists, err = transactionDB.IsExists("t001")
	assert.Equal(t, err, nil, "Error while checking for existing transaction")
	assert.Equal(t, exists, true, "Transaction should exist")
}

func (ts *TransactionsModelSuite) TestIsConflict() {
	t := ts.T()

	transactionDB := NewTransactionDB(ts.db)
	transaction := &Transaction{
		Reference: "t002",
		Entries: []*TransactionEntry{
			{
				Account: "a1",
				Amount:    100,
			},
			{
				Account: "a2",
				Amount:    -100,
			},
		},
	}
	done := transactionDB.Transact(transaction)
	assert.Equal(t, done, true, "Transaction should be created")

	conflicts, err := transactionDB.IsConflict(transaction)
	assert.Equal(t, nil, err, "Error while checking for conflict transaction")
	assert.Equal(t, false, conflicts, "Transaction should not conflict")

	transaction = &Transaction{
		Reference: "t002",
		Entries: []*TransactionEntry{
			{
				Account: "a1",
				Amount:    50,
			},
			{
				Account: "a2",
				Amount:    -50,
			},
		},
	}
	conflicts, err = transactionDB.IsConflict(transaction)
	assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
	assert.Equal(t, conflicts, true, "Transaction should conflict since amounts are different from first received")

	transaction = &Transaction{
		Reference: "t002",
		Entries: []*TransactionEntry{
			{
				Account: "b1",
				Amount:    100,
			},
			{
				Account: "b2",
				Amount:    -100,
			},
		},
	}
	conflicts, err = transactionDB.IsConflict(transaction)
	assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
	assert.Equal(t, conflicts, true, "Transaction should conflict since accounts are different from first received")
}

func (ts *TransactionsModelSuite) TestTransact() {
	t := ts.T()

	transactionDB := NewTransactionDB(ts.db)

	transaction := &Transaction{
		Reference: "t003",
		Entries: []*TransactionEntry{
			{
				Account: "a1",
				Amount:    100,
			},
			{
				Account: "a2",
				Amount:    -100,
			},
		},
		Data: map[string]interface{}{
			"tag1": "val1",
			"tag2": "val2",
		},
	}
	done := transactionDB.Transact(transaction)
	assert.Equal(t, done, true, "Transaction should be created")

	exists, err := transactionDB.IsExists("t003")
	assert.Equal(t, err, nil, "Error while checking for existing transaction")
	assert.Equal(t, exists, true, "Transaction should exist")
}

func (ts *TransactionsModelSuite) TestDuplicateTransactions() {
	t := ts.T()

	transactionDB := NewTransactionDB(ts.db)
	transaction := &Transaction{
		Reference: "t005",
		Entries: []*TransactionEntry{
			{
				Account: "a1",
				Amount:    100,
			},
			{
				Account: "a2",
				Amount:    -100,
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 1; i <= 5; i++ {
		go func(txn *Transaction) {
			done := transactionDB.Transact(transaction)
			assert.Equal(t, done, true, "Transaction creation should be success")
			wg.Done()
		}(transaction)
	}
	wg.Wait()

	exists, err := transactionDB.IsExists("t005")
	assert.Equal(t, err, nil, "Error while checking for existing transaction")
	assert.Equal(t, exists, true, "Transaction should exist")
}

func (ts *TransactionsModelSuite) TestTransactWithBoundaryValues() {
	t := ts.T()

	transactionDB := NewTransactionDB(ts.db)

	// In-boundary value transaction
	boundaryValue := 9223372036854775807 // Max +ve for 2^64
	transaction := &Transaction{
		Reference: "t004",
		Entries: []*TransactionEntry{
			{
				Account: "a3",
				Amount:    boundaryValue,
			},
			{
				Account: "a4",
				Amount:    -boundaryValue,
			},
		},
		Data: map[string]interface{}{
			"tag1": "val1",
			"tag2": "val2",
		},
	}
	done := transactionDB.Transact(transaction)
	assert.Equal(t, true, done, "Transaction should be created")
	exists, err := transactionDB.IsExists("t004")
	assert.Equal(t, nil, err, "Error while checking for existing transaction")
	assert.Equal(t, true, exists, "Transaction should exist")

	// Out-of-boundary value transaction
	// Note: Not able write test case for out of boundary value here,
	// due to overflow error while compilation.
	// The test case is written in `package controllers` using JSON
}

func (ts *TransactionsModelSuite) TearDownSuite() {
	log.Println("Cleaning up the test database")

	t := ts.T()
	_, err := ts.db.Exec(`DELETE FROM entries`)
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
	_, err = ts.db.Exec(`DELETE FROM transactions`)
	if err != nil {
		t.Fatal("Error deleting transactions:", err)
	}
	_, err = ts.db.Exec(`DELETE FROM accounts`)
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = ts.db.Exec(`DELETE FROM ledgers`)
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}

func TestTransactionsModelSuite(t *testing.T) {
	suite.Run(t, new(TransactionsModelSuite))
}
