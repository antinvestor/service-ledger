package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
	"log"
	"sync"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TransactionsModelSuite struct {
	BaseTestSuite
	ledger *models.Ledger
}

func (ts *TransactionsModelSuite) SetupSuite() {
	ts.BaseTestSuite.SetupSuite()
	//Create test accounts.
	ledgersDB := repositories.NewLedgerRepository(ts.service)
	accountsDB := repositories.NewAccountRepository(ts.service)

	lg1, err := ledgersDB.Create(ts.ctx, &models.Ledger{Type: models.LEDGER_TYPE_ASSET})
	if err != nil {
		ts.Errorf(err, "Unable to create ledger for account")
	}
	lg2, err := ledgersDB.Create(ts.ctx, &models.Ledger{Type: models.LEDGER_TYPE_INCOME})
	if err != nil {
		ts.Errorf(err, "Unable to create ledger 2 for account")
	}
	ts.ledger = lg1
	_, err = accountsDB.Create(ts.ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a1"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a2"}, LedgerID: lg2.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a3"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a4"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models.Account{BaseModel: frame.BaseModel{ID: "b1"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models.Account{BaseModel: frame.BaseModel{ID: "b2"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}

}

func (ts *TransactionsModelSuite) TestIsValid() {
	t := ts.T()

	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t001"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "a1",
				Amount:    models.New(100),
			},
			{
				AccountID: "a2",
				Amount:    models.New(-100),
			},
		},
	}
	valid := transaction.IsValid()
	assert.Equal(t, valid, true, "Transaction should be valid")

	transaction.Entries[0].Amount = models.New(200)
	valid = transaction.IsValid()
	assert.Equal(t, valid, false, "Transaction should not be valid")
}

func (ts *TransactionsModelSuite) TestIsConflict() {
	t := ts.T()

	accountRepo := repositories.NewAccountRepository(ts.service)

	transactionRepository := repositories.NewTransactionRepository(ts.service, accountRepo)
	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t0015"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "a1",
				Amount:    models.New(100),
			},
			{
				AccountID: "a2",
				Amount:    models.New(-100),
			},
		},
	}
	done, err := transactionRepository.Transact(ts.ctx, transaction)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, done, "Transaction should be created")

	conflicts, err := transactionRepository.IsConflict(ts.ctx, transaction)
	assert.Equal(t, nil, err, "Error while checking for conflict transaction")
	assert.Equal(t, false, conflicts, "Transaction should not conflict")

	transaction = &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t0015"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "a1",
				Amount:    models.New(50),
			},
			{
				AccountID: "a2",
				Amount:    models.New(-50),
			},
		},
	}

	conflicts, err = transactionRepository.IsConflict(ts.ctx, transaction)
	assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
	assert.Equal(t, true, conflicts, "Transaction should conflict since amounts are different from first received")

	transaction = &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t0015"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "b1",
				Amount:    models.New(100),
			},
			{
				AccountID: "b2",
				Amount:    models.New(-100),
			},
		},
	}
	conflicts, err = transactionRepository.IsConflict(ts.ctx, transaction)
	assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
	assert.Equal(t, conflicts, true, "Transaction should conflict since accounts are different from first received")
}

func (ts *TransactionsModelSuite) TestTransact() {
	t := ts.T()

	accountRepo := repositories.NewAccountRepository(ts.service)
	transactionRepository := repositories.NewTransactionRepository(ts.service, accountRepo)

	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t003"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "a1",
				Amount:    models.New(100),
			},
			{
				AccountID: "a2",
				Amount:    models.New(-100),
			},
		},
		Data: map[string]interface{}{
			"tag1": "val1",
			"tag2": "val2",
		},
	}
	done, err := transactionRepository.Transact(ts.ctx, transaction)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, done, "Transaction should be created")

	exists, err := transactionRepository.GetByID(ts.ctx, "t003")
	assert.Equal(t, nil, err, "Error while checking for existing transaction")
	assert.Equal(t, "t003", exists.ID, "Transaction should exist")
}

func (ts *TransactionsModelSuite) TestDuplicateTransactions() {
	t := ts.T()

	accountRepo := repositories.NewAccountRepository(ts.service)
	transactionRepository := repositories.NewTransactionRepository(ts.service, accountRepo)
	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t005"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "a1",
				Amount:    models.New(100),
			},
			{
				AccountID: "a2",
				Amount:    models.New(-100),
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 1; i <= 5; i++ {
		go func(txn *models.Transaction) {
			trxn, _ := transactionRepository.Transact(ts.ctx, transaction)
			assert.NotEqual(t, nil, trxn, "Transaction creation should be success")
			wg.Done()
		}(transaction)
	}
	wg.Wait()

	exists, err := transactionRepository.GetByID(ts.ctx, "t005")
	assert.Equal(t, nil, err, "Error while checking for existing transaction")
	assert.Equal(t, "t005", exists.ID, "Transaction should exist")
}

func (ts *TransactionsModelSuite) TestTransactWithBoundaryValues() {
	t := ts.T()

	accountRepo := repositories.NewAccountRepository(ts.service)

	transactionRepository := repositories.NewTransactionRepository(ts.service, accountRepo)

	// In-boundary value transaction
	boundaryValue := int64(9223372036854775807) // Max +ve for 2^64
	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{ID: "t004"},
		Currency:  "UGX",
		Entries: []models.TransactionEntry{
			{
				AccountID: "a3",
				Amount:    models.New(boundaryValue),
			},
			{
				AccountID: "a4",
				Amount:    models.New(-boundaryValue),
			},
		},
		Data: map[string]interface{}{
			"tag1": "val1",
			"tag2": "val2",
		},
	}
	done, _ := transactionRepository.Transact(ts.ctx, transaction)
	assert.NotEqual(t, nil, done, "Transaction should be created")
	exists, err := transactionRepository.GetByID(ts.ctx, "t004")
	assert.Equal(t, nil, err, "Error while checking for existing transaction")
	assert.Equal(t, "t004", exists.ID, "Transaction should exist")

	// Out-of-boundary value transaction
	// Note: Not able write test case for out of boundary value here,
	// due to overflow error while compilation.
	// The test case is written in `package controllers` using JSON
}

func (ts *TransactionsModelSuite) TearDownSuite() {
	log.Println("Cleaning up the test database")

	t := ts.T()
	err := ts.service.DB(ts.ctx, false).Exec(`DELETE FROM transaction_entries WHERE transaction_id 
		IN (SELECT transaction_id FROM transactions WHERE id IN($1, $2, $3, $4,$5, $6 ))`, "t001", "t002", "t003", "t004", "t005", "t0015").Error
	if err != nil {
		t.Fatal("Error deleting Entries:", err)
	}
	err = ts.service.DB(ts.ctx, false).Exec(`DELETE FROM transactions WHERE id IN ($1, $2, $3, $4,$5, $6 )`, "t001", "t002", "t003", "t004", "t005", "t0015").Error
	if err != nil {
		t.Fatal("Error deleting transactions:", err)
	}
	err = ts.service.DB(ts.ctx, false).Exec(`DELETE FROM accounts WHERE id IN ($1, $2, $3, $4,$5,$6 )`, "a1", "a2", "a3", "a4", "b1", "b2").Error
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	err = ts.service.DB(ts.ctx, false).Exec(`DELETE FROM ledgers WHERE id=$1`, ts.ledger.ID).Error
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}

func TestTransactionsModelSuite(t *testing.T) {
	suite.Run(t, new(TransactionsModelSuite))
}
