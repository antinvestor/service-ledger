package repository_test

import (
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	models2 "github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/repository"
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TransactionsModelSuite struct {
	BaseTestSuite
	ledger *models2.Ledger
}

func (ts *TransactionsModelSuite) SetupSuite() {
	ts.BaseTestSuite.SetupSuite()

	//Create test accounts.
	ledgersDB := repository.NewLedgerRepository(ts.service)
	accountsDB := repository.NewAccountRepository(ts.service)

	lg1, err := ledgersDB.Create(ts.ctx, &models2.Ledger{Type: models2.LEDGER_TYPE_ASSET})
	if err != nil {
		ts.Errorf(err, "Unable to create ledger for account")
	}
	lg2, err := ledgersDB.Create(ts.ctx, &models2.Ledger{Type: models2.LEDGER_TYPE_INCOME})
	if err != nil {
		ts.Errorf(err, "Unable to create ledger 2 for account")
	}
	ts.ledger = lg1
	_, err = accountsDB.Create(ts.ctx, &models2.Account{BaseModel: frame.BaseModel{ID: "a1"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models2.Account{BaseModel: frame.BaseModel{ID: "a2"}, LedgerID: lg2.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models2.Account{BaseModel: frame.BaseModel{ID: "a3"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models2.Account{BaseModel: frame.BaseModel{ID: "a4"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models2.Account{BaseModel: frame.BaseModel{ID: "b1"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ts.ctx, &models2.Account{BaseModel: frame.BaseModel{ID: "b2"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}

}

func (ts *TransactionsModelSuite) TestIsZeroSum() {
	t := ts.T()

	timeNow := time.Now().UTC()

	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t001"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
	}
	valid := transaction.IsZeroSum()
	assert.Equal(t, valid, true, "Transaction should be zero summed")

	transaction.Entries[0].Amount = decimal.NewNullDecimal(decimal.NewFromInt(200))
	valid = transaction.IsZeroSum()
	assert.Equal(t, valid, false, "Transaction should not be zero summed")
}

func (ts *TransactionsModelSuite) TestIsTrueDrCr() {
	t := ts.T()

	timeNow := time.Now().UTC()
	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t001"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(30)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(30)),
			},
		},
	}
	valid := transaction.IsTrueDrCr()
	assert.Equal(t, valid, true, "Transaction should contain one dr and other cr entries")

	transaction.Entries[0].Credit = true
	valid = transaction.IsTrueDrCr()
	assert.Equal(t, valid, false, "Transaction should fail DrCr test")
}

func (ts *TransactionsModelSuite) TestIsConflict() {
	t := ts.T()

	timeNow := time.Now().UTC()
	accountRepo := repository.NewAccountRepository(ts.service)

	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)
	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t0015"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
	}
	done, err := transactionRepository.Transact(ts.ctx, transaction)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, done, "Transaction should be created")

	conflicts, err := transactionRepository.IsConflict(ts.ctx, transaction)
	assert.Equal(t, nil, err, "Error while checking for conflict transaction")
	assert.Equal(t, false, conflicts, "Transaction should not conflict")

	transaction = &models2.Transaction{
		BaseModel:    frame.BaseModel{ID: "t0015"},
		Currency:     "UGX",
		TransactedAt: &timeNow,
		ClearedAt:    &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(50)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(50)),
			},
		},
	}

	conflicts, err = transactionRepository.IsConflict(ts.ctx, transaction)
	assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
	assert.Equal(t, true, conflicts, "Transaction should conflict since amounts are different from first received")

	transaction = &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t0015"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "b1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "b2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
	}
	conflicts, err = transactionRepository.IsConflict(ts.ctx, transaction)
	assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
	assert.Equal(t, conflicts, true, "Transaction should conflict since accounts are different from first received")
}

func (ts *TransactionsModelSuite) TestTransact() {
	t := ts.T()

	timeNow := time.Now().UTC()
	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t003"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
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

func (ts *TransactionsModelSuite) TestReserveTransaction() {
	t := ts.T()

	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	initialAcc, err := accountRepo.GetByID(ts.ctx, "a3")
	assert.NoError(t, err)

	timeNow := time.Now().UTC()
	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t031"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_RESERVATION.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a3",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(98)),
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

	exists, err := transactionRepository.GetByID(ts.ctx, "t031")
	assert.Equal(t, nil, err, "Error while checking for existing transaction")
	assert.Equal(t, "t031", exists.ID, "Transaction should exist")

	finalAcc, err := accountRepo.GetByID(ts.ctx, "a3")
	assert.NoError(t, err)

	assert.Equal(t, decimal.NewFromInt(0), finalAcc.Balance.Decimal.Sub(initialAcc.Balance.Decimal), "Reservation Balance should be consistent")

	assert.Equal(t, decimal.NewFromInt(98), finalAcc.ReservedBalance.Decimal, "reserved balance should be consistent")
}

func (ts *TransactionsModelSuite) TestTransactBalanceCheck() {
	t := ts.T()

	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	initialAccMap, err := accountRepo.ListByID(ts.ctx, "a3", "a4")
	assert.NoError(t, err)

	timeNow := time.Now().UTC()
	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t008"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a3",
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(51)),
				Credit:    false,
			},
			{
				AccountID: "a4",
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(51)),
				Credit:    true,
			},
		},
		Data: map[string]interface{}{
			"tag1": "transaction balance check",
		},
	}
	done, err1 := transactionRepository.Transact(ts.ctx, transaction)
	assert.NoError(t, err1)
	assert.NotEqual(t, nil, done, "Transaction should be created")

	finalAccMap, err2 := accountRepo.ListByID(ts.ctx, "a3", "a4")
	assert.NoError(t, err2)

	assert.Equal(t, decimal.NewFromInt(51), finalAccMap["a3"].Balance.Decimal.Sub(initialAccMap["a3"].Balance.Decimal), "Debited Balance should be equal")
	assert.Equal(t, decimal.NewFromInt(-51), finalAccMap["a4"].Balance.Decimal.Sub(initialAccMap["a4"].Balance.Decimal), "Credited Balance should be equal")

}

func (ts *TransactionsModelSuite) TestDuplicateTransactions() {
	t := ts.T()

	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	timeNow := time.Now().UTC()

	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t005"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 1; i <= 5; i++ {
		go func(txn *models2.Transaction) {
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
func (ts *TransactionsModelSuite) TestTransactionReversaL() {
	t := ts.T()

	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	timeNow := time.Now().UTC()

	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t053"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "a2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
	}

	trxn, err := transactionRepository.Transact(ts.ctx, transaction)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, trxn, "Transaction creation should be success")

	reversal, err := transactionRepository.Reverse(ts.ctx, trxn.ID)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, reversal, "Transaction reversal should be success")

	exists, err := transactionRepository.GetByID(ts.ctx, "t053_REVERSAL")
	assert.Equal(t, nil, err, "Error while checking for existing transaction")
	assert.Equal(t, "t053_REVERSAL", exists.ID, "Transaction should exist")
	assert.Equal(t, ledgerV1.TransactionType_REVERSAL.String(), exists.TransactionType, "Transaction type is not setup correctly")
}

func (ts *TransactionsModelSuite) TestUnClearedTransactions() {
	t := ts.T()

	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	initialAccMap, err := accountRepo.ListByID(ts.ctx, "b1", "b2")
	assert.NoError(t, err)

	timeNow := time.Now().UTC()

	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t051"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "b1",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
			{
				AccountID: "b2",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(100)),
			},
		},
	}

	done, err1 := transactionRepository.Transact(ts.ctx, transaction)
	assert.NoError(t, err1)
	assert.NotEqual(t, nil, done, "Transaction should be created")

	finalAccMap, err2 := accountRepo.ListByID(ts.ctx, "b1", "b2")
	assert.NoError(t, err2)

	assert.Equal(t, decimal.NewFromInt(0), finalAccMap["b1"].Balance.Decimal.Sub(initialAccMap["b1"].Balance.Decimal), "Debited Balance should be equal")
	assert.Equal(t, decimal.NewFromInt(0), finalAccMap["b2"].Balance.Decimal.Sub(initialAccMap["b2"].Balance.Decimal), "Credited Balance should be equal")

	assert.Equal(t, decimal.NewFromInt(100), finalAccMap["b1"].UnClearedBalance.Decimal, "b1 Uncleared balance should be equal")
	assert.Equal(t, decimal.NewFromInt(-100), finalAccMap["b2"].UnClearedBalance.Decimal, "b2 Uncleared balance should be equal")

	assert.Equal(t, decimal.NewFromInt(0), finalAccMap["b1"].ReservedBalance.Decimal, "b1 reserved balance should be zero")
	assert.Equal(t, decimal.NewFromInt(0), finalAccMap["b2"].ReservedBalance.Decimal, "b2 reserved balance should be zero")

}

func (ts *TransactionsModelSuite) TestTransactWithBoundaryValues() {
	t := ts.T()

	accountRepo := repository.NewAccountRepository(ts.service)
	transactionRepository := repository.NewTransactionRepository(ts.service, accountRepo)

	timeNow := time.Now().UTC()

	// In-boundary value transaction
	boundaryValue := int64(9223372036854775807) // Max +ve for 2^64
	transaction := &models2.Transaction{
		BaseModel:       frame.BaseModel{ID: "t004"},
		Currency:        "UGX",
		TransactionType: ledgerV1.TransactionType_NORMAL.String(),
		TransactedAt:    &timeNow,
		ClearedAt:       &timeNow,
		Entries: []*models2.TransactionEntry{
			{
				AccountID: "a3",
				Credit:    false,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(boundaryValue)),
			},
			{
				AccountID: "a4",
				Credit:    true,
				Amount:    decimal.NewNullDecimal(decimal.NewFromInt(boundaryValue)),
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

func TestTransactionsModelSuite(t *testing.T) {
	suite.Run(t, new(TransactionsModelSuite))
}
