package repository_test

import (
	"context"
	"sync"
	"testing"
	"time"

	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	repository2 "github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	"github.com/antinvestor/service-ledger/internal/utility"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/tests/testdef"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TransactionsModelSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func (ts *TransactionsModelSuite) setupFixtures(ctx context.Context, svc *frame.Service) {

	// Create test accounts.
	ledgersDB := repository2.NewLedgerRepository(svc)
	accountsDB := repository2.NewAccountRepository(svc)

	lg1, err := ledgersDB.Create(ctx, &models.Ledger{Type: models.LEDGER_TYPE_ASSET})
	if err != nil {
		ts.Errorf(err, "Unable to create ledger for account")
	}
	lg2, err := ledgersDB.Create(ctx, &models.Ledger{Type: models.LEDGER_TYPE_INCOME})
	if err != nil {
		ts.Errorf(err, "Unable to create ledger 2 for account")
	}
	ts.ledger = lg1
	_, err = accountsDB.Create(ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a1"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a2"}, LedgerID: lg2.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a3"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ctx, &models.Account{BaseModel: frame.BaseModel{ID: "a4"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ctx, &models.Account{BaseModel: frame.BaseModel{ID: "b1"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}
	_, err = accountsDB.Create(ctx, &models.Account{BaseModel: frame.BaseModel{ID: "b2"}, LedgerID: ts.ledger.ID, Currency: "UGX"})
	if err != nil {
		ts.Errorf(err, "Unable to create account")
	}

}

func (ts *TransactionsModelSuite) TestIsZeroSum() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		timeNow := time.Now().UTC()

		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t001"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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

	})
}

func (ts *TransactionsModelSuite) TestIsTrueDrCr() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		timeNow := time.Now().UTC()
		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t001"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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

	})
}

func (ts *TransactionsModelSuite) TestIsConflict() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		timeNow := time.Now().UTC()
		accountRepo := repository2.NewAccountRepository(svc)

		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)
		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t0015"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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
		done, err := transactionRepository.Transact(ctx, transaction)
		assert.NoError(t, err)
		assert.NotEqual(t, nil, done, "Transaction should be created")

		conflicts, err := transactionRepository.IsConflict(ctx, transaction)
		assert.Equal(t, nil, err, "Error while checking for conflict transaction")
		assert.Equal(t, false, conflicts, "Transaction should not conflict")

		transaction = &models.Transaction{
			BaseModel:    frame.BaseModel{ID: "t0015"},
			Currency:     "UGX",
			TransactedAt: &timeNow,
			ClearedAt:    &timeNow,
			Entries: []*models.TransactionEntry{
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

		conflicts, err = transactionRepository.IsConflict(ctx, transaction)
		assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
		assert.Equal(t, true, conflicts, "Transaction should conflict since amounts are different from first received")

		transaction = &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t0015"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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
		conflicts, err = transactionRepository.IsConflict(ctx, transaction)
		assert.Equal(t, err, nil, "Error while checking for conflicting transaction")
		assert.Equal(t, conflicts, true, "Transaction should conflict since accounts are different from first received")

	})
}

func (ts *TransactionsModelSuite) TestTransact() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		timeNow := time.Now().UTC()
		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t003"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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
		done, err := transactionRepository.Transact(ctx, transaction)
		assert.NoError(t, err)
		assert.NotEqual(t, nil, done, "Transaction should be created")

		exists, err := transactionRepository.GetByID(ctx, "t003")
		assert.Equal(t, nil, err, "Error while checking for existing transaction")
		assert.Equal(t, "t003", exists.ID, "Transaction should exist")

	})
}

func (ts *TransactionsModelSuite) TestReserveTransaction() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		initialAcc, err := accountRepo.GetByID(ctx, "a3")
		assert.NoError(t, err)

		timeNow := time.Now().UTC()
		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t031"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_RESERVATION.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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
		done, err := transactionRepository.Transact(ctx, transaction)
		assert.NoError(t, err)
		assert.NotEqual(t, nil, done, "Transaction should be created")

		exists, err := transactionRepository.GetByID(ctx, "t031")
		assert.Equal(t, nil, err, "Error while checking for existing transaction")
		assert.Equal(t, "t031", exists.ID, "Transaction should exist")

		finalAcc, err := accountRepo.GetByID(ctx, "a3")
		assert.NoError(t, err)

		assert.Equal(t, decimal.NewFromInt(0), finalAcc.Balance.Decimal.Sub(initialAcc.Balance.Decimal), "Reservation Balance should be consistent")

		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(98)), finalAcc.ReservedBalance.Decimal, "reserved balance should be consistent")

	})
}

func (ts *TransactionsModelSuite) TestTransactBalanceCheck() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		initialAccMap, err := accountRepo.ListByID(ctx, "a3", "a4")
		assert.NoError(t, err)

		timeNow := time.Now().UTC()
		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t008"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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
		done, err1 := transactionRepository.Transact(ctx, transaction)
		assert.NoError(t, err1)
		assert.NotEqual(t, nil, done, "Transaction should be created")

		finalAccMap, err2 := accountRepo.ListByID(ctx, "a3", "a4")
		assert.NoError(t, err2)

		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(51)), finalAccMap["a3"].Balance.Decimal.Sub(initialAccMap["a3"].Balance.Decimal), "Debited Balance should be equal")
		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(-51)), finalAccMap["a4"].Balance.Decimal.Sub(initialAccMap["a4"].Balance.Decimal), "Credited Balance should be equal")

	})
}

func (ts *TransactionsModelSuite) TestDuplicateTransactions() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		timeNow := time.Now().UTC()

		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t005"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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
			go func(txn *models.Transaction) {
				trxn, _ := transactionRepository.Transact(ctx, txn)
				assert.NotEqual(t, nil, trxn, "Transaction creation should be success")
				wg.Done()
			}(transaction)
		}
		wg.Wait()

		exists, err := transactionRepository.GetByID(ctx, "t005")
		assert.Equal(t, nil, err, "Error while checking for existing transaction")
		assert.Equal(t, "t005", exists.ID, "Transaction should exist")

	})
}
func (ts *TransactionsModelSuite) TestTransactionReversaL() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		timeNow := time.Now().UTC()

		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t053"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
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

		trxn, err := transactionRepository.Transact(ctx, transaction)
		assert.NoError(t, err)
		assert.NotEqual(t, nil, trxn, "Transaction creation should be success")

		reversal, err := transactionRepository.Reverse(ctx, trxn.ID)
		assert.NoError(t, err)
		assert.NotEqual(t, nil, reversal, "Transaction reversal should be success")

		exists, err := transactionRepository.GetByID(ctx, "t053_REVERSAL")
		assert.Equal(t, nil, err, "Error while checking for existing transaction")
		assert.Equal(t, "t053_REVERSAL", exists.ID, "Transaction should exist")
		assert.Equal(t, ledgerV1.TransactionType_REVERSAL.String(), exists.TransactionType, "Transaction type is not setup correctly")

	})
}

func (ts *TransactionsModelSuite) TestUnClearedTransactions() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		initialAccMap, err := accountRepo.ListByID(ctx, "b1", "b2")
		assert.NoError(t, err)

		timeNow := time.Now().UTC()

		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t051"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			Entries: []*models.TransactionEntry{
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

		done, err1 := transactionRepository.Transact(ctx, transaction)
		assert.NoError(t, err1)
		assert.NotEqual(t, nil, done, "Transaction should be created")

		finalAccMap, err2 := accountRepo.ListByID(ctx, "b1", "b2")
		assert.NoError(t, err2)

		assert.Equal(t, utility.CleanDecimal(decimal.NewFromFloat(0.0)), utility.CleanDecimal(finalAccMap["b1"].Balance.Decimal.Sub(initialAccMap["b1"].Balance.Decimal)), "Debited Balance should be equal")
		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(0)), utility.CleanDecimal(finalAccMap["b2"].Balance.Decimal.Sub(initialAccMap["b2"].Balance.Decimal)), "Credited Balance should be equal")

		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(100)), utility.CleanDecimal(finalAccMap["b1"].UnClearedBalance.Decimal), "b1 Uncleared balance should be equal")
		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(-100)), utility.CleanDecimal(finalAccMap["b2"].UnClearedBalance.Decimal), "b2 Uncleared balance should be equal")

		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(0)), utility.CleanDecimal(finalAccMap["b1"].ReservedBalance.Decimal), "b1 reserved balance should be zero")
		assert.Equal(t, utility.CleanDecimal(decimal.NewFromInt(0)), utility.CleanDecimal(finalAccMap["b2"].ReservedBalance.Decimal), "b2 reserved balance should be zero")
	})
}

func (ts *TransactionsModelSuite) TestTransactWithBoundaryValues() {
	ts.WithTestDependancies(ts.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, svc)

		accountRepo := repository2.NewAccountRepository(svc)
		transactionRepository := repository2.NewTransactionRepository(svc, accountRepo)

		timeNow := time.Now().UTC()

		// In-boundary value transaction
		boundaryValue := utility.CleanDecimal(utility.MaxDecimalValue) // Max +ve for 2^64
		transaction := &models.Transaction{
			BaseModel:       frame.BaseModel{ID: "t004"},
			Currency:        "UGX",
			TransactionType: ledgerV1.TransactionType_NORMAL.String(),
			TransactedAt:    &timeNow,
			ClearedAt:       &timeNow,
			Entries: []*models.TransactionEntry{
				{
					AccountID: "a3",
					Credit:    false,
					Amount:    decimal.NewNullDecimal(boundaryValue),
				},
				{
					AccountID: "a4",
					Credit:    true,
					Amount:    decimal.NewNullDecimal(boundaryValue),
				},
			},
			Data: map[string]interface{}{
				"tag1": "val1",
				"tag2": "val2",
			},
		}
		done, _ := transactionRepository.Transact(ctx, transaction)
		assert.NotNil(t, done, "Transaction should be created")
		exists, err := transactionRepository.GetByID(ctx, "t004")
		assert.Nil(t, err, "Error while checking for existing transaction")
		assert.Equal(t, "t004", exists.ID, "Transaction should exist")

		// Out-of-boundary value transaction
		// Note: Not able write test case for out of boundary value here,
		// due to overflow error while compilation.
		// The test case is written in `package controllers` using JSON
	})
}

func TestTransactionsModelSuite(t *testing.T) {
	suite.Run(t, new(TransactionsModelSuite))
}
