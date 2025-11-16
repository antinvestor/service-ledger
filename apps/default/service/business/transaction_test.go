package business_test

import (
	"context"
	"testing"
	"time"

	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/type/money"
	"google.golang.org/protobuf/types/known/structpb"
)

type TransactionBusinessSuite struct {
	tests.BaseTestSuite
	assetLedger  *models.Ledger
	incomeLedger *models.Ledger
}

func TestTransactionBusinessSuite(t *testing.T) {
	suite.Run(t, new(TransactionBusinessSuite))
}

func (ts *TransactionBusinessSuite) setupFixtures(ctx context.Context, resources *tests.ServiceResources) {
	// Create test ledgers using business layer
	ledgerBusiness := resources.LedgerBusiness
	accountBusiness := resources.AccountBusiness

	// Create asset ledger
	assetLedgerReq := &ledgerv1.CreateLedgerRequest{
		Id:   "test-asset-ledger",
		Type: ledgerv1.LedgerType_ASSET,
	}
	assetLedger, err := ledgerBusiness.CreateLedger(ctx, assetLedgerReq)
	ts.Require().NoError(err, "Unable to create asset ledger")
	ts.assetLedger = &models.Ledger{
		BaseModel: data.BaseModel{ID: assetLedger.GetId()},
		Type:      models.FromLedgerType(assetLedger.GetType()),
	}
	ts.T().Logf("Created asset ledger with ID: %s", assetLedger.GetId())

	// Create income ledger
	incomeLedgerReq := &ledgerv1.CreateLedgerRequest{
		Id:   "test-income-ledger",
		Type: ledgerv1.LedgerType_INCOME,
	}
	incomeLedger, err := ledgerBusiness.CreateLedger(ctx, incomeLedgerReq)
	ts.Require().NoError(err, "Unable to create income ledger")
	ts.incomeLedger = &models.Ledger{
		BaseModel: data.BaseModel{ID: incomeLedger.GetId()},
		Type:      models.FromLedgerType(incomeLedger.GetType()),
	}
	ts.T().Logf("Created income ledger with ID: %s", incomeLedger.GetId())

	// Create test accounts
	assetAccountReq := &ledgerv1.CreateAccountRequest{
		Id:       "asset-account",
		LedgerId:  assetLedger.GetId(),
		Currency: "USD",
	}
	assetAccount, err := accountBusiness.CreateAccount(ctx, assetAccountReq)
	ts.Require().NoError(err, "Unable to create asset account")
	ts.T().Logf("Created asset account with ID: %s, Ledger: %s", assetAccount.GetId(), assetAccount.GetLedger())

	incomeAccountReq := &ledgerv1.CreateAccountRequest{
		Id:       "income-account",
		LedgerId:  incomeLedger.GetId(),
		Currency: "USD",
	}
	incomeAccount, err := accountBusiness.CreateAccount(ctx, incomeAccountReq)
	ts.Require().NoError(err, "Unable to create income account")
	ts.T().Logf("Created income account with ID: %s, Ledger: %s", incomeAccount.GetId(), incomeAccount.GetLedger())

	// Verify accounts exist before creating transaction
	_, err = accountBusiness.GetAccount(ctx, "asset-account")
	ts.Require().NoError(err, "Asset account should exist before transaction creation")
	_, err = accountBusiness.GetAccount(ctx, "income-account")
	ts.Require().NoError(err, "Income account should exist before transaction creation")
}

func (ts *TransactionBusinessSuite) TestCreateTransactionWithBusinessValidation() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		timeNow := time.Now().UTC()
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "test-transaction-" + timeNow.Format("20060102150405"),
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
			},
			TransactedAt: timeNow.Format(time.RFC3339),
			Cleared:      true,
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		require.NoError(t, err, "Error creating transaction through business layer")
		require.NotNil(t, transaction, "Transaction should be created")

		assert.Equal(t, "test-transaction-"+timeNow.Format("20060102150405"), transaction.GetId(), "Invalid transaction ID")
		assert.Equal(t, "USD", transaction.GetCurrencyCode(), "Invalid currency")
		assert.Equal(t, ledgerv1.TransactionType_NORMAL, transaction.GetType(), "Invalid transaction type")
		assert.Len(t, transaction.GetEntries(), 2, "Should have 2 entries")
	})
}

func (ts *TransactionBusinessSuite) TestCreateTransactionNonZeroSum() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "invalid-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 200, Nanos: 0}, // Non-zero sum
				},
			},
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		assert.Error(t, err, "Should fail with non-zero sum transaction")
		assert.Nil(t, transaction, "Transaction should not be created")
		assert.Contains(t, err.Error(), "non-zero sum", "Error should mention zero sum validation")
	})
}

func (ts *TransactionBusinessSuite) TestCreateTransactionInvalidDebitCredit() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		// Create transaction with invalid debit/credit combination (both debit but equal amounts)
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "invalid-dr-cr-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false, // Debit
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    false, // Also debit - invalid but amounts equal for zero sum
					Amount:    &money.Money{CurrencyCode: "USD", Units: -100, Nanos: 0}, // Negative amount to make zero sum
				},
			},
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		assert.Error(t, err, "Should fail with invalid debit/credit entry")
		assert.Nil(t, transaction, "Transaction should not be created")
		assert.Contains(t, err.Error(), "invalid debit/credit", "Error should mention debit/credit validation")
	})
}

func (ts *TransactionBusinessSuite) TestCreateTransactionWithNonExistentAccount() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "orphan-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "non-existent-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
			},
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		assert.Error(t, err, "Should fail with non-existent account")
		assert.Nil(t, transaction, "Transaction should not be created")
		assert.Contains(t, err.Error(), "not found", "Error should mention account not found")
	})
}

func (ts *TransactionBusinessSuite) TestCreateTransactionWithCurrencyMismatch() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		// Create transaction with different currency than accounts
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "currency-mismatch-transaction",
			Currency: "EUR", // Different from account currency (USD)
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "EUR", Units: 100, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "EUR", Units: 100, Nanos: 0},
				},
			},
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		assert.Error(t, err, "Should fail with currency mismatch")
		assert.Nil(t, transaction, "Transaction should not be created")
		assert.Contains(t, err.Error(), "currency", "Error should mention currency mismatch")
	})
}

func (ts *TransactionBusinessSuite) TestCreateReservationTransaction() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "reservation-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_RESERVATION,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "reservation-entry",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 500, Nanos: 0},
				},
			},
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		require.NoError(t, err, "Error creating reservation transaction")
		require.NotNil(t, transaction, "Reservation transaction should be created")

		assert.Equal(t, ledgerv1.TransactionType_RESERVATION, transaction.GetType(), "Should be reservation type")
		assert.Len(t, transaction.GetEntries(), 1, "Reservation should have exactly 1 entry")
	})
}

func (ts *TransactionBusinessSuite) TestCreateReservationTransactionInvalidEntries() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		// Create reservation transaction with multiple entries (invalid)
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "invalid-reservation-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_RESERVATION,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 100, Nanos: 0},
				},
			},
		}

		transaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		assert.Error(t, err, "Should fail with invalid reservation transaction")
		assert.Nil(t, transaction, "Reservation transaction should not be created")
		assert.Contains(t, err.Error(), "invalid debit/credit", "Error should mention entry validation")
	})
}

func (ts *TransactionBusinessSuite) TestReverseTransaction() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		// First create a normal transaction
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "original-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 1000, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 1000, Nanos: 0},
				},
			},
		}

		originalTransaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		require.NoError(t, err, "Error creating original transaction")

		// Now reverse it
		reverseReq := &ledgerv1.ReverseTransactionRequest{
			Id: originalTransaction.GetId(),
		}

		reversedTransaction, err := transactionBusiness.ReverseTransaction(ctx, reverseReq)
		require.NoError(t, err, "Error reversing transaction")
		require.NotNil(t, reversedTransaction, "Reversed transaction should be created")

		assert.Equal(t, ledgerv1.TransactionType_REVERSAL, reversedTransaction.GetType(), "Should be reversal type")
		assert.Contains(t, reversedTransaction.GetId(), "_REVERSAL", "Reversal transaction ID should contain _REVERSAL")
		assert.Len(t, reversedTransaction.GetEntries(), 2, "Reversal should have same number of entries")
	})
}

func (ts *TransactionBusinessSuite) TestGetTransaction() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		// Create a transaction first
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "get-test-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 250, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 250, Nanos: 0},
				},
			},
		}

		createdTransaction, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		require.NoError(t, err, "Error creating transaction")

		// Now retrieve it
		retrievedTransaction, err := transactionBusiness.GetTransaction(ctx, "get-test-transaction")
		require.NoError(t, err, "Error retrieving transaction")

		assert.Equal(t, createdTransaction.GetId(), retrievedTransaction.GetId(), "Retrieved transaction should match created transaction")
		assert.Equal(t, createdTransaction.GetCurrencyCode(), retrievedTransaction.GetCurrencyCode(), "Currency should match")
		assert.Equal(t, createdTransaction.GetType(), retrievedTransaction.GetType(), "Type should match")
	})
}

func (ts *TransactionBusinessSuite) TestUpdateTransaction() {
	ts.WithTestDependencies(ts.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ts.CreateService(t, dep)
		ts.setupFixtures(ctx, resources)

		transactionBusiness := resources.TransactionBusiness

		// Create a transaction first
		createTransactionReq := &ledgerv1.CreateTransactionRequest{
			Id:       "update-test-transaction",
			Currency: "USD",
			Type:     ledgerv1.TransactionType_NORMAL,
			Entries: []*ledgerv1.TransactionEntry{
				{
					Id:        "entry1",
					AccountId: "asset-account",
					Credit:    false,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 300, Nanos: 0},
				},
				{
					Id:        "entry2",
					AccountId: "income-account",
					Credit:    true,
					Amount:    &money.Money{CurrencyCode: "USD", Units: 300, Nanos: 0},
				},
			},
		}

		_, err := transactionBusiness.CreateTransaction(ctx, createTransactionReq)
		require.NoError(t, err, "Error creating transaction")

		// Update the transaction data
		updateTransactionReq := &ledgerv1.UpdateTransactionRequest{
			Id: "update-test-transaction",
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"reference": {Kind: &structpb.Value_StringValue{StringValue: "Updated reference"}},
					"category":  {Kind: &structpb.Value_StringValue{StringValue: "Payment"}},
				},
			},
		}

		updatedTransaction, err := transactionBusiness.UpdateTransaction(ctx, updateTransactionReq)
		require.NoError(t, err, "Error updating transaction")
		require.NotNil(t, updatedTransaction, "Updated transaction should not be nil")

		// Verify the update
		assert.Equal(t, "Updated reference", updatedTransaction.GetData().GetFields()["reference"].GetStringValue())
		assert.Equal(t, "Payment", updatedTransaction.GetData().GetFields()["category"].GetStringValue())
	})
}
