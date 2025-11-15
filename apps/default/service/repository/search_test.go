package repository_test

import (
	"context"
	"testing"
	"time"

	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SearchSuite struct {
	tests.BaseTestSuite
	ledgerDB repository.LedgerRepository
	accDB    repository.AccountRepository
	txnDB    repository.TransactionRepository

	ledger *models.Ledger
}

func toSlice[T any](result workerpool.JobResultPipe[[]T]) ([]T, error) {
	var resultSlice []T

	for res := range result.ResultChan() {
		if res.IsError() {
			return nil, res.Error()
		}
		resultSlice = append(resultSlice, res.Item()...)
	}
	return resultSlice, nil
}

func (ss *SearchSuite) setupFixtures(ctx context.Context, svc *frame.Service) {
	t := ss.T()

	svc.Log(ctx).Info("Successfully established connection to database.")
	ss.accDB = repository.NewAccountRepository(svc)
	ss.txnDB = repository.NewTransactionRepository(svc, ss.accDB)
	ss.ledgerDB = repository.NewLedgerRepository(svc)

	lg, err := ss.ledgerDB.Create(ctx, &models.Ledger{Type: models.LedgerTypeAsset})
	require.NoError(t, err, "Unable to create ledger for search account")

	ss.ledger = lg
	// Create test accounts
	acc1 := &models.Account{
		BaseModel: data.BaseModel{ID: "acc1"},
		LedgerID:  ss.ledger.ID,
		Currency:  "UGX",
		Data: map[string]interface{}{
			"customer_id": "C1",
			"status":      "active",
			"created":     "2017-01-01",
		},
	}
	_, err = ss.accDB.Create(ctx, acc1)
	require.NoError(t, err, "Error creating test account with %s", err)
	acc2 := &models.Account{
		BaseModel: data.BaseModel{ID: "acc2"},
		LedgerID:  ss.ledger.ID,
		Currency:  "UGX",
		Data: map[string]interface{}{
			"customer_id": "C2",
			"status":      "inactive",
			"created":     "2017-06-30",
		},
	}
	_, err = ss.accDB.Create(ctx, acc2)
	require.NoError(t, err, "Error creating test account")

	timeNow := time.Now().UTC()
	// Create test transactions
	txn1 := &models.Transaction{
		BaseModel:       data.BaseModel{ID: "txn1"},
		Currency:        "UGX",
		TransactionType: ledgerv1.TransactionType_NORMAL.String(),
		TransactedAt:    timeNow,
		ClearedAt:       timeNow,
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
	tx, err := ss.txnDB.Transact(ctx, txn1)
	require.NoError(t, err, "Error creating test transaction")
	require.NotNil(t, tx, "Error creating test transaction")
	txn2 := &models.Transaction{
		BaseModel:       data.BaseModel{ID: "txn2"},
		Currency:        "UGX",
		TransactionType: ledgerv1.TransactionType_NORMAL.String(),
		TransactedAt:    timeNow,
		ClearedAt:       timeNow,
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
	tx, _ = ss.txnDB.Transact(ctx, txn2)
	require.NotNil(t, tx, "Error creating test transaction")
	txn3 := &models.Transaction{
		BaseModel:       data.BaseModel{ID: "txn3"},
		Currency:        "UGX",
		TransactionType: ledgerv1.TransactionType_NORMAL.String(),
		TransactedAt:    timeNow,
		ClearedAt:       timeNow,
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
	tx, err = ss.txnDB.Transact(ctx, txn3)

	require.NoError(t, err)

	require.NotNil(t, tx, "Error creating test transaction")
}

func (ss *SearchSuite) TestSearchAccountsWithBothMustAndShould() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		svc, ctx, _ := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

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

		resultChannel, err := ss.accDB.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 1, "Account count doesn't match")
		assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithBothMustAndShould() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		svc, ctx, _ := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

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
		resultChannel, err := ss.txnDB.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 1, "Transaction count doesn't match")
		if len(transactions) > 0 {
			assert.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")
		}
	})
}

func TestSearchSuite(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
