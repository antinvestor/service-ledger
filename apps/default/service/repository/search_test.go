package repository_test

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
	"github.com/pitabwire/frame/workerpool"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/structpb"
)

type SearchSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func resultsToSlice[T any](result workerpool.JobResultPipe[[]T]) ([]T, error) {
	var resultSlice []T

	for res := range result.ResultChan() {
		if res.IsError() {
			return nil, res.Error()
		}
		resultSlice = append(resultSlice, res.Item()...)
	}
	return resultSlice, nil
}

func (ss *SearchSuite) setupFixtures(ctx context.Context, resources *tests.ServiceResources) {
	t := ss.T()

	t.Log("Successfully established connection to database.")

	// Create ledger using business layer
	createLedgerReq := &ledgerv1.CreateLedgerRequest{
		Id:   "test-ledger",
		Type: ledgerv1.LedgerType_ASSET,
		Data: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"name": {Kind: &structpb.Value_StringValue{StringValue: "Test Ledger"}},
			},
		},
	}

	ledger, err := resources.LedgerBusiness.CreateLedger(ctx, createLedgerReq)
	require.NoError(t, err, "Unable to create ledger for search account")

	ss.ledger = &models.Ledger{
		BaseModel: data.BaseModel{ID: ledger.GetId()},
		Type:      ledger.GetType().String(),
	}

	// Create test accounts using business layer
	createAcc1Req := &ledgerv1.CreateAccountRequest{
		Id:       "acc1",
		LedgerId: ledger.GetId(),
		Currency: "UGX",
		Data: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"customer_id": {Kind: &structpb.Value_StringValue{StringValue: "C1"}},
				"status":      {Kind: &structpb.Value_StringValue{StringValue: "active"}},
				"created":     {Kind: &structpb.Value_StringValue{StringValue: "2017-01-01"}},
			},
		},
	}

	_, err = resources.AccountBusiness.CreateAccount(ctx, createAcc1Req)
	require.NoError(t, err, "Error creating test account acc1")

	createAcc2Req := &ledgerv1.CreateAccountRequest{
		Id:       "acc2",
		LedgerId: ledger.GetId(),
		Currency: "UGX",
		Data: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"customer_id": {Kind: &structpb.Value_StringValue{StringValue: "C2"}},
				"status":      {Kind: &structpb.Value_StringValue{StringValue: "inactive"}},
				"created":     {Kind: &structpb.Value_StringValue{StringValue: "2017-06-30"}},
			},
		},
	}

	_, err = resources.AccountBusiness.CreateAccount(ctx, createAcc2Req)
	require.NoError(t, err, "Error creating test account acc2")

	timeNow := time.Now().UTC()
	// Create test transactions using business layer
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
	tx, err := resources.TransactionBusiness.Transact(ctx, txn1)
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
	tx, _ = resources.TransactionBusiness.Transact(ctx, txn2)
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
	tx, err = resources.TransactionBusiness.Transact(ctx, txn3)
	require.NoError(t, err)
	require.NotNil(t, tx, "Error creating test transaction")
}

func (ss *SearchSuite) TestSearchAccountsWithBothMustAndShould() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

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

		resultChannel, err := resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err := resultsToSlice[*models.Account](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 1, "Account count doesn't match")
		assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithBothMustAndShould() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

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
		resultChannel, err := resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err := resultsToSlice[*models.Transaction](resultChannel)

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
