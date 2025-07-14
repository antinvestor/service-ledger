package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/tests/testdef"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldFields() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "should": {
                "fields": [
                    {"id": {"eq": "acc1"}},
                    {"id": {"eq": "acc2"}}
                ]
            }
        }
    }`
		resultChannel, err := ss.accDB.Search(ctx, query)
		assert.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)

		assert.Equal(t, nil, err, "Error in building search query")
		assert.Equal(t, 2, len(accounts), "Accounts count doesn't match")

		query = `{
        "query": {
            "should": {
                "fields": [
                    {"id": {"eq": "acc3"}},
                    {"id": {"eq": "acc4"}}
                ]
            }
        }
    }`
		resultChannel, err = ss.accDB.Search(ctx, query)
		assert.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		assert.Equal(t, nil, err, "Error in building search query")
		assert.Equal(t, 0, len(accounts), "No account should exist for the given query")

	})
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldFields() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "should": {
                "fields": [
                    {"id": {"eq": "txn1"}},
                    {"id": {"eq": "txn2"}},
                    {"id": {"eq": "txn3"}}
                ]
            }
        }
    }`

		resultChannel, err := ss.txnDB.Search(ctx, query)
		assert.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)

		assert.Equal(t, nil, err, "Error in building search query")
		assert.Equal(t, 3, len(transactions), "Transactions count doesn't match")

		query = `{
        "query": {
            "should": {
                "fields": [
                    {"id": {"eq": "txn4"}},
                    {"id": {"eq": "txn5"}}
                ]
            }
        }
    }`

		resultChannel, err = ss.txnDB.Search(ctx, query)
		assert.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)
		assert.Equal(t, nil, err, "Error in building search query")
		assert.Equal(t, 0, len(transactions), "No transaction should exist for the given query")

	})
}
