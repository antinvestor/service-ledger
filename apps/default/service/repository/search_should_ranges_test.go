package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldRanges() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *definition.DependancyOption) {
		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "should": {
                "ranges": [
                    {"created": {"gte": "2017-01-01", "lte": "2017-06-30"}},
                    {"created": {"gte": "2017-07-01", "lte": "2017-12-30"}}
                ]
            }
        }
    }`
		resultChannel, err := ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 2, "Accounts count doesn't match")

		query = `{
        "query": {
            "should": {
                "ranges": [
                    {"created": {"gte": "2017-07-01", "lte": "2017-12-30"}},
                    {"created": {"gte": "2018-01-01", "lte": "2018-06-30"}}
                ]
            }
        }
    }`
		resultChannel, err = ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, accounts, "No account should exist for given query")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldRanges() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *definition.DependancyOption) {
		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "should": {
                "ranges": [
                    {"expiry": {"gte": "2018-01-01", "lte": "2018-01-30"}},
                    {"expiry": {"gte": "2018-06-01", "lte": "2018-06-30"}}
                ]
            }
        }
    }`

		resultChannel, err := ss.txnDB.Search(ctx, query)
		require.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 3, "Transactions count doesn't match")

		query = `{
        "query": {
            "should": {
                "ranges": [
                    {"expiry": {"gte": "2018-06-01", "lte": "2018-06-30"}},
                    {"expiry": {"gte": "2018-07-01", "lte": "2018-07-30"}}
                ]
            }
        }
    }`

		resultChannel, err = ss.txnDB.Search(ctx, query)
		require.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, transactions, "No transaction should exist for given query")
	})
}
