package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldFields() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

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
		resultChannel, err := resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err := resultsToSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 2, "Accounts count doesn't match")

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
		resultChannel, err = resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err = resultsToSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, accounts, "No account should exist for the given query")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldFields() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

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

		resultChannel, err := resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err := resultsToSlice[*models.Transaction](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 3, "Transactions count doesn't match")

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

		resultChannel, err = resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err = resultsToSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, transactions, "No transaction should exist for the given query")
	})
}
