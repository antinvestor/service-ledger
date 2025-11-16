package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (ss *SearchSuite) TestSearchAccountsWithMustRanges() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

		query := `{
        "query": {
            "must": {
                "ranges": [
                    {"created": {"gte": "2017-01-01"}},
                    {"created": {"lte": "2017-02-01"}}
                ]
            }
        }
    }`
		resultChannel, err := resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 1, "Accounts count doesn't match")
		assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")

		query = `{
        "query": {
            "must": {
                "ranges": [
                    {"created": {"gte": "2017-07-01"}},
                    {"created": {"lte": "2017-12-30"}}
                ]
            }
        }
    }`
		resultChannel, err = resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, accounts, "No account should exist for given query")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithMustRanges() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

		query := `{
        "query": {
            "must": {
                "ranges": [
                    {"expiry": {"gte": "2018-01-01"}},
                    {"expiry": {"lte": "2018-01-02"}}
                ]
            }
        }
    }`
		resultChannel, err := resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 1, "Transactions count doesn't match")
		if len(transactions) > 0 {
			assert.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")
		}
		query = `{
        "query": {
            "must": {
                "ranges": [
                    {"expiry": {"gte": "2018-02-01"}},
                    {"expiry": {"lte": "2018-02-05"}}
                ]
            }
        }
    }`

		resultChannel, err = resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, transactions, "No transaction should exist for given query")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithIsOperator() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)
		// Test IS operator
		query := `{
		"query": {
			"must": {
				"ranges": [
					{"type": {"is": null}}
				]
			}
		}
	}`

		resultChannel, err := resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 3, "Transactions count doesn't match")

		// Test IS NOT operator
		query = `{
		"query": {
			"must": {
				"ranges": [
					{"action": {"isnot": null}}
				]
			}
		}
	}`

		resultChannel, err = resources.TransactionRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 3, "Transactions count doesn't match")
	})
}

func (ss *SearchSuite) TestSearchAccountsWithInOperator() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, resources)

		// Test IS operator
		query := `{
		"query": {
			"must": {
				"ranges": [
					{"customer_id": {"in": ["C1", "C2", "C3"]}}
				]
			}
		}
	}`
		resultChannel, err := resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 2, "Accounts count doesn't match")

		// Test IS NOT operator
		query = `{
		"query": {
			"must": {
				"ranges": [
					{"customer_id": {"in": ["C2", "C3", "C4"]}}
				]
			}
		}
	}`
		resultChannel, err = resources.AccountRepository.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 1, "Accounts count doesn't match")
	})
}
