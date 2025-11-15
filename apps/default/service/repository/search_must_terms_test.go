package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (ss *SearchSuite) TestSearchAccountsWithMustTerms() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		svc, ctx, _ := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"id": "acc1"}
                    }
                ]
            }
        }
    }`
		resultChannel, err := ss.accDB.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, accounts, 1, "Accounts count doesn't match")
		assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")

		query = `{
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"id": "nonexistent"}
                    }
                ]
            }
        }
    }`
		resultChannel, err = ss.accDB.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, accounts, "No account should exist for given query")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithMustTerms() {
	ss.WithTestDependencies(ss.T(), func(t *testing.T, dep *definition.DependencyOption) {
		svc, ctx, _ := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"id": "txn1"}
                    }
                ]
            }
        }
    }`
		resultChannel, err := ss.txnDB.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Len(t, transactions, 1, "Transactions count doesn't match")
		assert.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")

		query = `{
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"id": "nonexistent"}
                    }
                ]
            }
        }
    }`
		resultChannel, err = ss.txnDB.SearchAsESQ(ctx, query)
		require.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)

		require.NoError(t, err, "Error in building search query")
		assert.Empty(t, transactions, "No transaction should exist for given query")
	})
}
