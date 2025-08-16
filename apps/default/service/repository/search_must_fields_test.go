package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/require"
)

func (ss *SearchSuite) TestSearchAccountsWithMustFields() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *definition.DependancyOption) {
		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "acc1"}},
                    {"balance": {"gt": 0}}
                ]
            }
        }
    }`
		jobResult, err := ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](jobResult)

		require.NoError(t, err, "Error querying must fields")
		require.Len(t, accounts, 1, "Accounts count doesn't match")
		if len(accounts) > 0 {
			require.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
		}
		query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "acc2"}},
                    {"balance": {"gt": 0}}
                ]
            }
        }
    }`

		jobResult, err = ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](jobResult)

		require.NoError(t, err, "Error querying must fields")
		require.Empty(t, accounts, "No account should exist for given query")
	})
}

func (ss *SearchSuite) TestSearchTransactionsWithMustFields() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *definition.DependancyOption) {
		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "txn1"}},
                    {"transacted_at": {"gte": "2017-08-08"}}
                ]
            }
        }
    }`

		resultChannel, err := ss.txnDB.Search(ctx, query)
		require.NoError(t, err)
		transactions, err := toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		require.Len(t, transactions, 1, "Transactions count doesn't match")
		if len(transactions) > 0 {
			require.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")
		}
		query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "txn2"}},
                    {"transacted_at": {"lt": "2017-08-08"}}
                ]
            }
        }
    }`

		resultChannel, err = ss.txnDB.Search(ctx, query)
		require.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)
		require.NoError(t, err, "Error in building search query")
		require.Empty(t, transactions, "No transaction should exist for given query")
	})
}

func (ss *SearchSuite) TestSearchAccountsWithFieldOperators() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *definition.DependancyOption) {
		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "acc1"}}
                ]
            }
        }
    }`

		resultChannel, err := ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err := toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		require.Len(t, accounts, 1, "Accounts count doesn't match")
		if len(accounts) > 0 {
			require.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
		}

		query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"ne": "acc1"}}
                ]
            }
        }
    }`

		resultChannel, err = ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		require.Len(t, accounts, 1, "Accounts count doesn't match")
		require.Equal(t, "acc2", accounts[0].ID, "Account Reference doesn't match")

		query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"like": "%c1"}}
                ]
            }
        }
    }`

		resultChannel, err = ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		require.Len(t, accounts, 1, "Accounts count doesn't match")
		require.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")

		query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"notlike": "%c1"}}
                ]
            }
        }
    }`

		resultChannel, err = ss.accDB.Search(ctx, query)
		require.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		require.NoError(t, err, "Error in building search query")
		require.Len(t, accounts, 1, "Accounts count doesn't match")
		require.Equal(t, "acc2", accounts[0].ID, "Account Reference doesn't match")
	})
}
