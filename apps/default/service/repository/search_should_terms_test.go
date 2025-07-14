package repository_test

import (
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/tests/testdef"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldTerms() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "should": {
                "terms": [
                    {"status": "active"},
                    {"status": "inactive"}
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
                "terms": [
                    {"status": "pending"},
                    {"status": "removed"}
                ]
            }
        }
    }`
		resultChannel, err = ss.accDB.Search(ctx, query)
		assert.NoError(t, err)
		accounts, err = toSlice[*models.Account](resultChannel)

		assert.Equal(t, nil, err, "Error in building search query")
		assert.Equal(t, 0, len(accounts), "No account should exist for given query")

	})
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldTerms() {
	ss.WithTestDependancies(ss.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		svc, ctx := ss.CreateService(t, dep)
		ss.setupFixtures(ctx, svc)

		query := `{
        "query": {
            "should": {
                "terms": [
                    {"action": "setcredit"},
                    {"action": "refundpayment"}
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
                "terms": [
                    {"action": "cancelorder"},
                    {"action": "refundpayment"}
                ]
            }
        }
    }`

		resultChannel, err = ss.txnDB.Search(ctx, query)
		assert.NoError(t, err)
		transactions, err = toSlice[*models.Transaction](resultChannel)
		assert.Equal(t, nil, err, "Error in building search query")
		assert.Equal(t, 0, len(transactions), "No transaction should exist for given query")

	})
}
