package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldTerms() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "accounts")

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
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*models.Account)
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
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*models.Account)
	assert.Equal(t, 0, len(accounts), "No account should exist for given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldTerms() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "transactions")

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
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ := results.([]*models.Transaction)
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
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ = results.([]*models.Transaction)
	assert.Equal(t, 0, len(transactions), "No transaction should exist for given query")
}
