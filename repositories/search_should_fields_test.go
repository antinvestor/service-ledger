package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldFields() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "accounts")

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
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*models.Account)
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
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*models.Account)
	assert.Equal(t, 0, len(accounts), "No account should exist for the given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldFields() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "transactions")

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
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ := results.([]*models.Transaction)
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
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ = results.([]*models.Transaction)
	assert.Equal(t, 0, len(transactions), "No transaction should exist for the given query")
}
