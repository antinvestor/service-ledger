package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithMustFields() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "accounts")

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
	results, err := engine.Query(ss.ctx, query)
	if err != nil {
		ss.Errorf(err, "Error querying must fields")
	}
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*models.Account)
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	if len(accounts) > 0 {
		assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
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
	results, err = engine.Query(ss.ctx, query)
	if err != nil {
		ss.Errorf(err, "Error querying must fields")
	}
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*models.Account)
	assert.Equal(t, 0, len(accounts), "No account should exist for given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithMustFields() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "transactions")

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
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ := results.([]*models.Transaction)
	assert.Equal(t, 1, len(transactions), "Transactions count doesn't match")
	if len(transactions) > 0 {
		assert.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")
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
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ = results.([]*models.Transaction)
	assert.Equal(t, 0, len(transactions), "No transaction should exist for given query")
}

func (ss *SearchSuite) TestSearchAccountsWithFieldOperators() {
	t := ss.T()
	engine, _ := repositories.NewSearchEngine(ss.service, "accounts")

	query := `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"eq": "acc1"}}
                ]
            }
        }
    }`
	results, err := engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*models.Account)
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	if len(accounts) > 0 {
		assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")
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
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*models.Account)
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	assert.Equal(t, "acc2", accounts[0].ID, "Account Reference doesn't match")

	query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"like": "%c1"}}
                ]
            }
        }
    }`
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*models.Account)
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")

	query = `{
        "query": {
            "must": {
                "fields": [
                    {"id": {"notlike": "%c1"}}
                ]
            }
        }
    }`
	results, err = engine.Query(ss.ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*models.Account)
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	assert.Equal(t, "acc2", accounts[0].ID, "Account Reference doesn't match")
}
