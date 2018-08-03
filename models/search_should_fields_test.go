package models

import "github.com/stretchr/testify/assert"

func (ss *SearchSuite) TestSearchAccountsWithShouldFields() {
	t := ss.T()
	engine, _ := NewSearchEngine(ss.db, "accounts")

	query := `{
        "query": {
            "should": {
                "fields": [
                    {"reference": {"eq": "acc1"}},
                    {"reference": {"eq": "acc2"}}
                ]
            }
        }
    }`
	results, err := engine.Query(query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ := results.([]*AccountResult)
	assert.Equal(t, 2, len(accounts), "Accounts count doesn't match")

	query = `{
        "query": {
            "should": {
                "fields": [
                    {"reference": {"eq": "acc3"}},
                    {"reference": {"eq": "acc4"}}
                ]
            }
        }
    }`
	results, err = engine.Query(query)
	assert.Equal(t, nil, err, "Error in building search query")
	accounts, _ = results.([]*AccountResult)
	assert.Equal(t, 0, len(accounts), "No account should exist for the given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldFields() {
	t := ss.T()
	engine, _ := NewSearchEngine(ss.db, "transactions")

	query := `{
        "query": {
            "should": {
                "fields": [
                    {"reference": {"eq": "txn1"}},
                    {"reference": {"eq": "txn2"}},
                    {"reference": {"eq": "txn3"}}
                ]
            }
        }
    }`
	results, err := engine.Query(query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ := results.([]*TransactionResult)
	assert.Equal(t, 3, len(transactions), "Transactions count doesn't match")

	query = `{
        "query": {
            "should": {
                "fields": [
                    {"reference": {"eq": "txn4"}},
                    {"reference": {"eq": "txn5"}}
                ]
            }
        }
    }`
	results, err = engine.Query(query)
	assert.Equal(t, nil, err, "Error in building search query")
	transactions, _ = results.([]*TransactionResult)
	assert.Equal(t, 0, len(transactions), "No transaction should exist for the given query")
}
