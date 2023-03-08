package repositories_test

import (
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithShouldFields() {
	t := ss.T()
	ctx := ss.ctx

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
	accounts, err := ss.accDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
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
	accounts, err = ss.accDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(accounts), "No account should exist for the given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithShouldFields() {
	t := ss.T()
	ctx := ss.ctx

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
	transactions, err := ss.txnDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
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
	transactions, err = ss.txnDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(transactions), "No transaction should exist for the given query")
}
