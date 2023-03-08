package repositories_test

import (
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithMustTerms() {
	t := ss.T()
	ctx := ss.ctx

	query := `{
        "query": {
            "must": {
                "terms": [
                    {"customer_id": "C1"},
                    {"status": "active"}
                ]
            }
        }
    }`
	accounts, err := ss.accDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	assert.Equal(t, "acc1", accounts[0].ID, "Account Reference doesn't match")

	query = `{
        "query": {
            "must": {
                "terms": [
                    {"customer_id": "C2"},
                    {"status": "active"}
                ]
            }
        }
    }`
	accounts, err = ss.accDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(accounts), "No account should exist for given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithMustTerms() {
	t := ss.T()
	ctx := ss.ctx

	query := `{
        "query": {
            "must": {
                "terms": [
                    {"action": "setcredit"},
                    {"months": ["jan", "feb", "mar"]}
                ]
            }
        }
    }`
	transactions, err := ss.txnDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(transactions), "Transactions count doesn't match")
	if len(transactions) > 0 {
		assert.Equal(t, "txn1", transactions[0].ID, "Transaction Reference doesn't match")
	}
	query = `{
        "query": {
            "must": {
                "terms": [
                    {"action": "setcredit"},
                    {"months": ["oct", "nov", "dec"]}
                ]
            }
        }
    }`
	transactions, err = ss.txnDB.Search(ctx, query)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(transactions), "No transaction should exist for given query")
}
