package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithMustRanges() {
	t := ss.T()
	ctx := ss.ctx

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
	resultChannel, err := ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err := toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
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
	resultChannel, err = ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err = toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(accounts), "No account should exist for given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithMustRanges() {
	t := ss.T()
	ctx := ss.ctx

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
	resultChannel, err := ss.txnDB.Search(ctx, query)
	assert.NoError(t, err)
	transactions, err := toSlice[*models.Transaction](resultChannel)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(transactions), "Transactions count doesn't match")
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

	resultChannel, err = ss.txnDB.Search(ctx, query)
	assert.NoError(t, err)
	transactions, err = toSlice[*models.Transaction](resultChannel)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(transactions), "No transaction should exist for given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithIsOperator() {
	t := ss.T()
	ctx := ss.ctx

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

	resultChannel, err := ss.txnDB.Search(ctx, query)
	assert.NoError(t, err)
	transactions, err := toSlice[*models.Transaction](resultChannel)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 3, len(transactions), "Transactions count doesn't match")

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

	resultChannel, err = ss.txnDB.Search(ctx, query)
	assert.NoError(t, err)
	transactions, err = toSlice[*models.Transaction](resultChannel)
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 3, len(transactions), "Transactions count doesn't match")
}

func (ss *SearchSuite) TestSearchAccountsWithInOperator() {
	t := ss.T()
	ctx := ss.ctx

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
	resultChannel, err := ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err := toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 2, len(accounts), "Accounts count doesn't match")

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
	resultChannel, err = ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err = toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
}
