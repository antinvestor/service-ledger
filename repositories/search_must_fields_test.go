package repositories_test

import (
	"github.com/antinvestor/service-ledger/models"
	"github.com/stretchr/testify/assert"
)

func (ss *SearchSuite) TestSearchAccountsWithMustFields() {
	t := ss.T()
	ctx := ss.ctx

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
	resultChannel, err := ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err := toSlice[*models.Account](resultChannel)

	if err != nil {
		ss.Errorf(err, "Error querying must fields")
	}
	assert.Equal(t, nil, err, "Error in building search query")
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

	resultChannel, err = ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err = toSlice[*models.Account](resultChannel)

	if err != nil {
		ss.Errorf(err, "Error querying must fields")
	}
	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 0, len(accounts), "No account should exist for given query")
}

func (ss *SearchSuite) TestSearchTransactionsWithMustFields() {
	t := ss.T()
	ctx := ss.ctx

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
                "fields": [
                    {"id": {"eq": "txn2"}},
                    {"transacted_at": {"lt": "2017-08-08"}}
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

func (ss *SearchSuite) TestSearchAccountsWithFieldOperators() {
	t := ss.T()
	ctx := ss.ctx

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
	assert.NoError(t, err)
	accounts, err := toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
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

	resultChannel, err = ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err = toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
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

	resultChannel, err = ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err = toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
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

	resultChannel, err = ss.accDB.Search(ctx, query)
	assert.NoError(t, err)
	accounts, err = toSlice[*models.Account](resultChannel)

	assert.Equal(t, nil, err, "Error in building search query")
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	assert.Equal(t, "acc2", accounts[0].ID, "Account Reference doesn't match")
}
