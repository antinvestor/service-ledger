package controllers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	ledgerContext "bitbucket.org/caricah/service-ledger/context"
	"bitbucket.org/caricah/service-ledger/middlewares"
	"bitbucket.org/caricah/service-ledger/models"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var (
	TransactionsSearchAPI = "/v1/transactions"
)

type TransactionSearchSuite struct {
	suite.Suite
	context *ledgerContext.AppContext
	ledgerId int64
}

func (as *TransactionSearchSuite) SetupTest() {
	t := as.T()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	assert.NotEmpty(t, databaseURL)
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Panic("Unable to connect to Database:", err)
	}
	log.Println("Successfully established connection to database.")
	as.context = &ledgerContext.AppContext{DB: db}


	//Create test ledger.
	ledgersDB := models.NewLedgerDB(db)
	as.ledgerId, err = ledgersDB.CreateLedger(&models.Ledger{Type: "ASSET",})
	assert.Equal(t, nil, err, "Error creating test ledger")

	// Create test accounts
	accDB := models.NewAccountDB(db)
	acc1 := &models.Account{
		Reference: "acc1",
		LedgerID: as.ledgerId,
		Data: map[string]interface{}{
			"customer_id": "C1",
			"status":      "active",
			"created":     "2017-01-01",
		},
	}
	err = accDB.CreateAccount(acc1)
	assert.Equal(t, nil, err,"Error creating test account with ledger "+string(as.ledgerId))
	acc2 := &models.Account{
		Reference: "acc2",
		LedgerID: as.ledgerId,
		Data: map[string]interface{}{
			"customer_id": "C2",
			"status":      "inactive",
			"created":     "2017-06-30",
		},
	}
	err = accDB.CreateAccount(acc2)
	assert.Equal(t, nil, err, "Error creating test account")


	// Create test transactions
	txnDB := models.NewTransactionDB(db)
	txn1 := &models.Transaction{
		Reference: "txn1",
		Entries: []*models.TransactionEntry{
			{
				Account: "acc1",
				Amount:    1000,
			},
			{
				Account: "acc2",
				Amount:    -1000,
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-01",
			"months": []string{"jan", "feb", "mar"},
		},
	}
	ok := txnDB.Transact(txn1)
	assert.Equal(t, true, ok, "Error creating test transaction")
	txn2 := &models.Transaction{
		Reference: "txn2",
		Entries: []*models.TransactionEntry{
			{
				Account: "acc1",
				Amount:    100,
			},
			{
				Account: "acc2",
				Amount:    -100,
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-15",
			"months": []string{"apr", "may", "jun"},
		},
	}
	ok = txnDB.Transact(txn2)
	assert.Equal(t, true, ok, "Error creating test transaction")
	txn3 := &models.Transaction{
		Reference: "txn3",
		Entries: []*models.TransactionEntry{
			{
				Account: "acc1",
				Amount:    400,
			},
			{
				Account: "acc2",
				Amount:    -400,
			},
		},
		Data: map[string]interface{}{
			"action": "setcredit",
			"expiry": "2018-01-30",
			"months": []string{"jul", "aug", "sep"},
		},
	}
	ok = txnDB.Transact(txn3)
	assert.Equal(t, true, ok, "Error creating test transaction")
}

func (as *TransactionSearchSuite) TestTransactionsSearch() {
	t := as.T()

	// Prepare search query
	payload := `{
        "query": {
            "must": {
                "fields": [
                    {"reference": {"eq": "txn1"}}
                ],
                "terms": [
                    {"action": "setcredit"}
                ]
            },
            "should": {
                "terms": [
                    {"months": ["jan", "feb", "mar"]},
                    {"months": ["apr", "may", "jun"]},
                    {"months": ["jul", "aug", "sep"]}
                ],
                "ranges": [
                    {"expiry": {"gte": "2018-01-01", "lte": "2018-01-30"}}
                ]
            }
        }
    }`
	handler := middlewares.ContextMiddleware(GetTransactions, as.context)
	req, err := http.NewRequest("GET", TransactionsSearchAPI, bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "Invalid response code")

	var transactions []models.TransactionResult
	err = json.Unmarshal(rr.Body.Bytes(), &transactions)
	if err != nil {
		t.Errorf("Invalid json response: %v", rr.Body.String())
	}
	assert.Equal(t, 1, len(transactions), "Transactions count doesn't match")
	assert.Equal(t, "txn1", transactions[0].Reference, "Transaction Reference doesn't match")
}

func (as *TransactionSearchSuite) TearDownSuite() {
	log.Println("Cleaning up the test search transactions database")

	t := as.T()
	_, err := as.context.DB.Exec(`DELETE FROM entries`)
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = as.context.DB.Exec(`DELETE FROM transactions WHERE reference IN ($1, $2, $3)`, "txn1", "txn2", "txn3")
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = as.context.DB.Exec(`DELETE FROM accounts WHERE reference IN($1, $2)`, "acc1", "acc2")
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = as.context.DB.Exec(`DELETE FROM ledgers WHERE ledger_id = $1`, as.ledgerId)
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}



func TestTransactionSearchSuite(t *testing.T) {
	suite.Run(t, new(TransactionSearchSuite))
}
