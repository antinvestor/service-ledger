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
	AccountSearchAPI = "/v1/accounts"
)

type AccountsSearchSuite struct {
	suite.Suite
	context *ledgerContext.AppContext
	ledgerId int64
}

func (as *AccountsSearchSuite) SetupTest() {
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
}

func (as *AccountsSearchSuite) TestAccountsSearch() {
	t := as.T()

	// Prepare search query
	payload := `{
        "query": {
            "must": {
                "fields": [
                    {"reference": {"eq": "acc1"}}
                ],
                "terms": [
                    {"status": "active"}
                ]
            },
            "should": {
                "terms": [
                    {"customer_id": "C1"}
                ],
                "ranges": [
                    {"created": {"gte": "2018-01-01", "lte": "2018-01-30"}}
                ]
            }
        }
    }`
	handler := middlewares.ContextMiddleware(GetAccounts, as.context)
	req, err := http.NewRequest("GET", AccountSearchAPI, bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "Invalid response code")

	var accounts []models.AccountResult
	err = json.Unmarshal(rr.Body.Bytes(), &accounts)
	if err != nil {
		t.Errorf("Invalid json response: %v", rr.Body.String())
	}
	assert.Equal(t, 1, len(accounts), "Accounts count doesn't match")
	if len(accounts) == 1 {
		assert.Equal(t, "acc1", accounts[0].Reference, "Account Reference doesn't match")
	}
}

func (as *AccountsSearchSuite) TearDownSuite() {
	log.Println("Cleaning up the test database")

	t := as.T()
	_, err := as.context.DB.Exec(`DELETE FROM accounts CASCADE `)
	if err != nil {
		t.Fatal("Error deleting accounts:", err)
	}
	_, err = as.context.DB.Exec(`DELETE FROM ledgers CASCADE`)
	if err != nil {
		t.Fatal("Error deleting ledgers:", err)
	}
}


func TestAccountsSuite(t *testing.T) {
	suite.Run(t, new(AccountsSearchSuite))
}
