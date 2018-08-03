package models

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	ledgerError "bitbucket.org/caricah/service-ledger/errors"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"strings"
	"fmt"
)

const (
	// LedgerTimestampLayout is the timestamp layout followed in Ledger
	LedgerTimestampLayout = "2006-01-02 15:04:05.000"
)

// Transaction represents a transaction in a ledger
type Transaction struct {
	ID 			int64				 `json:"id"`
	Reference   string          	 `json:"reference"`
	Data     	DataMap				 `json:"data"`
	TranactedAt string               `json:"transacted_at"`
	Entries   	[]*TransactionEntry  `json:"entries"`
}

// TransactionEntry represents a transaction line in a ledger
type TransactionEntry struct {
	ID int64
	AccountID int64
	Account string `json:"account"`
	Amount    int    `json:"amount"`
}

func (t *TransactionEntry) Equal(ot TransactionEntry) bool {
	return strings.ToLower(t.Account) == strings.ToLower(ot.Account) && t.Amount == ot.Amount
}

// IsValid validates the Amount list of a transaction
func (t *Transaction) IsValid() bool {
	sum := 0
	for _, entry := range t.Entries {
		sum += entry.Amount
	}
	return sum == 0
}

// TransactionDB is the interface to all transaction operations
type TransactionDB struct {
	db *sql.DB
}

// NewTransactionDB returns a new instance of `TransactionDB`
func NewTransactionDB(db *sql.DB) TransactionDB {
	return TransactionDB{db: db}
}

// IsExists says whether a transaction already exists or not
func (t *TransactionDB) IsExists(reference string) (bool, ledgerError.ApplicationError) {
	var exists bool
	err := t.db.QueryRow("SELECT EXISTS (SELECT transaction_id FROM transactions WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		log.Println("Error executing transaction exists query:", err)
		return false, DBError(err)
	}
	return exists, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *TransactionDB) IsConflict(transaction *Transaction) (bool, ledgerError.ApplicationError) {
	// Read existing Entries
	rows, err := t.db.Query("SELECT entries.entry_id, accounts.account_id, accounts.reference, entries.amount FROM entries LEFT JOIN accounts USING(account_id) LEFT JOIN transactions USING(transaction_id) WHERE transactions.reference=$1", transaction.Reference)
	if err != nil {
		log.Println("Error executing transaction Entries query:", err)
		return false, DBError(err)
	}
	defer rows.Close()
	var existingentries []*TransactionEntry
	for rows.Next() {
		entry := &TransactionEntry{}
		if err := rows.Scan(&entry.ID, &entry.AccountID, &entry.Account, &entry.Amount); err != nil {
			log.Println("Error scanning transaction Entries:", err)
			return false, DBError(err)
		}
		existingentries = append(existingentries, entry)
	}
	if err := rows.Err(); err != nil {
		log.Println("Error iterating transaction Entries rows:", err)
		return false, DBError(err)
	}

	// Compare new and existing transaction Entries
	return !containsSameElements(transaction.Entries, existingentries), nil
}

// Transact creates the input transaction in the DB
func (t *TransactionDB) Transact(txn *Transaction) bool {


	//// Accounts have to be predefined hence check all references exist.

	for _, entry := range txn.Entries {

		err := t.db.QueryRow("SELECT account_id FROM accounts WHERE reference = $1", entry.Account ).Scan(&entry.AccountID)
		if err != nil {
			log.Println( fmt.Sprintf("could not validate account %s exist", entry.Account))
			return false
		}

		if entry.Amount == 0{
			log.Println( fmt.Sprintf("a zero amount can not participate in a transaction for account %s exist", entry.Account))
			return false
		}
	}

	// Start the transaction
	var err error
	tx, err := t.db.Begin()
	if err != nil {
		log.Println("Error beginning transaction:", err)
		return false
	}

	// Rollback transaction on any failures
	handleTransactionError := func(tx *sql.Tx, err error) bool {
		log.Println(err)
		log.Println("Rolling back the transaction:", txn.ID)
		err = tx.Rollback()
		if err != nil {
			log.Println("Error rolling back transaction:", err)
		}
		return false
	}


	// Add transaction
	data, err := json.Marshal(txn.Data)
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "transaction data parse error"))
	}
	transactionData := "{}"
	if txn.Data != nil && data != nil {
		transactionData = string(data)
	}

	if txn.TranactedAt == "" {
		txn.TranactedAt = time.Now().UTC().Format(LedgerTimestampLayout)
	}

	var transactionID int64
	 err = tx.QueryRow("INSERT INTO transactions (reference, transacted_at, data) VALUES ($1, $2, $3)  RETURNING transaction_id", txn.Reference, txn.TranactedAt, transactionData).Scan(&transactionID)
	if err != nil {
		// Ignore duplicate transactions and return success response
		if err.(*pq.Error).Code.Name() == "unique_violation" {
			log.Println("Ignoring duplicate transaction of id:", txn.ID)
			err = tx.Rollback()
			if err != nil {
				log.Println("Error rolling back transaction:", err)
			}
			return true
		}
		return handleTransactionError(tx, errors.Wrap(err, "insert transaction failed"))
	}


	// Add transaction Entries
	for _, line := range txn.Entries {
		_, err = tx.Exec(
			"INSERT INTO entries (transaction_id, account_id, amount) VALUES ($1, $2, $3)  RETURNING entry_id",
			transactionID, line.AccountID, line.Amount)
		if err != nil {
			return handleTransactionError(tx, errors.Wrap(err, "insert Entries failed"))
		}
	}

	// Commit the entire transaction
	err = tx.Commit()
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "commit transaction failed"))
	}

	return true
}

// UpdateTransaction updates data of the given transaction
func (t *TransactionDB) UpdateTransaction(txn *Transaction) ledgerError.ApplicationError {
	data, err := json.Marshal(txn.Data)
	if err != nil {
		return JSONError(err)
	}
	tData := "{}"
	if txn.Data != nil && data != nil {
		tData = string(data)
	}

	q := "UPDATE transactions SET data = $1 WHERE transaction_id = $2"
	_, err = t.db.Exec(q, tData, txn.ID)
	if err != nil {
		return DBError(err)
	}
	return nil
}
