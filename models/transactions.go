package models

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	ledgerError "bitbucket.org/caricah/ledger/errors"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	// LedgerTimestampLayout is the timestamp layout followed in Ledger
	LedgerTimestampLayout = "2006-01-02 15:04:05.000"
)

// Transaction represents a transaction in a ledger
type Transaction struct {
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	Timestamp string                 `json:"timestamp"`
	entries   []*TransactionLine     `json:"entries"`
}

// TransactionLine represents a transaction line in a ledger
type TransactionLine struct {
	AccountID string `json:"account"`
	amount    int    `json:"amount"`
}

// IsValid validates the amount list of a transaction
func (t *Transaction) IsValid() bool {
	sum := 0
	for _, line := range t.entries {
		sum += line.amount
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
func (t *TransactionDB) IsExists(id string) (bool, ledgerError.ApplicationError) {
	var exists bool
	err := t.db.QueryRow("SELECT EXISTS (SELECT id FROM transactions WHERE id=$1)", id).Scan(&exists)
	if err != nil {
		log.Println("Error executing transaction exists query:", err)
		return false, DBError(err)
	}
	return exists, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *TransactionDB) IsConflict(transaction *Transaction) (bool, ledgerError.ApplicationError) {
	// Read existing entries
	rows, err := t.db.Query("SELECT account_id, amount FROM entries WHERE transaction_id=$1", transaction.ID)
	if err != nil {
		log.Println("Error executing transaction entries query:", err)
		return false, DBError(err)
	}
	defer rows.Close()
	var existingentries []*TransactionLine
	for rows.Next() {
		line := &TransactionLine{}
		if err := rows.Scan(&line.AccountID, &line.amount); err != nil {
			log.Println("Error scanning transaction entries:", err)
			return false, DBError(err)
		}
		existingentries = append(existingentries, line)
	}
	if err := rows.Err(); err != nil {
		log.Println("Error iterating transaction entries rows:", err)
		return false, DBError(err)
	}

	// Compare new and existing transaction entries
	return !containsSameElements(transaction.entries, existingentries), nil
}

// Transact creates the input transaction in the DB
func (t *TransactionDB) Transact(txn *Transaction) bool {
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

	// Accounts do not need to be predefined
	// they are called into existence when they are first used.
	for _, line := range txn.entries {
		_, err = tx.Exec("INSERT INTO accounts (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", line.AccountID)
		if err != nil {
			return handleTransactionError(tx, errors.Wrap(err, "insert account failed"))
		}
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

	if txn.Timestamp == "" {
		txn.Timestamp = time.Now().UTC().Format(LedgerTimestampLayout)
	}

	_, err = tx.Exec("INSERT INTO transactions (id, timestamp, data) VALUES ($1, $2, $3)", txn.ID, txn.Timestamp, transactionData)
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

	// Add transaction entries
	for _, line := range txn.entries {
		_, err = tx.Exec("INSERT INTO entries (transaction_id, account_id, amount) VALUES ($1, $2, $3)", txn.ID, line.AccountID, line.amount)
		if err != nil {
			return handleTransactionError(tx, errors.Wrap(err, "insert entries failed"))
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

	q := "UPDATE transactions SET data = $1 WHERE id = $2"
	_, err = t.db.Exec(q, tData, txn.ID)
	if err != nil {
		return DBError(err)
	}
	return nil
}
