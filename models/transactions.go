package models

import (
	"database/sql"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"strings"
	"fmt"
	"bitbucket.org/caricah/service-ledger/ledger"
)

const (
	// LedgerTimestampLayout is the timestamp layout followed in Ledger
	LedgerTimestampLayout = "2006-01-02 15:04:05.000"
)

// Transaction represents a transaction in a ledger
type Transaction struct {
	ID 			int64
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

// Validate checks all issues around transaction are satisfied
func (t *TransactionDB) Validate(txn *Transaction) (bool, *ledger.ApplicationLedgerError) {


	accountRefSet := map[string]interface{}{}
	for _,entry := range txn.Entries {
		accountRefSet[entry.Account] = true
	}

	accountRefs := make([]interface{}, 0, len(accountRefSet))
	placeholders := make([]string, 0, len(accountRefSet))
	count := 0
	for k := range accountRefSet {
		accountRefs = append(accountRefs, strings.ToLower(k))
		placeholders = append(placeholders, fmt.Sprintf("$%d", count) )
		count++
	}

	placeholderString := strings.Join(placeholders, ",")

	rows, err := t.db.Query(fmt.Sprintf(
		"SELECT id, reference, currency, data, balance FROM account LEFT JOIN current_balances " +
			   "WHERE account_id IN (%s)", placeholderString), accountRefs ...)

	switch {

	case err == sql.ErrNoRows:
		return false, ledger.ErrorAccountsNotFound
	case err != nil:
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}


	defer rows.Close()
	accountsMap := map[string]Account{}
	for rows.Next() {
		account := Account{}
		if err := rows.Scan(&account.ID, &account.Reference, &account.Currency, &account.Data, &account.Balance); err != nil {
			return false, ledger.ErrorSystemFailure.Override(err.Error())
		}
		accountsMap[account.Reference] = account
	}
	if err := rows.Err(); err != nil {
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}

	defaultCurrency := ""
	for _, entry := range txn.Entries {

		if entry.Amount == 0 {
			return false, ledger.ErrorTransactionEntryHasZeroAmount.Override(fmt.Sprintf("A transaction entry for account : %s has a zero amount", entry.Account))
		}

		account, ok := accountsMap[entry.Account]
		if !ok{
			//// Accounts have to be predefined hence check all references exist.
			return false, ledger.ErrorAccountNotFound.Override("Account %s was not found in the system")
		}

		if defaultCurrency == ""{
			defaultCurrency = account.Currency
		}

		if defaultCurrency != account.Currency {
			log.Println(fmt.Sprintf("Account %s currency of %s has to match %s", entry.Account, account.Currency, defaultCurrency))
			return false, ledger.ErrorTransactionAccountsDifferCurrency.Override(fmt.Sprintf("Account %s currency of %s is different from %s", entry.Account, account.Currency, defaultCurrency))
		}

	}

	return true, nil
}

func (t *TransactionDB) IsExists(reference string) (bool, *ledger.ApplicationLedgerError) {
	var exists bool
	err := t.db.QueryRow("SELECT EXISTS (SELECT transaction_id FROM transactions WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}
	return exists, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *TransactionDB) IsConflict(transaction *Transaction) (bool, *ledger.ApplicationLedgerError) {
	// Read existing Entries
	rows, err := t.db.Query("SELECT entries.entry_id, accounts.account_id, accounts.reference, entries.amount FROM entries LEFT JOIN accounts USING(account_id) LEFT JOIN transactions USING(transaction_id) WHERE transactions.reference=$1", transaction.Reference)
	switch {

	case err == sql.ErrNoRows:
		return false, ledger.ErrorTransactionEntriesNotFound
	case err != nil:
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}

	defer rows.Close()
	var existingentries []*TransactionEntry
	for rows.Next() {
		entry := &TransactionEntry{}
		if err := rows.Scan(&entry.ID, &entry.AccountID, &entry.Account, &entry.Amount); err != nil {
			return false, ledger.ErrorSystemFailure.Override(err.Error())
		}
		existingentries = append(existingentries, entry)
	}
	if err := rows.Err(); err != nil {
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}

	// Compare new and existing transaction Entries
	return !containsSameElements(transaction.Entries, existingentries), nil
}

// Transact creates the input transaction in the DB
func (t *TransactionDB) Transact(txn *Transaction) (bool, *ledger.ApplicationLedgerError) {

	var err error
	_, err1 := t.Validate(txn)
	if err1 != nil {
		return false, err1
	}

	// Start the transaction
	tx, err := t.db.Begin()
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}

	// Rollback transaction on any failures
	handleTransactionError := func(tx *sql.Tx, err error) (bool, *ledger.ApplicationLedgerError) {
		log.Println("Rolling back the transaction:", txn.ID)
		err1 := tx.Rollback()
		if err1 != nil {
			log.Println("Error rolling back transaction:", err1)
			return false, ledger.ErrorSystemFailure.Override(err.Error() +" Also "+ err1.Error())
		}
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}

	if txn.TranactedAt == "" {
		txn.TranactedAt = time.Now().UTC().Format(LedgerTimestampLayout)
	}

	var transactionID int64
	 err = tx.QueryRow("INSERT INTO transactions (reference, transacted_at, data) VALUES ($1, $2, $3)  RETURNING transaction_id",
	 	txn.Reference, txn.TranactedAt, txn.Data).Scan(&transactionID)
	if err != nil {
		// Ignore duplicate transactions and return success response
		if err.(*pq.Error).Code.Name() == "unique_violation" {
			err = tx.Rollback()
			if err != nil {
				log.Println("Error rolling back transaction:", err)
			}
			return true,  ledger.ErrorTransactionAlreadyExists
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

	return true, nil
}


// getByRef returns a transaction with the given Reference
func (t *TransactionDB) getByRef(reference string) (*Transaction, *ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	transaction := new(Transaction)
	err := t.db.QueryRow(
		"SELECT  transaction_id, reference, transacted_at, data FROM transactions WHERE reference=$1", &reference).Scan(
		&transaction.ID, &transaction.Reference, &transaction.TranactedAt,&transaction.Data)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorTransactionNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err.Error())
	}

	return transaction, nil
}


// UpdateTransaction updates data of the given transaction
func (t *TransactionDB) UpdateTransaction(txn *Transaction) *ledger.ApplicationLedgerError {
	existingTransaction, err := t.getByRef(txn.Reference)
	if err != nil {
		return err
	}

	for key, value := range txn.Data {
		if value != nil && value != existingTransaction.Data[key] {
			existingTransaction.Data[key] = value
		}
	}

	q := "UPDATE transactions SET data = $1 WHERE transaction_id = $2"
	_, err1 := t.db.Exec(q, existingTransaction.Data, txn.ID)
	if err1 != nil {
		return ledger.ErrorSystemFailure.Override(err1.Error())
	}
	return nil
}
