package models

import (
	"database/sql"
	"log"
	"time"

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
	ID           int64
	Reference    string              `json:"reference"`
	Data         DataMap             `json:"data"`
	TransactedAt string              `json:"transacted_at"`
	Entries      []*TransactionEntry `json:"entries"`
}

// TransactionEntry represents a transaction line in a ledger
type TransactionEntry struct {
	ID int64
	AccountID int64
	Account string `json:"account"`
	Amount    int64    `json:"amount"`
}

func (t *TransactionEntry) Equal(ot TransactionEntry) bool {
	return strings.ToUpper(t.Account) == strings.ToUpper(ot.Account) && t.Amount == ot.Amount
}

// IsValid validates the Amount list of a transaction
func (t *Transaction) IsValid() bool {
	sum := int64(0)
	for _, entry := range t.Entries {
		sum += entry.Amount
	}
	return sum == int64(0)
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
func (t *TransactionDB) Validate(txn *Transaction) (map[string]Account, ledger.ApplicationLedgerError) {


	// Skip if the transaction is invalid
	// by validating the amount values
	if !txn.IsValid() {
		return nil, ledger.ErrorTransactionHasNonZeroSum
	}

	accountRefSet := map[string]bool{}
	for _,entry := range txn.Entries {
		accountRefSet[entry.Account] = true
	}

	accountRefs := make([]interface{}, 0, len(accountRefSet))
	placeholders := make([]string, 0, len(accountRefSet))
	count := 1
	for k := range accountRefSet {
		accountRefs = append(accountRefs, strings.ToUpper(k))
		placeholders = append(placeholders, fmt.Sprintf("$%d", count) )
		count++
	}

	if len(placeholders) == 0 {
		return nil, ledger.ErrorAccountsNotFound.Extend("No Accounts were found in the system for the transaction")
	}


	placeholderString := strings.Join(placeholders, ",")

	query := "SELECT account_id, reference, currency, data, balance FROM accounts LEFT JOIN current_balances USING(account_id) WHERE reference IN (%s)"

	rows, err := t.db.Query(fmt.Sprintf( query, placeholderString), accountRefs ...)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorAccountsNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}


	defer rows.Close()

	accountsMap := map[string]Account{}
	for rows.Next() {
		account := Account{}
		if err := rows.Scan(&account.ID, &account.Reference, &account.Currency, &account.Data, &account.Balance); err != nil {
			return nil,ledger.ErrorSystemFailure.Override(err)
		}
		accountsMap[account.Reference] = account
	}
	if err := rows.Err(); err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	defaultCurrency := ""
	for _, entry := range txn.Entries {

		if entry.Amount == 0 {
			return nil, ledger.ErrorTransactionEntryHasZeroAmount.Extend(fmt.Sprintf("A transaction entry for account : %s has a zero amount", entry.Account))
		}

		account, ok := accountsMap[strings.ToUpper(entry.Account)]
		if !ok{
			//// Accounts have to be predefined hence check all references exist.
			return nil, ledger.ErrorAccountNotFound.Extend(fmt.Sprintf("Account %s was not found in the system", entry.Account))

		}

		if defaultCurrency == ""{
			defaultCurrency = account.Currency
		}

		if defaultCurrency != account.Currency {
			log.Println(fmt.Sprintf("Account %s currency of %s has to match %s", entry.Account, account.Currency, defaultCurrency))
			return nil,  ledger.ErrorTransactionAccountsDifferCurrency.Extend(fmt.Sprintf("Account %s currency of %s is different from %s", entry.Account, account.Currency, defaultCurrency))
		}

	}

	return accountsMap, nil
}

func (t *TransactionDB) IsExists(reference string) (bool, ledger.ApplicationLedgerError) {
	var exists bool
	err := t.db.QueryRow("SELECT EXISTS (SELECT transaction_id FROM transactions WHERE reference=$1)", strings.ToUpper(reference)).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err)
	}
	return exists, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *TransactionDB) IsConflict(transaction *Transaction) (bool, ledger.ApplicationLedgerError) {
	// Read existing Entries
	rows, err := t.db.Query("SELECT entries.entry_id, accounts.account_id, accounts.reference, entries.amount FROM entries LEFT JOIN accounts USING(account_id) LEFT JOIN transactions USING(transaction_id) WHERE transactions.reference=$1", strings.ToUpper(transaction.Reference))
	switch {

	case err == sql.ErrNoRows:
		return false, ledger.ErrorTransactionEntriesNotFound
	case err != nil:
		return false, ledger.ErrorSystemFailure.Override(err)
	}

	defer rows.Close()
	var existingentries []*TransactionEntry
	for rows.Next() {
		entry := &TransactionEntry{}
		if err := rows.Scan(&entry.ID, &entry.AccountID, &entry.Account, &entry.Amount); err != nil {
			return false, ledger.ErrorSystemFailure.Override(err)
		}
		existingentries = append(existingentries, entry)
	}
	if err := rows.Err(); err != nil {
		return false, ledger.ErrorSystemFailure.Override(err)
	}

	// Compare new and existing transaction Entries
	return !containsSameElements(transaction.Entries, existingentries), nil
}

// Transact creates the input transaction in the DB
func (t *TransactionDB) Transact(txn *Transaction) (*Transaction, ledger.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	isExists, err1 := t.IsExists(txn.Reference)
	if err1 != nil {
		return nil, err1
	}

	if isExists {
		// Check if the transaction entries are different
		// and conflicts with the existing entries
		isConflict, err1 := t.IsConflict(txn)
		if err1 != nil {
			return nil, err1
		}
		if isConflict {
			// The conflicting transactions are denied
			return nil, ledger.ErrorTransactionIsConfilicting
		}
		// Otherwise the transaction is just a duplicate
		// The exactly duplicate transactions are ignored
		transaction, err1 := t.getByRef(txn.Reference)
		if err1 != nil {
			return nil, err1
		}
		return transaction, ledger.ErrorTransactionAlreadyExists
	}


	accountsMap, err1 := t.Validate(txn)
	if err1 != nil {
		return nil, err1
	}

	// Start the transaction
	tx, err := t.db.Begin()
	if err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	// Rollback transaction on any failures
	handleTransactionError := func(tx *sql.Tx, err error) (*Transaction, ledger.ApplicationLedgerError) {
		log.Println("Rolling back the transaction:", txn.ID)
		err1 := tx.Rollback()
		if err1 != nil {
			log.Println("Error rolling back transaction:", err1)
			return nil, ledger.ErrorSystemFailure.Override(err, err1)
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	if txn.TransactedAt == "" {
		txn.TransactedAt = time.Now().UTC().Format(LedgerTimestampLayout)
	}

	var transactionID int64
	 err = tx.QueryRow("INSERT INTO transactions (reference, transacted_at, data) VALUES ($1, $2, $3)  RETURNING transaction_id",
	 	strings.ToUpper(txn.Reference), txn.TransactedAt, txn.Data).Scan(&transactionID)
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "insert transaction failed"))
	}

	// Add transaction Entries in one go to succeed or fail all
	placeHolders := make([]string, len(txn.Entries))
	entryParams := make([]interface{}, 0)
	for i, line := range txn.Entries {
		account := accountsMap[strings.ToUpper(line.Account)]
		placeHolders[i] = fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3 )
		entryParams = append(entryParams, transactionID,  account.ID, line.Amount)
	}

	insertQuery := "INSERT INTO entries (transaction_id, account_id, amount) VALUES " + strings.Join(placeHolders, ",")

	_, err = tx.Exec(insertQuery, entryParams...)
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "insert Entries failed"))
	}

	// Commit the entire transaction
	err = tx.Commit()
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "commit transaction failed"))
	}

	return t.getByRef(txn.Reference)
}


// getByRef returns a transaction with the given Reference
func (t *TransactionDB) getByRef(reference string) (*Transaction, ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	reference = strings.ToUpper(reference)

	transaction := new(Transaction)
	err := t.db.QueryRow(
		"SELECT  transaction_id, reference, transacted_at, data FROM transactions WHERE reference=$1", &reference).Scan(
		&transaction.ID, &transaction.Reference, &transaction.TransactedAt,&transaction.Data)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorTransactionNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return transaction, nil
}


// UpdateTransaction updates data of the given transaction
func (t *TransactionDB) UpdateTransaction(txn *Transaction) (*Transaction, ledger.ApplicationLedgerError) {
	existingTransaction, err := t.getByRef(txn.Reference)
	if err != nil {
		return nil, err
	}

	for key, value := range txn.Data {
		if value != nil && value != existingTransaction.Data[key] {
			existingTransaction.Data[key] = value
		}
	}

	q := "UPDATE transactions SET data = $1 WHERE transaction_id = $2"
	_, err1 := t.db.Exec(q, existingTransaction.Data, txn.ID)
	if err1 != nil {
		return nil, ledger.ErrorSystemFailure.Override(err1)
	}
	return existingTransaction, nil
}

// Reverse creates a reversal  of the input transaction by creating a new transaction
func (t *TransactionDB) Reverse(txn *Transaction) (*Transaction, ledger.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	reversalTxn, err1 := t.getByRef(txn.Reference)
	if err1 != nil {
		return nil, err1
	}

	for _, entry := range reversalTxn.Entries {
		entry.Amount *= -1
	}

	reversalTxn.Reference = fmt.Sprintf("REVERSAL_%s",reversalTxn.Reference)
	return t.Transact(reversalTxn)
}