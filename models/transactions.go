package models

import (
	"database/sql"
	"log"
	"time"

	"fmt"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/pkg/errors"
	"strings"
)

const (
	// LedgerTimestampLayout is the timestamp layout followed in Ledger
	LedgerTimestampLayout = "2006-01-02 15:04:05.000"
)

// Transaction represents a transaction in a ledger
type Transaction struct {
	ID           sql.NullInt64
	Reference    sql.NullString      `json:"reference"`
	Currency     sql.NullString      `json:"currency"`
	Data         DataMap             `json:"data"`
	TransactedAt sql.NullString      `json:"transacted_at"`
	Entries      []*TransactionEntry `json:"entries"`
}

// TransactionEntry represents a transaction line in a ledger
type TransactionEntry struct {
	ID            sql.NullInt64
	AccountID     sql.NullInt64
	Account       sql.NullString `json:"account"`
	TransactionID sql.NullInt64
	Transaction   sql.NullString `json:"transaction"`
	Amount        sql.NullInt64  `json:"amount"`
	Credit        bool           `json:"credit"`
	Balance       sql.NullInt64  `json:"balance"`
	Currency      sql.NullString `json:"currency"`
	TransactedAt  sql.NullString `json:"transacted_at"`
}

func (t *TransactionEntry) Equal(ot TransactionEntry) bool {
	return t.Account.String == ot.Account.String && t.Amount.Int64 == ot.Amount.Int64
}

// IsValid validates the Amount list of a transaction
func (t *Transaction) IsValid() bool {
	sum := int64(0)
	for _, entry := range t.Entries {
		if entry.Credit {
			sum += entry.Amount.Int64
		} else {
			sum -= entry.Amount.Int64
		}

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
	for _, entry := range txn.Entries {
		accountRefSet[entry.Account.String] = true
	}

	accountRefs := make([]interface{}, 0, len(accountRefSet))
	placeholders := make([]string, 0, len(accountRefSet))
	count := 1
	for k := range accountRefSet {
		accountRefs = append(accountRefs, strings.ToUpper(k))
		placeholders = append(placeholders, fmt.Sprintf("$%d", count))
		count++
	}

	if len(placeholders) == 0 {
		return nil, ledger.ErrorAccountsNotFound.Extend("No Accounts were found in the system for the transaction")
	}

	placeholderString := strings.Join(placeholders, ",")

	query := "SELECT a.account_id, a.reference, a.currency, a.data, b.balance, l.ledger_type FROM accounts a LEFT JOIN current_balances b USING(account_id) LEFT JOIN ledgers l USING(ledger_id) WHERE a.reference IN (%s)"

	rows, err := t.db.Query(fmt.Sprintf(query, placeholderString), accountRefs...)

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
		if err := rows.Scan(&account.ID, &account.Reference, &account.Currency, &account.Data, &account.Balance, &account.LedgerType); err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}

		accountsMap[account.Reference.String] = account
	}
	if err := rows.Err(); err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	for _, entry := range txn.Entries {

		if entry.Amount.Int64 == 0 {
			return nil, ledger.ErrorTransactionEntryHasZeroAmount.Extend(fmt.Sprintf("A transaction entry for account : %s has a zero amount", entry.Account))
		}

		account, ok := accountsMap[entry.Account.String]
		if !ok {
			//// Accounts have to be predefined hence check all references exist.
			return nil, ledger.ErrorAccountNotFound.Extend(fmt.Sprintf("Account %s was not found in the system", entry.Account))
		}

		if txn.Currency != account.Currency {
			log.Println(fmt.Sprintf("Account %s has differing currency of %s to transaction currency of %s", entry.Account, account.Currency, txn.Currency))
			return nil, ledger.ErrorTransactionAccountsDifferCurrency.Extend(fmt.Sprintf("Account %s has differing currency of %s to transaction currency of %s", entry.Account, account.Currency, txn.Currency))
		}
	}

	return accountsMap, nil
}

func (t *TransactionDB) IsExists(reference string) (bool, ledger.ApplicationLedgerError) {
	var exists bool
	err := t.db.QueryRow("SELECT EXISTS (SELECT transaction_id FROM transactions WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err)
	}
	return exists, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *TransactionDB) IsConflict(transaction *Transaction) (bool, ledger.ApplicationLedgerError) {
	// Read existing Entries
	rows, err := t.db.Query("SELECT e.entry_id, a.account_id, a.reference, e.amount, e.account_balance  FROM entries e LEFT JOIN accounts a USING(account_id) LEFT JOIN transactions t USING(transaction_id) WHERE t.reference=$1", transaction.Reference.String)
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
		if err := rows.Scan(&entry.ID, &entry.AccountID, &entry.Account, &entry.Amount, &entry.Balance); err != nil {
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

	if !txn.Reference.Valid {
		txn.Reference = generateReference("txn")
	} else {

		// Check if a transaction with same Reference already exists
		isExists, err1 := t.IsExists(txn.Reference.String)
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
			return t.getByRef(txn.Reference.String)
		}
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

	if !txn.TransactedAt.Valid {
		txn.TransactedAt.String = time.Now().UTC().Format(LedgerTimestampLayout)
	}

	var transactionID int64
	err = tx.QueryRow("INSERT INTO transactions (reference, currency, transacted_at, data) VALUES ($1, $2, $3, $4)  RETURNING transaction_id, reference",
		txn.Reference.String, txn.Currency.String, txn.TransactedAt, txn.Data).Scan(&transactionID, &txn.Reference.String)
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "insert transaction failed"))
	}

	// Add transaction Entries in one go to succeed or fail all
	placeHolders := make([]string, len(txn.Entries))
	entryParams := make([]interface{}, 0)
	for i, line := range txn.Entries {
		account := accountsMap[line.Account.String]
		placeHolders[i] = fmt.Sprintf("($%d, $%d, $%d, $%d)", i*4+1, i*4+2, i*4+3, i*4+4)

		entryAmount := line.Amount.Int64
		if line.Amount.Int64 < 0 {
			entryAmount = - line.Amount.Int64
		}
		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit && (account.LedgerType.String == "ASSET" || account.LedgerType.String == "EXPENSE") ||
			!line.Credit && (account.LedgerType.String == "LIABILITY" || account.LedgerType.String == "INCOME" || account.LedgerType.String == "CAPITAL") {
			entryAmount = -entryAmount
		}

		entryParams = append(entryParams, transactionID, account.ID, line.Credit, entryAmount)
	}

	insertQuery := "INSERT INTO entries (transaction_id, account_id, credit, amount) VALUES " + strings.Join(placeHolders, ",")

	_, err = tx.Exec(insertQuery, entryParams...)
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "insert Entries failed"))
	}

	// Commit the entire transaction
	err = tx.Commit()
	if err != nil {
		return handleTransactionError(tx, errors.Wrap(err, "commit transaction failed"))
	}

	return t.getByRef(txn.Reference.String)
}

// getByRef returns a transaction with the given Reference
func (t *TransactionDB) getByRef(reference string) (*Transaction, ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	reference = strings.ToUpper(reference)

	transaction := new(Transaction)
	err := t.db.QueryRow(
		"SELECT  t.transaction_id, t.reference, t.transacted_at, t.data FROM transactions t WHERE t.reference=$1", &reference).Scan(
		&transaction.ID, &transaction.Reference, &transaction.TransactedAt, &transaction.Data)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorTransactionNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	var entries []*TransactionEntry
	rows, err := t.db.Query(
		"SELECT e.entry_id, a.reference, a.account_id, e.amount, e.account_balance, e.credit, a.currency, t.transacted_at, t.transaction_id FROM entries e JOIN transactions t USING(transaction_id)  LEFT JOIN accounts a USING(account_id) WHERE t.reference=$1 ORDER BY e.account_id", &reference)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorAccountsNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	defer rows.Close()

	for rows.Next() {
		entry := new(TransactionEntry)
		if err := rows.Scan(&entry.ID, &entry.Account, &entry.AccountID, &entry.Amount,
			&entry.Balance, &entry.Credit, &entry.Currency, &entry.TransactedAt, &entry.TransactionID); err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}

		entries = append(entries, entry)
	}

	transaction.Entries = entries

	return transaction, nil
}

// UpdateTransaction updates data of the given transaction
func (t *TransactionDB) UpdateTransaction(txn *Transaction) (*Transaction, ledger.ApplicationLedgerError) {
	existingTransaction, err := t.getByRef(txn.Reference.String)
	if err != nil {
		return nil, err
	}

	for key, value := range txn.Data {
		if value != "" && value != existingTransaction.Data[key] {
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
func (t *TransactionDB) Reverse(reference string) (*Transaction, ledger.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	reversalTxn, err1 := t.getByRef(reference)
	if err1 != nil {
		return nil, err1
	}

	for _, entry := range reversalTxn.Entries {
		entry.Credit = !entry.Credit
		entry.Amount.Int64 = Abs(entry.Amount.Int64)
	}

	reversalTxn.Reference = sql.NullString{String:  fmt.Sprintf("REVERSAL_%s", reversalTxn.Reference.String), Valid: true}
	return t.Transact(reversalTxn)
}
