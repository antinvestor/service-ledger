package models

import (
	"database/sql"
	"github.com/antinvestor/service-ledger/ledger"
	"golang.org/x/text/currency"
	"strings"
)

// Account represents the ledger account with information such as Reference, balance and JSON data
type Account struct {
	ID         sql.NullInt64  `json:"id"`
	Reference  sql.NullString `json:"reference"`
	Currency   sql.NullString `json:"currency"`
	Balance    sql.NullInt64  `json:"balance"`
	LedgerID   sql.NullInt64
	Ledger     sql.NullString `json:"ledger"`
	Data       DataMap        `json:"data"`
	LedgerType sql.NullString `json:"ledger_type"`
}

// AccountDB provides all functions related to ledger account
type AccountDB struct {
	db *sql.DB
}

// NewAccountDB provides instance of `AccountDB`
func NewAccountDB(db *sql.DB) AccountDB {
	return AccountDB{db: db}
}

// GetByID returns an acccount with the given Reference
func (a *AccountDB) GetByID(id int64) (*Account, ledger.ApplicationLedgerError) {

	if id <= 0 {
		return nil, ledger.ErrorUnspecifiedID
	}

	account := new(Account)

	err := a.db.QueryRow(
		"SELECT account_id, reference, currency, data, balance FROM account LEFT JOIN current_balances WHERE account_id=$1", &id).Scan(
		&account.ID, &account.Reference, &account.Currency, &account.Data, &account.Balance)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorAccountNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return account, nil
}

// GetByRef returns an acccount with the given Reference
func (a *AccountDB) GetByRef(reference string) (*Account, ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	reference = strings.ToUpper(reference)

	account := new(Account)
	err := a.db.QueryRow(
		"SELECT  accounts.account_id, reference, currency, ledger_id, data, balance FROM accounts LEFT JOIN current_balances USING(account_id) WHERE reference=$1", reference).Scan(
		&account.ID, &account.Reference, &account.Currency, &account.LedgerID, &account.Data, &account.Balance)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorAccountNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return account, nil
}

// IsExists says whether an account exists or not
func (a *AccountDB) IsExists(reference string) (bool, ledger.ApplicationLedgerError) {
	var exists bool
	err := a.db.QueryRow("SELECT EXISTS (SELECT account_id FROM accounts WHERE reference=$1)", strings.ToUpper(reference)).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err)
	}
	return exists, nil
}

// CreateAccount creates a new account in the ledger
func (a *AccountDB) CreateAccount(account *Account) (*Account, ledger.ApplicationLedgerError) {

	if account.Reference.Valid {

		// Check if an account with same Reference already exists
		isExists, err := a.IsExists(account.Reference.String)

		if err != nil {
			return nil, err
		}

		if isExists {
			return nil, ledger.ErrorAccountWithReferenceExists
		}

	} else {
		account.Reference = generateReference("acc")
	}

	if account.Ledger.Valid {
		err := a.db.QueryRow("SELECT ledger_id FROM ledgers WHERE reference=$1", account.Ledger.String).Scan(&account.LedgerID)
		switch {
		case err == sql.ErrNoRows:
			return nil, ledger.ErrorLedgerNotFound
		case err != nil:
			return nil, ledger.ErrorSystemFailure.Override(err)
		}
	} else if account.LedgerID.Valid {
		err := a.db.QueryRow("SELECT ledger_id FROM ledgers WHERE ledger_id = $1", account.LedgerID).Scan(&account.LedgerID)
		switch {

		case err == sql.ErrNoRows:
			return nil, ledger.ErrorLedgerNotFound
		case err != nil:
			return nil, ledger.ErrorSystemFailure.Override(err)
		}
	} else {
		return nil, ledger.ErrorUnspecifiedID
	}

	currencyUnit, err := currency.ParseISO(account.Currency.String)
	if err != nil {
		return nil, ledger.ErrorAccountsCurrencyUnknown
	}

	q := "INSERT INTO accounts (reference, currency, ledger_id, data)  VALUES ($1, $2, $3, $4)"
	_, err = a.db.Exec(q, strings.ToUpper(account.Reference.String), currencyUnit.String(),
		account.LedgerID, account.Data)
	if err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return a.GetByRef(account.Reference.String)
}

// UpdateAccount updates the account with new data
func (a *AccountDB) UpdateAccount(reference string, dataMap DataMap) (*Account, ledger.ApplicationLedgerError) {

	existingAccount, err := a.GetByRef(reference)
	if err != nil {
		return nil, err
	}

	for key, value := range dataMap {
		if value != "" && value != existingAccount.Data[key] {
			existingAccount.Data[key] = value
		}
	}

	q := "UPDATE accounts SET data = $1 WHERE account_id = $2"
	_, err1 := a.db.Exec(q, existingAccount.Data, existingAccount.ID)
	if err1 != nil {
		return nil, ledger.ErrorSystemFailure.Override(err1)
	}

	return a.GetByRef(reference)
}
