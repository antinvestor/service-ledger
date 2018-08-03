package models

import (
	"database/sql"
	"bitbucket.org/caricah/service-ledger/ledger"
)

// Account represents the ledger account with information such as Reference, balance and JSON data
type Account struct {
	ID        int64                  `json:"id"`
	Reference string                 `json:"reference"`
	Currency  string 				 `json:"currency"`
	Balance   int                    `json:"balance"`
	LedgerID  int64
	Ledger  string                   `json:"ledger"`
	Data      DataMap  				 `json:"data"`
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
func (a *AccountDB) GetByID(id int64) (*Account, *ledger.ApplicationLedgerError) {

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
		return nil, ledger.ErrorSystemFailure.Override(err.Error())
	}

	return account, nil
}

// GetByRef returns an acccount with the given Reference
func (a *AccountDB) GetByRef(reference string) (*Account, *ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	account := new(Account)
	err := a.db.QueryRow(
		"SELECT  accounts.account_id, reference, currency, ledger_id, data, balance FROM accounts LEFT JOIN current_balances USING(account_id) WHERE reference=$1", &reference).Scan(
		&account.ID, &account.Reference, &account.Currency, &account.LedgerID, &account.Data, &account.Balance)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorAccountNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err.Error())
	}

	return account, nil
}

// IsExists says whether an account exists or not
func (a *AccountDB) IsExists(reference string) (bool, *ledger.ApplicationLedgerError) {
	var exists bool
	err := a.db.QueryRow("SELECT EXISTS (SELECT account_id FROM accounts WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}
	return exists, nil
}

// CreateAccount creates a new account in the ledger
func (a *AccountDB) CreateAccount(account *Account) *ledger.ApplicationLedgerError {

	if account.LedgerID > 0 {
		err := a.db.QueryRow("SELECT ledger_id FROM ledgers WHERE ledger_id = ($1)", account.LedgerID).Scan(&account.LedgerID)
		switch {

		case err == sql.ErrNoRows:
			return ledger.ErrorLedgerNotFound
		case err != nil:
			return ledger.ErrorSystemFailure.Override(err.Error())
		}
	}

	q := "INSERT INTO accounts (reference, currency, ledger_id, data)  VALUES ($1, $2, $3, $4)"
	_, err := a.db.Exec(q, account.Reference, account.Currency, account.LedgerID, account.Data)
	if err != nil {
		return ledger.ErrorSystemFailure.Override(err.Error())
	}

	return nil
}

// UpdateAccount updates the account with new data
func (a *AccountDB) UpdateAccount(account *Account) *ledger.ApplicationLedgerError {

	existingAccount, err := a.GetByRef(account.Reference)
	if err != nil {
		return err
	}

	for key, value := range account.Data {
		if value != nil && value != existingAccount.Data[key] {
			existingAccount.Data[key] = value
		}
	}


	q := "UPDATE accounts SET data = $1 WHERE account_id = $2"
	_, err1 := a.db.Exec(q, existingAccount.Data, account.ID)
	if err1 != nil {
		return ledger.ErrorSystemFailure.Override(err1.Error())
	}

	return nil
}
