package models

import (
	"database/sql"
	"encoding/json"
	"log"

	ledgerError "bitbucket.org/caricah/service-ledger/errors"
	"database/sql/driver"
	"errors"
)

// Account represents the ledger account with information such as Reference, balance and JSON data
type Account struct {
	ID        int64                  `json:"id"`
	Reference string                 `json:"reference"`
	Balance   int                    `json:"balance"`
	LedgerID  int64                  `json:"ledger"`
	Data      DataMap  				 `json:"data"`
}

type DataMap map[string]interface{}

func (p DataMap) Value() (driver.Value, error) {
	j, err := json.Marshal(p)
	return j, err
}

func (p *DataMap) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("Type assertion .([]byte) failed.")
	}

	var i interface{}
	err := json.Unmarshal(source, &i)
	if err != nil {
		return err
	}

	*p, ok = i.(map[string]interface{})
	if !ok {
		return errors.New("Type assertion .(map[string]interface{}) failed.")
	}

	return nil
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
func (a *AccountDB) GetByID(id int64) (*Account, ledgerError.ApplicationError) {
	account := &Account{ID: id}

	err := a.db.QueryRow(
		"SELECT reference, data, balance FROM account LEFT JOIN current_balances WHERE account_id=$1", &account.ID).Scan(
		&account.Reference, &account.Data, &account.Balance)
	switch {
	case err == sql.ErrNoRows:
		account.Balance = 0
	case err != nil:
		return nil, DBError(err)
	}

	return account, nil
}

// GetByRef returns an acccount with the given Reference
func (a *AccountDB) GetByRef(reference string) (*Account, ledgerError.ApplicationError) {
	account := &Account{}
	err := a.db.QueryRow(
		"SELECT  accounts.account_id, reference, ledger_id, data, balance FROM accounts LEFT JOIN current_balances USING(account_id) WHERE reference=$1", &reference).Scan(
		&account.ID, &account.Reference, &account.LedgerID, &account.Data, &account.Balance)
	if err != nil {
		return nil, DBError(err)
	}

	return account, nil
}

// IsExists says whether an account exists or not
func (a *AccountDB) IsExists(reference string) (bool, ledgerError.ApplicationError) {
	var exists bool
	err := a.db.QueryRow("SELECT EXISTS (SELECT account_id FROM accounts WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		log.Println("Error executing account exists query:", err)
		return false, DBError(err)
	}
	return exists, nil
}

// CreateAccount creates a new account in the ledger
func (a *AccountDB) CreateAccount(account *Account) ledgerError.ApplicationError {
	data, err := json.Marshal(account.Data)
	if err != nil {
		return JSONError(err)
	}

	accountData := "{}"
	if account.Data != nil && data != nil {
		accountData = string(data)
	}

	if account.LedgerID > 0 {
		err := a.db.QueryRow("SELECT ledger_id FROM ledgers WHERE ledger_id = ($1)", account.LedgerID).Scan(&account.LedgerID)
		if err != nil {
			return DBError(err)
		}
	}

	q := "INSERT INTO accounts (reference, ledger_id, data)  VALUES ($1, $2, $3)"
	_, err = a.db.Exec(q, account.Reference, account.LedgerID, accountData)
	if err != nil {
		return DBError(err)
	}

	return nil
}

// UpdateAccount updates the account with new data
func (a *AccountDB) UpdateAccount(account *Account) ledgerError.ApplicationError {
	data, err := json.Marshal(account.Data)
	if err != nil {
		return JSONError(err)
	}
	accountData := "{}"
	if account.Data != nil && data != nil {
		accountData = string(data)
	}

	q := "UPDATE accounts SET data = $1 WHERE account_id = $2"
	_, err = a.db.Exec(q, accountData, account.ID)
	if err != nil {
		return DBError(err)
	}

	return nil
}
