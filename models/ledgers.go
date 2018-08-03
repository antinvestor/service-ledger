package models

import (
	"database/sql"
	"encoding/json"
	"log"

	ledgerError "bitbucket.org/caricah/service-ledger/errors"
)

// Ledger represents the hierachy for organizing ledgers with information such as type, and JSON data
type Ledger struct {
	ID       int64                	`json:"id"`
	Type     string               	`json:"type"`
	ParentId sql.NullInt64          `json:"parent"`
	Data     DataMap 				`json:"data"`
}

// LedgerDB provides all functions related to ledger Ledger
type LedgerDB struct {
	db *sql.DB
}

// NewLedgerDB provides instance of `LedgerDB`
func NewLedgerDB(db *sql.DB) LedgerDB {
	return LedgerDB{db: db}
}

// GetByID returns an acccount with the given Reference
func (l *LedgerDB) GetByID(id int64) (*Ledger, ledgerError.ApplicationError) {
	ledger := &Ledger{ID: id}

	err := l.db.QueryRow(
		"SELECT ledger_type, parent_ledger_id, data FROM ledgers WHERE ledger_id=$1", &id).
		Scan(&ledger.Type, &ledger.ParentId, &ledger.Data)
	if err != nil {
		return nil, DBError(err)
	}

	return ledger, nil
}

// IsExists says whether an ledger exists or not
func (l *LedgerDB) IsExists(hash string) (bool, ledgerError.ApplicationError) {
	var exists bool
	err := l.db.QueryRow("SELECT EXISTS (SELECT ledger_id FROM ledgers WHERE reference=$1)", hash).Scan(&exists)
	if err != nil {
		log.Println("Error executing ledger exists query:", err)
		return false, DBError(err)
	}
	return exists, nil
}

// CreateLedger creates a new ledger in the ledger
func (l *LedgerDB) CreateLedger(ledger *Ledger) (int64, ledgerError.ApplicationError) {
	data, err := json.Marshal(ledger.Data)
	if err != nil {
		return 0, JSONError(err)
	}

	ledgerData := "{}"
	if ledger.Data != nil && data != nil {
		ledgerData = string(data)
	}

	if ledger.ParentId.Valid {
		err := l.db.QueryRow("SELECT ledger_id FROM ledgers WHERE id = ($1)", ledger.ParentId).Scan(&ledger.ParentId)
		if err != nil {
			return 0, DBError(err)
		}
	}

	var ledgerID int64
	q := "INSERT INTO ledgers (ledger_type, data)  VALUES ($1, $2) RETURNING ledger_id"
	err = l.db.QueryRow(q, ledger.Type, ledgerData).Scan(&ledgerID)
	if err != nil {
		return 0, DBError(err)
	}

	return ledgerID, nil

}

// UpdateLedger updates the ledger with new data
func (l *LedgerDB) UpdateLedger(ledger *Ledger) ledgerError.ApplicationError {
	data, err := json.Marshal(ledger.Data)
	if err != nil {
		return JSONError(err)
	}
	ledgerData := "{}"
	if ledger.Data != nil && data != nil {
		ledgerData = string(data)
	}

	q := "UPDATE ledgers SET data = $1 WHERE ledger_id = $2"
	_, err = l.db.Exec(q, ledgerData, ledger.ID)
	if err != nil {
		return DBError(err)
	}

	return nil
}
