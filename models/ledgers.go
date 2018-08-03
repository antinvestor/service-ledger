package models

import (
	"database/sql"
	"encoding/json"
	"database/sql/driver"
	"errors"
	"bitbucket.org/caricah/service-ledger/ledger"
)

// Ledger represents the hierachy for organizing ledgers with information such as type, and JSON data
type Ledger struct {
	ID        int64          `json:"id"`
	Reference sql.NullString `json:"reference"`
	Type      string         `json:"type"`
	ParentID  sql.NullInt64
	Parent    string  `json:"parent"`
	Data      DataMap `json:"data"`
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

// LedgerDB provides all functions related to ledger Ledger
type LedgerDB struct {
	db *sql.DB
}

// NewLedgerDB provides instance of `LedgerDB`
func NewLedgerDB(db *sql.DB) LedgerDB {
	return LedgerDB{db: db}
}

// GetByID returns an acccount with the given Reference
func (l *LedgerDB) GetByID(id int64) (*Ledger, *ledger.ApplicationLedgerError) {

	if id <= 0 {
		return nil, ledger.ErrorUnspecifiedID
	}

	gl := &Ledger{ID: id}

	err := l.db.QueryRow(
		"SELECT ledger_id, reference, ledger_type, parent_ledger_id, data FROM ledgers WHERE ledger_id=$1", &id).
		Scan(&gl.ID, &gl.Reference, &gl.Type, &gl.ParentID, &gl.Data)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorLedgerNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err.Error())
	}

	return gl, nil
}

// GetByID returns an acccount with the given Reference
func (l *LedgerDB) GetByRef(reference string) (*Ledger, *ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	lg := &Ledger{Reference: sql.NullString{String: reference}}

	err := l.db.QueryRow(
		"SELECT ledger_id, reference, ledger_type, parent_ledger_id, data FROM ledgers WHERE reference=$1", &reference).
		Scan(&lg.ID, &lg.Reference, &lg.Type, &lg.ParentID, &lg.Data)
	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorLedgerNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err.Error())
	}

	return lg, nil
}

// IsExists says whether an ledger exists or not
func (l *LedgerDB) IsExists(reference string) (bool, *ledger.ApplicationLedgerError) {
	var exists bool
	err := l.db.QueryRow("SELECT EXISTS (SELECT ledger_id FROM ledgers WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err.Error())
	}
	return exists, nil
}

// CreateLedger creates a new ledger in the ledger
func (l *LedgerDB) CreateLedger(lg *Ledger) (int64, *ledger.ApplicationLedgerError) {

	if lg.ParentID.Valid {
		err := l.db.QueryRow("SELECT ledger_id FROM ledgers WHERE id = ($1)", lg.ParentID).Scan(&lg.ParentID)
		if err != nil {
			return 0, ledger.ErrorSystemFailure.Override(err.Error())
		}
	}

	var ledgerID int64
	q := "INSERT INTO ledgers (reference, parent_ledger_id, ledger_type, data)  VALUES ($1, $2) RETURNING ledger_id"
	err := l.db.QueryRow(q, lg.Reference, lg.ParentID, lg.Type, lg.Data).Scan(&ledgerID)
	if err != nil {
		return 0, ledger.ErrorSystemFailure.Override(err.Error())
	}

	return ledgerID, nil

}

// UpdateLedger updates the ledger with new data
func (l *LedgerDB) UpdateLedger(lg *Ledger) *ledger.ApplicationLedgerError {

	existingLedger, err := l.GetByRef(lg.Reference.String)
	if err != nil {
		return err
	}

	for key, value := range lg.Data {
		if value != nil && value != existingLedger.Data[key] {
			existingLedger.Data[key] = value
		}
	}

	q := "UPDATE ledgers SET data = $1 WHERE ledger_id = $2"
	_, err1 := l.db.Exec(q, existingLedger.Data, existingLedger.ID)
	if err1 != nil {
		return ledger.ErrorSystemFailure.Override(err.Error())
	}

	return nil
}
