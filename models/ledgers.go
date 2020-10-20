package models

import (
	"github.com/antinvestor/service-ledger/ledger"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log"
	"strings"
)

// Ledger represents the hierachy for organizing ledgers with information such as type, and JSON data
type Ledger struct {
	ID        int64          `json:"id"`
	Reference sql.NullString `json:"reference"`
	Type      sql.NullString `json:"type"`
	ParentID  sql.NullInt64
	Parent    sql.NullString `json:"parent"`
	Data      DataMap        `json:"data"`
}

type DataMap map[string]string

func (p DataMap) Value() (driver.Value, error) {

	if p == nil {
		p = make(map[string]string)
	}

	j, err := json.Marshal(p)
	return j, err
}

func (p *DataMap) Scan(src interface{}) error {

	if src == nil {
		*p = nil
		return nil
	}

	source, ok := src.([]byte)
	if !ok {
		return errors.New("database value was not a jsonb string")
	}

	var i map[string]string
	err := json.Unmarshal(source, &i)
	if err != nil {
		return err
	}

	*p = i

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
func (l *LedgerDB) GetByID(id int64) (*Ledger, ledger.ApplicationLedgerError) {

	if id <= 0 {
		return nil, ledger.ErrorUnspecifiedID
	}

	gl := &Ledger{ID: id}

	rows, err := l.db.Query(
		"SELECT ledger_id, reference, ledger_type, parent_ledger_id, data FROM ledgers WHERE ledger_id=$1", &id)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorLedgerNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	for rows.Next() {
		err = rows.Scan(&gl.ID, &gl.Reference, &gl.Type, &gl.ParentID, &gl.Data)
		if err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}
	}

	return gl, nil
}

// GetByID returns an acccount with the given Reference
func (l *LedgerDB) GetByRef(reference string) (*Ledger, ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	lg := new(Ledger)

	rows, err := l.db.Query(
		"SELECT ledger_id, reference, ledger_type, parent_ledger_id, data FROM ledgers WHERE reference=$1", reference)

	switch {

	case err == sql.ErrNoRows:
		return nil, ledger.ErrorLedgerNotFound
	case err != nil:
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	for rows.Next() {
		err = rows.Scan(&lg.ID, &lg.Reference, &lg.Type, &lg.ParentID, &lg.Data)
		if err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}
	}
	return lg, nil
}

// IsExists says whether an ledger exists or not
func (l *LedgerDB) IsExists(reference string) (bool, ledger.ApplicationLedgerError) {
	var exists bool
	err := l.db.QueryRow("SELECT EXISTS (SELECT ledger_id FROM ledgers WHERE reference=$1)", reference).Scan(&exists)
	if err != nil {
		return false, ledger.ErrorSystemFailure.Override(err)
	}
	return exists, nil
}

// CreateLedger creates a new ledger in the ledger
func (l *LedgerDB) CreateLedger(lg *Ledger) (*Ledger, ledger.ApplicationLedgerError) {

	var err error

	if lg.Reference.String == "" {
		lg.Reference = generateReference("lgr")
	}

	if lg.Parent.Valid {

		pLg, err := l.GetByRef(lg.Parent.String)
		if err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}

		log.Printf("parent ledger found to have type value : %v, id : %v ", pLg.Type, pLg.ID)


		lg.ParentID.Int64 = pLg.ID
	}

	log.Printf("ledger type value : %v and parent id %v : from reference : %v", lg.Type, lg.ParentID, lg.Parent)

	if lg.Reference.Valid {
		q := "INSERT INTO ledgers (reference, parent_ledger_id, ledger_type, data)  VALUES ($1, $2, $3, $4) RETURNING ledger_id, reference"
		err = l.db.QueryRow(q, strings.ToUpper(lg.Reference.String), lg.ParentID, lg.Type.String, lg.Data).Scan(&lg.ID, &lg.Reference)
	} else {
		q := "INSERT INTO ledgers (parent_ledger_id, ledger_type, data)  VALUES ($1, $2, $3) RETURNING ledger_id, reference"
		err = l.db.QueryRow(q, lg.ParentID, lg.Type.String, lg.Data).Scan(&lg.ID, &lg.Reference)
	}

	if err != nil {
		log.Printf("error : %v", err)
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return lg, nil

}

// UpdateLedger updates the ledger with new data
func (l *LedgerDB) UpdateLedger(lg *Ledger) (*Ledger, ledger.ApplicationLedgerError) {

	existingLedger, err := l.GetByRef(lg.Reference.String)
	if err != nil {
		return nil, err
	}

	for key, value := range lg.Data {
		if value != "" && value != existingLedger.Data[key] {
			existingLedger.Data[key] = value
		}
	}

	q := "UPDATE ledgers SET data = $1 WHERE ledger_id = $2"
	_, err1 := l.db.Exec(q, existingLedger.Data, existingLedger.ID)
	if err1 != nil {
		return nil, ledger.ErrorSystemFailure.Override(err1)
	}

	return existingLedger, nil
}
