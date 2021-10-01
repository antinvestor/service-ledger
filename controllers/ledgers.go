package controllers

import (
	"context"
	"database/sql"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
)

type LedgerServer struct {
	DB *sql.DB
}

func ToMap(raw map[string]string) models.DataMap {

	dataMap := make(models.DataMap)
	for key, val := range raw {
		dataMap[key] = val
	}
	return dataMap
}

func FromMap(model models.DataMap) map[string]string {
	return model
}

func fromLedgerType(raw ledger.LedgerType) string {
	return ledger.LedgerType_name[int32(raw)]
}

func toLedgerType(model string) ledger.LedgerType {
	ledgerType := ledger.LedgerType_value[model]
	return ledger.LedgerType(ledgerType)
}

func ledgerToApi(mLg *models.Ledger) *ledger.Ledger {
	return &ledger.Ledger{Reference: mLg.Reference.String, Type: toLedgerType(mLg.Type.String),
		Parent: mLg.Parent.String, Data: FromMap(mLg.Data)}
}

func ledgerFromApi(aLg *ledger.Ledger) *models.Ledger {
	return &models.Ledger{
		Reference: sql.NullString{String: aLg.Reference, Valid: aLg.Reference != ""},
		Type: sql.NullString{String:fromLedgerType(aLg.Type), Valid: true},
		Parent: sql.NullString{String:aLg.Parent, Valid: aLg.Parent != ""},
		ParentID: sql.NullInt64{Valid: false},
		Data: ToMap(aLg.Data)}

}

// Searches for an ledger based on search request json query
func (ledgerSrv *LedgerServer) SearchLedgers(request *ledger.SearchRequest, server ledger.LedgerService_SearchLedgersServer) error {

	engine, aerr := models.NewSearchEngine(ledgerSrv.DB, models.SearchNamespaceLedgers)
	if aerr != nil {
		return aerr
	}

	results, aerr := engine.Query(request.GetQuery())
	if aerr != nil {
		return aerr
	}

	castLedgers, ok := results.([]*models.Ledger)
	if !ok {
		return ledger.ErrorSearchQueryResultsNotCasting
	}

	for _, lg := range castLedgers {
		server.Send(ledgerToApi(lg))
	}

	return nil
}

// Creates a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateLedger(context context.Context, lg *ledger.Ledger) (*ledger.Ledger, error) {

	accountsDB := models.NewLedgerDB(ledgerSrv.DB)

	// Otherwise, add lg
	mAcc, aerr := accountsDB.CreateLedger(ledgerFromApi(lg))
	if aerr != nil {
		return nil, aerr
	}

	return ledgerToApi(mAcc), nil

}

// Updates the data component of the account.
func (ledgerSrv *LedgerServer) UpdateLedger(context context.Context, aLg *ledger.Ledger) (*ledger.Ledger, error) {

	ledgerDB := models.NewLedgerDB(ledgerSrv.DB)

	// Otherwise, add account
	mLg, aerr := ledgerDB.UpdateLedger(ledgerFromApi(aLg))
	if aerr != nil {
		return nil, aerr
	}

	return ledgerToApi(mLg), nil

}
