package controllers

import (
	"bitbucket.org/caricah/service-ledger/models"
	"bitbucket.org/caricah/service-ledger/ledger"
	"context"
	"database/sql"
		)

type LedgerServer struct {
	DB    *sql.DB
}

func ToMap(raw map[string]*ledger.Any) models.DataMap {

	dataMap := make(models.DataMap, 0)
	for key, val := range raw{
		dataMap[key] = val.Value
	}
	return dataMap
}

func FromMap(model  models.DataMap)map[string]*ledger.Any {
	return make(map[string]*ledger.Any, 0)
}


func fromLedgerType(raw ledger.LedgerType) string {
	return ledger.LedgerType_name[int32(raw)]
}

func toLedgerType(model string) ledger.LedgerType {
	ledgerType := ledger.LedgerType_value[model]
	return ledger.LedgerType(ledgerType)
}


func ledgerToApi(mLg *models.Ledger) *ledger.Ledger{
	return &ledger.Ledger{Reference: mLg.Reference.String, Type: toLedgerType(mLg.Type),
		Parent: mLg.Parent, Data: FromMap(mLg.Data)}
}

func ledgerFromApi(aLg *ledger.Ledger) *models.Ledger{
	return &models.Ledger{Reference: sql.NullString{String: aLg.Reference}, Type: fromLedgerType(aLg.Type),
		Parent: aLg.Parent, Data: ToMap(aLg.Data)}

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

	castLedgers, ok := results.([]models.Ledger)
	if !ok {
		return ledger.ErrorSearchQueryResultsNotCasting
	}

	for _, lg := range castLedgers {
		server.Send(ledgerToApi(&lg))
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
