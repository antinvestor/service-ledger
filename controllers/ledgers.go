package controllers

import (
	"context"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
)

type LedgerServer struct {
	Service *frame.Service
}

func fromLedgerType(raw ledger.LedgerType) string {
	return ledger.LedgerType_name[int32(raw)]
}

func toLedgerType(model string) ledger.LedgerType {
	ledgerType := ledger.LedgerType_value[model]
	return ledger.LedgerType(ledgerType)
}

func ledgerToApi(mLg *models.Ledger) *ledger.Ledger {
	return &ledger.Ledger{Reference: mLg.ID, Type: toLedgerType(mLg.Type),
		Parent: mLg.ParentID, Data: frame.DBPropertiesToMap(mLg.Data)}
}

func ledgerFromApi(aLg *ledger.Ledger) *models.Ledger {
	return &models.Ledger{
		BaseModel: frame.BaseModel{ID: aLg.GetReference()},
		Type:      fromLedgerType(aLg.GetType()),
		ParentID:  aLg.GetParent(),
		Data:      frame.DBPropertiesFromMap(aLg.GetData())}

}

// SearchLedgers for an ledger based on search request json query
func (ledgerSrv *LedgerServer) SearchLedgers(request *ledger.SearchRequest, server ledger.LedgerService_SearchLedgersServer) error {

	ctx := server.Context()
	ledgerRepository := repositories.NewLedgerRepository(ledgerSrv.Service)

	castLedgers, aerr := ledgerRepository.Search(ctx, request.GetQuery())
	if aerr != nil {
		return aerr
	}

	for _, lg := range castLedgers {
		server.Send(ledgerToApi(lg))
	}

	return nil
}

// CreateLedger a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateLedger(ctx context.Context, lg *ledger.Ledger) (*ledger.Ledger, error) {

	ledgerRepository := repositories.NewLedgerRepository(ledgerSrv.Service)

	// Otherwise, add lg
	mAcc, aerr := ledgerRepository.Create(ctx, ledgerFromApi(lg))
	if aerr != nil {
		return nil, aerr
	}

	return ledgerToApi(mAcc), nil

}

// UpdateLedger the data component of the account.
func (ledgerSrv *LedgerServer) UpdateLedger(context context.Context, aLg *ledger.Ledger) (*ledger.Ledger, error) {

	ledgerDB := repositories.NewLedgerRepository(ledgerSrv.Service)

	// Otherwise, add account
	mLg, aerr := ledgerDB.Update(context, ledgerFromApi(aLg))
	if aerr != nil {
		return nil, aerr
	}

	return ledgerToApi(mLg), nil

}
