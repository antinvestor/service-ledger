package handlers

import (
	"context"

	commonv1 "github.com/antinvestor/apis/go/common/v1"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame"
)

type LedgerServer struct {
	Service *frame.Service
	ledgerV1.UnimplementedLedgerServiceServer
}

func fromLedgerType(raw ledgerV1.LedgerType) string {
	return ledgerV1.LedgerType_name[int32(raw)]
}

func toLedgerType(model string) ledgerV1.LedgerType {
	ledgerType := ledgerV1.LedgerType_value[model]
	return ledgerV1.LedgerType(ledgerType)
}

func ledgerToApi(mLg *models.Ledger) *ledgerV1.Ledger {
	return &ledgerV1.Ledger{Reference: mLg.ID, Type: toLedgerType(mLg.Type),
		Parent: mLg.ParentID, Data: frame.DBPropertiesToMap(mLg.Data)}
}

func ledgerFromApi(aLg *ledgerV1.Ledger) *models.Ledger {
	return &models.Ledger{
		BaseModel: frame.BaseModel{ID: aLg.GetReference()},
		Type:      fromLedgerType(aLg.GetType()),
		ParentID:  aLg.GetParent(),
		Data:      frame.DBPropertiesFromMap(aLg.GetData())}

}

// SearchLedgers for an ledger based on search request json query
func (ledgerSrv *LedgerServer) SearchLedgers(request *commonv1.SearchRequest, server ledgerV1.LedgerService_SearchLedgersServer) error {

	ctx := server.Context()
	ledgerRepository := repository.NewLedgerRepository(ledgerSrv.Service)

	jobResult, err := ledgerRepository.Search(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for result := range jobResult.ResultChan() {

		if result.IsError() {
			return apperrors.ErrorSystemFailure.Override(result.Error())
		}

		for _, ledger := range result.Item() {
			if err = server.Send(ledgerToApi(ledger)); err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateLedger a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateLedger(ctx context.Context, lg *ledgerV1.Ledger) (*ledgerV1.Ledger, error) {

	ledgerRepository := repository.NewLedgerRepository(ledgerSrv.Service)

	// Otherwise, add lg
	mAcc, aerr := ledgerRepository.Create(ctx, ledgerFromApi(lg))
	if aerr != nil {
		return nil, aerr
	}

	return ledgerToApi(mAcc), nil

}

// UpdateLedger the data component of the account.
func (ledgerSrv *LedgerServer) UpdateLedger(context context.Context, aLg *ledgerV1.Ledger) (*ledgerV1.Ledger, error) {

	ledgerDB := repository.NewLedgerRepository(ledgerSrv.Service)

	// Otherwise, add account
	mLg, aerr := ledgerDB.Update(context, ledgerFromApi(aLg))
	if aerr != nil {
		return nil, aerr
	}

	return ledgerToApi(mLg), nil

}
