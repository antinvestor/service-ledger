package handlers

import (
	commonv1 "github.com/antinvestor/apis/go/common/v1"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	repository2 "github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	utility2 "github.com/antinvestor/service-ledger/internal/utility"
)

func transactionEntryToApi(mEntry *models.TransactionEntry) *ledgerV1.TransactionEntry {

	entryAmount := utility2.ToMoney(mEntry.Currency, mEntry.Amount.Decimal)

	balanceAmount := utility2.ToMoney(mEntry.Currency, mEntry.Balance.Decimal)

	return &ledgerV1.TransactionEntry{
		Account:     mEntry.AccountID,
		Transaction: mEntry.TransactionID,
		Amount:      &entryAmount,
		Credit:      mEntry.Credit,
		AccBalance:  &balanceAmount,
	}
}

// SearchTransactionEntries for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactionEntries(request *commonv1.SearchRequest, server ledgerV1.LedgerService_SearchTransactionEntriesServer) error {

	ctx := server.Context()

	accountRepository := repository2.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository2.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	jobResult, err := transactionRepository.SearchEntries(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for result := range jobResult.ResultChan() {

		if result.IsError() {
			return apperrors.ErrorSystemFailure.Override(result.Error())
		}
		for _, entry := range result.Item() {
			if err = server.Send(transactionEntryToApi(entry)); err != nil {
				return err
			}
		}
	}
	return nil
}
