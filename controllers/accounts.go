package controllers

import (
	"bitbucket.org/caricah/service-ledger/models"
	"bitbucket.org/caricah/service-ledger/ledger"
	"context"
)

func accountToApi(mAcc *models.Account) *ledger.Account {
	return &ledger.Account{Reference: mAcc.Reference, Ledger: mAcc.Ledger, Currency: mAcc.Currency, Data: FromMap(mAcc.Data)}
}

func accountFromApi(account *ledger.Account) *models.Account {
	return &models.Account{Reference: account.Reference, Ledger: account.Ledger,
		Currency: account.Currency, Data: ToMap(account.Data)}

}

func (ledgerSrv *LedgerServer) SearchAccounts(request *ledger.SearchRequest, server ledger.LedgerService_SearchAccountsServer) error {

	engine, aerr := models.NewSearchEngine(ledgerSrv.DB, models.SearchNamespaceAccounts)
	if aerr != nil {
		return aerr
	}

	request.GetQuery()

	results, aerr := engine.Query(request.GetQuery())
	if aerr != nil {
		return aerr
	}

	castAccounts, ok := results.([]models.Account)
	if !ok {
		return ledger.ErrorSearchQueryResultsNotCasting
	}

	for _, account := range castAccounts {
		server.Send(accountToApi(&account))
	}

	return nil
}

// Creates a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateAccount(context context.Context, aAcc *ledger.Account) (*ledger.Account, error) {

	accountsDB := models.NewAccountDB(ledgerSrv.DB)

	// Otherwise, add account
	mAcc, aerr := accountsDB.CreateAccount(accountFromApi(aAcc))
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}

// Updates the data component of the account.
func (ledgerSrv *LedgerServer) UpdateAccount(context context.Context, aAcc *ledger.Account) (*ledger.Account, error) {

	accountsDB := models.NewAccountDB(ledgerSrv.DB)

	// Otherwise, add account
	mAcc, aerr := accountsDB.UpdateAccount(accountFromApi(aAcc))
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}
