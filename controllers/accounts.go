package controllers

import (
	"bitbucket.org/caricah/service-ledger/ledger"
	"bitbucket.org/caricah/service-ledger/models"
	"context"
	"google.golang.org/genproto/googleapis/type/money"
)

const NanoAmountDivisor = 1000000000
const DefaultAmountDivisor = 10000

func toMoneyInt(naive int64) (unit int64, nanos int32) {
	unit = naive / DefaultAmountDivisor
	nanos = int32(naive-unit) * NanoAmountDivisor / DefaultAmountDivisor
	return
}

func fromMoney(m *money.Money) (naive int64) {
	return m.Units*DefaultAmountDivisor + int64(m.Nanos)*DefaultAmountDivisor/NanoAmountDivisor
}

func accountToApi(mAcc *models.Account) *ledger.Account {

	units, nanos := toMoneyInt(mAcc.Balance)
	balance := money.Money{CurrencyCode: mAcc.Currency, Units: units, Nanos: nanos}

	return &ledger.Account{Reference: mAcc.Reference,
		Ledger: mAcc.Ledger, Balance: &balance, Data: FromMap(mAcc.Data)}
}

func accountFromApi(account *ledger.Account) *models.Account {

	naive := fromMoney(account.Balance)

	return &models.Account{Reference: account.Reference, Ledger: account.Ledger,
		Currency: account.Balance.CurrencyCode, Balance: naive, Data: ToMap(account.Data)}

}

func (ledgerSrv *LedgerServer) SearchAccounts(
	request *ledger.SearchRequest, server ledger.LedgerService_SearchAccountsServer) error {

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
