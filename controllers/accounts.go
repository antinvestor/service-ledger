package controllers

import (
	"context"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
	"google.golang.org/genproto/googleapis/type/money"
	"math/big"
)

const NanoAmountDivisor = 1000000000
const DefaultAmountDivisor = 10000

func toMoneyInt(currency string, naive *models.Int) money.Money {
	naive = naive.ToAbs()
	unitBig := big.NewInt(0).Div(naive.ToInt(), big.NewInt(DefaultAmountDivisor))
	nanosBig := big.NewInt(0).Sub(naive.ToInt(), unitBig)
	nanosBig = big.NewInt(0).Mul(nanosBig, big.NewInt(NanoAmountDivisor))
	nanosBig = big.NewInt(0).Div(nanosBig, big.NewInt(DefaultAmountDivisor))

	return money.Money{CurrencyCode: currency, Units: unitBig.Int64(), Nanos: int32(nanosBig.Int64())}
}

func fromMoney(m *money.Money) (naive *models.Int) {

	unitsBig := big.NewInt(0).Mul(big.NewInt(m.Units), big.NewInt(DefaultAmountDivisor))
	nanosBig := big.NewInt(0).Mul(big.NewInt(int64(m.Nanos)), big.NewInt(DefaultAmountDivisor/NanoAmountDivisor))

	total := big.NewInt(0).Add(unitsBig, nanosBig)
	return models.FromBigInt(total)
}

func accountToApi(mAcc *models.Account) *ledgerV1.Account {

	balance := toMoneyInt(mAcc.Currency, mAcc.Balance)

	return &ledgerV1.Account{Reference: mAcc.ID,
		Ledger: mAcc.LedgerID, Balance: &balance, Data: frame.DBPropertiesToMap(mAcc.Data)}
}

func accountFromApi(account *ledgerV1.Account) *models.Account {

	return &models.Account{
		BaseModel: frame.BaseModel{ID: account.GetReference()},
		LedgerID:  account.GetLedger(),
		Currency:  account.GetBalance().CurrencyCode,
		Balance:   fromMoney(account.GetBalance()),
		Data:      frame.DBPropertiesFromMap(account.Data)}
}

func (ledgerSrv *LedgerServer) SearchAccounts(
	request *ledgerV1.SearchRequest, server ledgerV1.LedgerService_SearchAccountsServer) error {

	ctx := server.Context()

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)

	castAccounts, aerr := accountsRepo.Search(ctx, request.GetQuery())
	if aerr != nil {
		return aerr
	}

	for _, account := range castAccounts {
		_ = server.Send(accountToApi(account))
	}

	return nil
}

// CreateAccount a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Create(ctx, accountFromApi(aAcc))
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}

// UpdateAccount the data component of the account.
func (ledgerSrv *LedgerServer) UpdateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Update(ctx, aAcc.Reference, aAcc.Data)
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}
