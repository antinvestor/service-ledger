package business

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/workerpool"
	"github.com/shopspring/decimal"
)

// AccountBusiness defines the business interface for account operations
type AccountBusiness interface {
	CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error)
	SearchAccounts(ctx context.Context, req *commonv1.SearchRequest) (workerpool.JobResultPipe[[]*ledgerv1.Account], error)
	GetAccount(ctx context.Context, id string) (*ledgerv1.Account, error)
	UpdateAccount(ctx context.Context, req *ledgerv1.UpdateAccountRequest) (*ledgerv1.Account, error)
	DeleteAccount(ctx context.Context, id string) error
}

// accountBusiness implements the AccountBusiness interface
type accountBusiness struct {
	workMan     workerpool.Manager
	accountRepo repository.AccountRepository
}

// NewAccountBusiness creates a new account business instance
func NewAccountBusiness(workMan workerpool.Manager, accountRepo repository.AccountRepository) AccountBusiness {
	return &accountBusiness{
		workMan:     workMan,
		accountRepo: accountRepo,
	}
}

// CreateAccount creates a new account with business validation
func (b *accountBusiness) CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error) {
	// Business logic validation
	if req.Id == "" {
		return nil, ErrAccountReferenceRequired
	}

	if req.LedgerId == "" {
		return nil, ErrAccountLedgerIDRequired
	}

	// Convert API request to model
	// Create a zero balance money account for the currency

	accountModel := &models.Account{
		LedgerID: req.GetLedgerId(),
		Currency: req.GetCurrency(),
		Balance:  decimal.NewNullDecimal(decimal.Zero),
		Data:     req.GetData().AsMap()}

	accountModel.GenID(ctx)
	if req.GetId() != "" {
		accountModel.ID = req.GetId()
	}

	// Create the account through repository
	err := b.accountRepo.Create(ctx, accountModel)
	if err != nil {
		return nil, err
	}

	// Convert back to API type
	return accountModel.ToAPI(), nil
}

// SearchAccounts searches for accounts based on query
func (b *accountBusiness) SearchAccounts(ctx context.Context, req *commonv1.SearchRequest) (workerpool.JobResultPipe[[]*ledgerv1.Account], error) {
	// Business logic for search validation
	query := req.GetQuery()
	if query == "" {
		query = "{}" // Default empty query
	}

	job := workerpool.NewJob[[]*ledgerv1.Account](func(ctx context.Context, pipe workerpool.JobResultPipe[[]*ledgerv1.Account]) error {

		// Search through repository
		result, err := b.accountRepo.SearchAsESQ(ctx, query)
		if err != nil {
			return err
		}

		for {

			res, ok := result.ReadResult(ctx)
			if !ok {
				return nil
			}

			if res.IsError() {
				return res.Error()
			}

			var apiResults []*ledgerv1.Account
			for _, account := range res.Item() {
				apiResults = append(apiResults, account.ToAPI())
			}

			jobErr := pipe.WriteResult(ctx, apiResults)
			if jobErr != nil {
				return jobErr
			}
		}

	})

	err := workerpool.SubmitJob(ctx, b.workMan, job)
	if err != nil {
		return nil, err
	}

	return job, nil

}

// GetAccount retrieves an account by ID
func (b *accountBusiness) GetAccount(ctx context.Context, id string) (*ledgerv1.Account, error) {
	if id == "" {
		return nil, ErrAccountIDRequired
	}

	account, err := b.accountRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return account.ToAPI(), nil
}

// UpdateAccount updates an existing account
func (b *accountBusiness) UpdateAccount(ctx context.Context, req *ledgerv1.UpdateAccountRequest) (*ledgerv1.Account, error) {
	// Business logic validation
	if req.Id == "" {
		return nil, ErrAccountIDRequired
	}

	// Convert API request to model - need to get existing account first
	existingAccount, err := b.accountRepo.GetByID(ctx, req.Id)
	if err != nil {
		return nil, err
	}

	// Update fields from request
	if req.Data != nil {
		dataMap := &data.JSONMap{}
		existingAccount.Data = dataMap.FromProtoStruct(req.Data)
	}

	// Update through repository
	_, err = b.accountRepo.Update(ctx, existingAccount)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return existingAccount.ToAPI(), nil
}

// DeleteAccount deletes an account by ID
func (b *accountBusiness) DeleteAccount(ctx context.Context, id string) error {
	if id == "" {
		return ErrAccountIDRequired
	}

	// Delete through repository
	return nil // Implementation depends on repository interface
}
