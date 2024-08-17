package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/utility"
	"github.com/pitabwire/frame"
	"golang.org/x/text/currency"
)

const constAccountQuery = `WITH current_balance_summary AS (
    SELECT 
        e.account_id, 
        t.currency,
        COALESCE(SUM(CASE WHEN t.transaction_type IN ('NORMAL', 'REVERSAL') AND t.cleared_at IS NOT NULL THEN e.amount ELSE 0 END), 0) AS balance,
        COALESCE(SUM(CASE WHEN t.transaction_type IN ('NORMAL', 'REVERSAL') AND t.cleared_at IS NULL THEN e.amount ELSE 0 END), 0) AS uncleared_balance,
        COALESCE(SUM(CASE WHEN t.transaction_type = 'RESERVATION' THEN e.amount ELSE 0 END), 0) AS reserved_balance
    FROM transaction_entries e 
    LEFT JOIN transactions t ON e.transaction_id = t.id
    GROUP BY e.account_id, t.currency
)
SELECT 
    a.id,
    a.currency,
    a.data,
    COALESCE(bs.balance, 0) AS total_balance,
    COALESCE(bs.uncleared_balance, 0) AS total_uncleared_balance,
    COALESCE(bs.reserved_balance, 0) AS total_reserved_balance,
    a.ledger_id,
    a.ledger_type,
    a.created_at,
    a.modified_at,
    a.version,
    a.tenant_id,
    a.partition_id,
    a.access_id,
    a.deleted_at
FROM accounts a
LEFT JOIN current_balance_summary bs ON a.id = bs.account_id AND a.currency = bs.currency `

type AccountRepository interface {
	GetByID(ctx context.Context, id string) (*models.Account, utility.ApplicationLedgerError)
	ListByID(ctx context.Context, ids ...string) (map[string]*models.Account, utility.ApplicationLedgerError)
	Search(ctx context.Context, query string) (<-chan any, error)
	Create(ctx context.Context, ledger *models.Account) (*models.Account, utility.ApplicationLedgerError)
	Update(ctx context.Context, id string, data map[string]string) (*models.Account, utility.ApplicationLedgerError)
}

// accountRepository provides all functions related to ledger account
type accountRepository struct {
	service          *frame.Service
	ledgerRepository LedgerRepository
}

// NewAccountRepository provides instance of `accountRepository`
func NewAccountRepository(service *frame.Service) AccountRepository {
	return &accountRepository{service: service, ledgerRepository: NewLedgerRepository(service)}
}

// GetByID returns an acccount with the given Reference
func (a *accountRepository) GetByID(ctx context.Context, id string) (*models.Account, utility.ApplicationLedgerError) {

	if id == "" {
		return nil, utility.ErrorUnspecifiedID
	}

	accList, err := a.ListByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return accList[id], nil
}

// ListByID returns a list of acccounts with the given list of ids
func (a *accountRepository) ListByID(ctx context.Context, ids ...string) (map[string]*models.Account, utility.ApplicationLedgerError) {

	if len(ids) == 0 {
		return nil, utility.ErrorAccountsNotFound.Extend("No Accounts were specified")
	}

	accountsMap := map[string]*models.Account{}

	queryMap := map[string]any{
		"query": map[string]any{
			"must": map[string]any{
				"fields": []map[string]any{
					{
						"id": map[string]any{
							"in": ids,
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(queryMap)
	if err != nil {
		return nil, utility.ErrorSystemFailure.Override(err).Extend("Json marshalling error")
	}

	query := string(queryBytes)

	resultChannel, err := a.Search(ctx, query)
	if err != nil {
		return nil, utility.ErrorSystemFailure.Override(err).Extend(fmt.Sprintf("db query error [%s]", query))
	}

	for {
		select {
		case result, ok := <-resultChannel:
			if !ok {
				return accountsMap, nil
			}

			switch v := result.(type) {
			case *models.Account:
				accountsMap[v.ID] = v
			case error:
				return nil, utility.ErrorSystemFailure.Override(v)
			default:
				return nil, utility.ErrorBadDataSupplied.Extend(fmt.Sprintf(" unsupported type supplied %v", v))
			}

		case <-ctx.Done():
			return nil, utility.ErrorSystemFailure.Override(ctx.Err())
		}
	}
}

func (a *accountRepository) Search(ctx context.Context, query string) (<-chan any, error) {

	resultChannel := make(chan any)

	service := a.service
	job := service.NewJob(func(ctx context.Context) error {

		defer close(resultChannel)

		rawQuery, aerr := NewSearchRawQuery(ctx, query)
		if aerr != nil {
			select {
			case resultChannel <- aerr:
			case <-ctx.Done():
			}
			return nil
		}

		sqlQuery := rawQuery.ToQueryConditions()

		for sqlQuery.canLoad() {

			rows, err := service.DB(ctx, true).
				Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
				Raw(fmt.Sprintf(`%s WHERE %s`, constAccountQuery, sqlQuery.sql), sqlQuery.args...).Rows()
			if err != nil {
				if frame.DBErrorIsRecordNotFound(err) {
					select {
					case resultChannel <- utility.ErrorLedgerNotFound:
					case <-ctx.Done():
					}
					return nil
				}
				select {
				case resultChannel <- utility.ErrorSystemFailure.Override(err).Extend("Query execution error"):
				case <-ctx.Done():
				}
				return nil
			}

			var accountList []*models.Account
			for rows.Next() {
				acc := models.Account{}
				err = rows.Scan(
					&acc.ID, &acc.Currency, &acc.Data, &acc.Balance, &acc.UnClearedBalance, &acc.ReservedBalance,
					&acc.LedgerID, &acc.LedgerType, &acc.CreatedAt, &acc.ModifiedAt, &acc.Version, &acc.TenantID,
					&acc.PartitionID, &acc.AccessID, &acc.DeletedAt)
				if err != nil {
					select {
					case resultChannel <- utility.ErrorSystemFailure.Override(err).Extend("Query binding error"):
					case <-ctx.Done():
					}
					return nil
				}
				accountList = append(accountList, &acc)
			}

			err = rows.Close()
			if err != nil {
				select {
				case resultChannel <- utility.ErrorSystemFailure.Override(err).Extend("Query closure error"):
				case <-ctx.Done():
				}
				return nil
			}

			for _, acc := range accountList {
				select {
				case resultChannel <- acc:
				case <-ctx.Done():
					return nil
				}
			}
			if sqlQuery.next(len(accountList)) {
				return nil
			}
		}
		return nil

	})

	err := service.SubmitJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return resultChannel, nil
}

// Update persists an existing account in the ledger if it is existent
func (a *accountRepository) Update(ctx context.Context, id string, data map[string]string) (*models.Account, utility.ApplicationLedgerError) {

	existingAccount, errAcc := a.GetByID(ctx, id)
	if errAcc != nil {
		return nil, errAcc
	}

	for key, value := range data {
		if value != "" && value != existingAccount.Data[key] {
			existingAccount.Data[key] = value
		}
	}

	err := a.service.DB(ctx, false).Save(&existingAccount).Error
	if err != nil {
		a.service.L().WithError(err).Error("could not save the account")
		return nil, utility.ErrorSystemFailure.Override(err)
	}
	return existingAccount, nil

}

// Create persists a new account in the ledger if its none existent
func (a *accountRepository) Create(ctx context.Context, account *models.Account) (*models.Account, utility.ApplicationLedgerError) {

	if account.LedgerID != "" {

		lg, err := a.ledgerRepository.GetByID(ctx, account.LedgerID)
		if err != nil {
			return nil, utility.ErrorSystemFailure.Override(err)
		}
		account.LedgerID = lg.ID
		account.LedgerType = lg.Type
	}

	currencyUnit, err := currency.ParseISO(account.Currency)
	if err != nil {
		return nil, utility.ErrorAccountsCurrencyUnknown
	}

	account.Currency = currencyUnit.String()

	err = a.service.DB(ctx, false).Save(account).Error
	if err != nil {
		a.service.L().WithError(err).Error("could not save the ledger")
		return nil, utility.ErrorSystemFailure.Override(err)
	}

	return account, nil

}
